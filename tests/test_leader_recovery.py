"""
Regression tests for safe leader recovery and replica rejoin.

Reproduces the Docker failure where the node configured as the initial
primary (node-a) restarted after a failover, came back as a second primary
with epoch 0 and an empty store, and split the cluster.

Unit tests cover each safety mechanism in isolation; the integration tests
run three real ``MiniRedisNode`` processes in-process on dynamic ports and
drive the full lifecycle (bootstrap, failover, restart, partitions). Every
integration test runs a background monitor asserting that at no sampled
instant are two nodes authorized to accept primary writes.
"""

from __future__ import annotations

import asyncio
import json
import socket
from typing import Callable, Optional

import grpc
import pytest

from miniredis.cluster import WriteAdmissionGate
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.leadership import ClusterView
from miniredis.logical_clock import LamportClock
from miniredis.node import MiniRedisNode, NodeConfig
from miniredis.protocol import Command
from miniredis.pubsub import PubSub
from miniredis.replication.progress import EMPTY_HISTORY_DIGEST, ReplicationProgress
from miniredis.replication.proto import replication_pb2, replication_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, Replicator, sync_state_from_primary
from miniredis.storage import Store

NODE_IDS = ("node-a", "node-b", "node-c")


class _Ctx:
    def peer(self) -> str:
        return "test"


async def _wait_until(predicate: Callable[[], bool], timeout: float, message: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {message}")
        await asyncio.sleep(0.05)


# ──────────────────────────────────────────────────────────────────────
# Unit tests
# ──────────────────────────────────────────────────────────────────────


def test_restored_history_digest_extends_like_the_leader() -> None:
    """A snapshot-restored replica keeps producing the leader's digest."""
    leader = ReplicationProgress("node-c", role="primary", epoch=7)
    leader.record_applied(7, 1, "SET", "k1", b"v1")

    rejoined = ReplicationProgress("node-a", role="replica", epoch=0)
    snap = leader.snapshot()
    rejoined.restore_from_snapshot(snap.epoch, snap.last_applied_sequence, snap.history_digest)

    leader.record_applied(7, 2, "SET", "k2", b"v2")
    rejoined.record_applied(7, 2, "SET", "k2", b"v2")

    assert rejoined.snapshot().history_digest == leader.snapshot().history_digest
    assert rejoined.snapshot().history_digest != EMPTY_HISTORY_DIGEST


async def test_replica_fences_deposed_leader_after_coordinator() -> None:
    """Once a coordinator for a newer epoch is accepted, old-epoch writes are refused.

    The servicer had only seen epoch 5 writes; the coordinator announcement
    advanced the progress tracker to epoch 9 before any epoch-9 write arrived.
    """
    store = Store()
    progress = ReplicationProgress("node-b", role="replica", epoch=0)
    observed: list[int] = []
    servicer = ReplicaServicer(store, node_id="node-b", progress=progress, on_newer_epoch=observed.append)
    progress.update_role_and_epoch("replica", 5)  # coordinator for epoch 5 accepted

    ok = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(op_type="SET", key="k", value=b"old", sequence=1, epoch=5), _Ctx()
    )
    assert ok.success
    progress.update_role_and_epoch("replica", 9)  # coordinator for epoch 9 accepted

    stale = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(op_type="SET", key="k", value=b"conflict", sequence=2, epoch=5), _Ctx()
    )
    assert not stale.success
    assert stale.epoch == 9, "rejection must tell the stale primary about the newer epoch"
    assert store.get(b"k") == b"old"

    newer = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(op_type="SET", key="k", value=b"new", sequence=1, epoch=11), _Ctx()
    )
    assert newer.success
    assert observed == [11], "writes from an unannounced newer leader must wake the reconciler"


async def test_primary_never_applies_replicated_writes() -> None:
    store = Store()
    observed: list[int] = []
    servicer = ReplicaServicer(
        store,
        node_id="node-a",
        progress=ReplicationProgress("node-a", role="primary", epoch=3),
        role_provider=lambda: "primary",
        on_newer_epoch=observed.append,
    )
    ack = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(op_type="SET", key="k", value=b"v", sequence=1, epoch=8), _Ctx()
    )
    assert not ack.success
    assert store.get(b"k") is None
    assert observed == [8]


async def test_stale_primary_is_fenced_by_replica_rejection() -> None:
    """A primary whose write is rejected for a newer epoch stops accepting writes."""
    replica_progress = ReplicationProgress("node-c", role="replica", epoch=0)
    replica_progress.update_role_and_epoch("replica", 100)
    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(Store(), node_id="node-c", progress=replica_progress), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    replica_address = f"127.0.0.1:{port}"

    gate = WriteAdmissionGate(can_serve_writes=True)
    dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer), writes_enabled=True, write_gate=gate)
    election = BullyElection(
        node_id="node-a",
        tcp_address="127.0.0.1:1",
        grpc_address="127.0.0.1:2",
        peers={},
        dispatcher=dispatcher,
        total_cluster_size=2,
        write_gate=gate,
        role="primary",
    )
    election._election_epoch = 5  # an epoch the cluster has already moved past
    dispatcher.set_replicator(
        Replicator(
            [replica_address],
            total_cluster_size=2,
            write_mode="quorum",
            epoch=5,
            node_id="node-a",
            on_higher_epoch=election.observe_higher_epoch,
        )
    )
    try:
        first = await dispatcher.handle_command(Command("SET", [b"k", b"v"]), object())
        assert first.startswith(b"-ERR"), first
        assert not dispatcher.writes_enabled, "fencing must be synchronous"
        await _wait_until(lambda: election.role == "replica", 2.0, "stale primary steps down")
        second = await dispatcher.handle_command(Command("SET", [b"k", b"v"]), object())
        assert second.startswith(b"-ERR"), second
    finally:
        await election.aclose()
        await server.stop(grace=0)


async def test_snapshot_sync_refuses_unsafe_sources() -> None:
    """Rejoin never adopts a non-primary snapshot or one from an older epoch."""
    source_progress = ReplicationProgress("node-b", role="replica", epoch=0)
    source_progress.record_applied(4, 1, "SET", "k", b"v")
    source_role = ["replica"]
    server = grpc.aio.server()
    source_store = Store()
    source_store.set(b"k", b"v")
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(source_store, node_id="node-b", progress=source_progress, role_provider=lambda: source_role[0]),
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    address = f"127.0.0.1:{port}"
    try:
        store = Store()
        progress = ReplicationProgress("node-a", role="replica", epoch=0)
        servicer = ReplicaServicer(store, node_id="node-a", progress=progress)

        async def sync(**kwargs) -> bool:
            return await sync_state_from_primary(address, store, servicer, progress, LamportClock(), "node-a", **kwargs)

        assert await sync() is False, "a replica's snapshot is not authoritative"
        assert store.get(b"k") is None

        source_role[0] = "primary"
        progress.update_role_and_epoch("replica", 9)  # already accepted a newer epoch
        assert await sync() is False, "an older-epoch snapshot must not roll back state"
        assert store.get(b"k") is None

        progress2 = ReplicationProgress("node-a2", role="replica", epoch=0)
        assert await sync_state_from_primary(address, store, servicer, progress2, LamportClock(), "node-a2")
        assert store.get(b"k") == b"v"
        assert progress2.snapshot().history_digest == source_progress.snapshot().history_digest
    finally:
        await server.stop(grace=0)


def test_confirmed_leader_requires_self_reported_primary_in_newest_epoch() -> None:
    def resp(node_id: str, role: str, epoch: int, primary: str = "") -> replication_pb2.ProgressResponse:
        return replication_pb2.ProgressResponse(node_id=node_id, role=role, epoch=epoch, primary_node_id=primary)

    # Replicas still naming a restarted node as leader is not a confirmation.
    view = ClusterView({"node-b": resp("node-b", "replica", 5, "node-a"), "node-c": resp("node-c", "replica", 5, "node-a")})
    assert view.confirmed_leader() is None
    assert view.claimed_leaders() == {"node-a"}

    # A deposed primary in an older epoch loses to the newer leader.
    view = ClusterView({"node-a": resp("node-a", "primary", 5), "node-c": resp("node-c", "primary", 9)})
    assert view.confirmed_leader() == "node-c"
    assert view.confirmed_leader(min_epoch=10) is None

    # A fresh cluster (epoch 0 everywhere) has no leader to confirm.
    assert ClusterView({"node-b": resp("node-b", "replica", 0)}).confirmed_leader() is None


# ──────────────────────────────────────────────────────────────────────
# In-process three-node cluster harness
# ──────────────────────────────────────────────────────────────────────


def _free_ports(count: int) -> list[int]:
    sockets = []
    for _ in range(count):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    ports = [sock.getsockname()[1] for sock in sockets]
    for sock in sockets:
        sock.close()
    return ports


class FaultInjector:
    """Directional per-RPC network faults between named nodes."""

    def __init__(self, address_to_node: dict[str, str]) -> None:
        self._address_to_node = address_to_node
        self._blocked: set[tuple[str, str, Optional[str]]] = set()

    def isolate(self, node_id: str, rpcs: Optional[set[str]] = None) -> None:
        for other in NODE_IDS:
            if other == node_id:
                continue
            for rpc in rpcs or {None}:
                self._blocked.add((node_id, other, rpc))
                self._blocked.add((other, node_id, rpc))

    def block(self, source: str, target: str, rpcs: Optional[set[str]] = None) -> None:
        """Block traffic in one direction only."""
        for rpc in rpcs or {None}:
            self._blocked.add((source, target, rpc))

    def cut(self, left: str, right: str) -> None:
        """Cut one link in both directions (asymmetric partition of the cluster)."""
        self.block(left, right)
        self.block(right, left)

    def heal(self) -> None:
        self._blocked.clear()

    def filter_for(self, source: str) -> Callable[[str, str], bool]:
        def _allowed(rpc_name: str, target_address: str) -> bool:
            target = self._address_to_node.get(target_address)
            if target is None:
                return True
            return (source, target, None) not in self._blocked and (source, target, rpc_name) not in self._blocked

        return _allowed


class LocalCluster:
    def __init__(self, election_algo: str = "bully", write_mode: str = "async") -> None:
        ports = _free_ports(2 * len(NODE_IDS))
        self.tcp = {nid: ports[i] for i, nid in enumerate(NODE_IDS)}
        self.grpc = {nid: ports[i + len(NODE_IDS)] for i, nid in enumerate(NODE_IDS)}
        self.election_algo = election_algo
        self.write_mode = write_mode
        self.faults = FaultInjector({f"127.0.0.1:{self.grpc[nid]}": nid for nid in NODE_IDS})
        self.nodes: dict[str, MiniRedisNode] = {}
        self.max_concurrent_primaries = 0
        self.overlap_seconds = 0.0  # sampled time with more than one write-authorized primary
        self.same_epoch_overlap = False  # two authorized primaries sharing one epoch: never allowed
        self._monitor: Optional[asyncio.Task[None]] = None

    def config(self, node_id: str) -> NodeConfig:
        is_primary = node_id == "node-a"
        return NodeConfig(
            node_id=node_id,
            host="127.0.0.1",
            port=self.tcp[node_id],
            grpc_port=self.grpc[node_id],
            role="primary" if is_primary else "replica",
            replica_of=[f"127.0.0.1:{self.grpc[o]}" for o in NODE_IDS if o != node_id] if is_primary else [],
            primary_address=None if is_primary else f"127.0.0.1:{self.grpc['node-a']}",
            primary_node_id=None if is_primary else "node-a",
            peers={
                o: PeerEndpoint(o, f"127.0.0.1:{self.grpc[o]}", f"127.0.0.1:{self.tcp[o]}")
                for o in NODE_IDS
                if o != node_id
            },
            write_mode=self.write_mode,
            election_algo=self.election_algo,
            reconcile_interval_seconds=0.2,
            rpc_filter=self.faults.filter_for(node_id),
        )

    async def start(self, *node_ids: str) -> None:
        new = {nid: MiniRedisNode(self.config(nid)) for nid in node_ids}
        self.nodes.update(new)
        await asyncio.gather(*(node.start() for node in new.values()))
        if self._monitor is None:
            self._monitor = asyncio.create_task(self._watch_primaries())

    async def stop(self, node_id: str) -> None:
        await self.nodes.pop(node_id).stop()

    async def close(self) -> None:
        if self._monitor is not None:
            self._monitor.cancel()
            await asyncio.gather(self._monitor, return_exceptions=True)
        results = await asyncio.gather(*(node.stop() for node in self.nodes.values()), return_exceptions=True)
        self.nodes.clear()
        errors = [r for r in results if isinstance(r, BaseException)]
        assert not errors, f"node shutdown failed: {errors!r}"

    def write_authorized(self) -> list[str]:
        """Nodes that would currently admit a client write as primary."""
        return [
            nid
            for nid, node in self.nodes.items()
            if node.election.role == "primary"
            and node.dispatcher.writes_enabled
            and node.write_gate.can_serve_writes
        ]

    async def _watch_primaries(self) -> None:
        loop = asyncio.get_running_loop()
        last = loop.time()
        while True:
            authorized = self.write_authorized()
            now = loop.time()
            self.max_concurrent_primaries = max(self.max_concurrent_primaries, len(authorized))
            if len(authorized) > 1:
                self.overlap_seconds += now - last
                epochs = [self.nodes[n].election.election_epoch for n in authorized]
                self.same_epoch_overlap |= len(set(epochs)) < len(epochs)
            last = now
            await asyncio.sleep(0.01)

    def primaries(self) -> list[str]:
        return [nid for nid, node in self.nodes.items() if node.election.role == "primary"]

    def follows(self, node_id: str, leader_id: str) -> bool:
        node, leader = self.nodes.get(node_id), self.nodes.get(leader_id)
        return (
            node is not None
            and leader is not None
            and node.election.role == "replica"
            and node.election.current_primary_id == leader_id
            and node.election.election_epoch == leader.election.election_epoch
        )

    def in_sync(self, node_id: str, leader_id: str) -> bool:
        if not self.follows(node_id, leader_id):
            return False
        mine = self.nodes[node_id].progress.snapshot()
        theirs = self.nodes[leader_id].progress.snapshot()
        return (
            not mine.gap_detected
            and (mine.epoch, mine.last_applied_sequence, mine.history_digest)
            == (theirs.epoch, theirs.last_applied_sequence, theirs.history_digest)
        )

    async def command(self, node_id: str, *args: str) -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.tcp[node_id])
        try:
            writer.write(
                f"*{len(args)}\r\n".encode()
                + b"".join(f"${len(a.encode())}\r\n".encode() + a.encode() + b"\r\n" for a in args)
            )
            await writer.drain()
            header = await asyncio.wait_for(reader.readline(), timeout=5)
            if header.startswith(b"$") and header != b"$-1\r\n":
                body = await asyncio.wait_for(reader.readexactly(int(header[1:-2]) + 2), timeout=5)
                return body[:-2]
            return header.rstrip(b"\r\n")
        finally:
            writer.close()

    async def staleness(self, node_id: str) -> dict:
        reply = await self.command(node_id, "STALENESS")
        assert not reply.startswith(b"-"), reply
        return {entry["node_id"]: entry for entry in json.loads(reply)["nodes"]}


@pytest.fixture
async def cluster_factory():
    clusters: list[LocalCluster] = []

    def _make(**kwargs) -> LocalCluster:
        cluster = LocalCluster(**kwargs)
        clusters.append(cluster)
        return cluster

    yield _make
    for cluster in clusters:
        await cluster.close()


async def _bootstrap(cluster: LocalCluster) -> None:
    await cluster.start(*NODE_IDS)
    await _wait_until(
        lambda: cluster.primaries() == ["node-a"]
        and cluster.follows("node-b", "node-a")
        and cluster.follows("node-c", "node-a"),
        timeout=10,
        message="node-a bootstraps and both replicas follow it",
    )


# ──────────────────────────────────────────────────────────────────────
# Integration tests
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("election_algo", ["bully", "ring"])
async def test_fresh_bootstrap_initial_primary(cluster_factory, election_algo: str) -> None:
    cluster = cluster_factory(election_algo=election_algo)
    await _bootstrap(cluster)

    leader_epoch = cluster.nodes["node-a"].election.election_epoch
    assert leader_epoch >= 1, "bootstrap must establish a real epoch, not epoch 0"
    assert await cluster.command("node-a", "SET", "boot", "1") == b"+OK"
    await _wait_until(
        lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a")
        and cluster.nodes["node-a"].progress.snapshot().last_applied_sequence == 1,
        timeout=5,
        message="bootstrap write replicated",
    )
    assert await cluster.command("node-b", "SET", "x", "y") != b"+OK"
    assert cluster.max_concurrent_primaries == 1


async def test_isolated_configured_primary_never_self_promotes(cluster_factory) -> None:
    cluster = cluster_factory()
    await cluster.start("node-a")
    # Absence check: give the node several discovery rounds to misbehave.
    for _ in range(30):
        await asyncio.sleep(0.05)
        assert cluster.nodes["node-a"].election.role == "replica"
    assert (await cluster.command("node-a", "SET", "k", "v")).startswith(b"-ERR")

    await cluster.start("node-b")
    await _wait_until(
        lambda: cluster.primaries() == ["node-a"] and cluster.follows("node-b", "node-a"),
        timeout=10,
        message="bootstrap once a majority is reachable",
    )
    assert cluster.max_concurrent_primaries == 1


@pytest.mark.parametrize(
    "election_algo,write_mode", [("bully", "async"), ("ring", "async"), ("bully", "quorum"), ("ring", "quorum")]
)
async def test_old_primary_restart_rejoins_as_replica_and_catches_up(
    cluster_factory, election_algo: str, write_mode: str
) -> None:
    """The Docker reproduction: failover to node-c, then restart node-a."""
    cluster = cluster_factory(election_algo=election_algo, write_mode=write_mode)
    await _bootstrap(cluster)
    assert await cluster.command("node-a", "SET", "before", "failover") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-c", "node-a"), 5, "pre-failover write replicated")
    initial_epoch = cluster.nodes["node-a"].election.election_epoch

    # 2. Primary failover.
    await cluster.stop("node-a")
    await _wait_until(
        lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"),
        timeout=15,
        message="node-c elected, node-b follows it",
    )
    failover_epoch = cluster.nodes["node-c"].election.election_epoch
    assert failover_epoch > initial_epoch

    # 3. Writes after failover replicate to node-b.
    assert await cluster.command("node-c", "SET", "after", "failover") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-c"), 5, "post-failover write on node-b")
    assert await cluster.command("node-b", "GET", "after") == b"failover"
    report = await cluster.staleness("node-c")
    assert report["node-b"]["status"] == "IN_SYNC"
    assert report["node-a"]["status"] == "UNREACHABLE"

    # 4-6. Restart node-a: no competing primary; it adopts node-c's epoch and data.
    await cluster.start("node-a")
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 10, "node-a follows node-c and catches up")
    assert cluster.primaries() == ["node-c"]
    assert cluster.nodes["node-a"].election.election_epoch == failover_epoch
    assert await cluster.command("node-a", "GET", "before") == b"failover"
    assert await cluster.command("node-a", "GET", "after") == b"failover"

    # 7. The returning node cannot accept primary writes.
    assert (await cluster.command("node-a", "SET", "after", "conflict")).startswith(b"-ERR")
    assert await cluster.command("node-c", "GET", "after") == b"failover"

    # Replication resumes to the rejoined node.
    assert await cluster.command("node-c", "SET", "resumed", "yes") == b"+OK"
    await _wait_until(
        lambda: cluster.in_sync("node-a", "node-c") and cluster.in_sync("node-b", "node-c")
        and cluster.nodes["node-c"].progress.snapshot().last_applied_sequence == 2,
        timeout=10,
        message="replication resumes to every replica",
    )
    assert await cluster.command("node-a", "GET", "resumed") == b"yes"

    # 8. STALENESS reflects real progress, digests included.
    report = await cluster.staleness("node-c")
    leader_digest = report["node-c"]["history_digest"]
    for node_id in ("node-a", "node-b"):
        assert report[node_id]["status"] == "IN_SYNC", report[node_id]
        assert report[node_id]["epoch"] == failover_epoch
        assert report[node_id]["last_applied_sequence"] == 2
        assert report[node_id]["history_digest"] == leader_digest
    assert cluster.max_concurrent_primaries == 1


async def test_fast_restart_before_failure_detection(cluster_factory) -> None:
    """Peers still name node-a leader when it returns empty: it must not lead with lost state."""
    cluster = cluster_factory()
    await _bootstrap(cluster)
    assert await cluster.command("node-a", "SET", "durable", "1") == b"+OK"
    await _wait_until(
        lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 5, "write replicated"
    )

    await cluster.stop("node-a")
    await cluster.start("node-a")  # well inside the 3 s failure-detection window

    await _wait_until(
        lambda: len(cluster.primaries()) == 1
        and cluster.primaries()[0] != "node-a"
        and all(cluster.in_sync(n, cluster.primaries()[0]) for n in NODE_IDS if n != cluster.primaries()[0]),
        timeout=20,
        message="a node with the data is elected and node-a catches up from it",
    )
    for node_id in NODE_IDS:
        assert await cluster.command(node_id, "GET", "durable") == b"1"
    assert cluster.max_concurrent_primaries == 1


async def test_rejoin_under_rpc_failures_reports_honestly(cluster_factory) -> None:
    """Temporary RPC failures neither promote the stale node nor fake synchronization."""
    cluster = cluster_factory()
    await _bootstrap(cluster)
    assert await cluster.command("node-a", "SET", "before", "failover") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-c", "node-a"), 5, "pre-failover write replicated")
    await cluster.stop("node-a")
    await _wait_until(lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"), 15, "failover")
    assert await cluster.command("node-c", "SET", "k", "v") == b"+OK"

    # Restart node-a fully cut off from the cluster.
    cluster.faults.isolate("node-a")
    await cluster.start("node-a")
    for _ in range(30):
        await asyncio.sleep(0.05)
        assert cluster.nodes["node-a"].election.role == "replica"
    assert (await cluster.command("node-a", "SET", "k", "conflict")).startswith(b"-ERR")
    report = await cluster.staleness("node-c")
    assert report["node-a"]["status"] == "UNREACHABLE"

    # Partially heal: node-a can see the leader but cannot fetch its snapshot.
    cluster.faults.heal()
    cluster.faults.isolate("node-a", rpcs={"SyncSnapshot", "ReplicateWrite"})
    await _wait_until(lambda: cluster.follows("node-a", "node-c"), 10, "node-a follows node-c")
    await asyncio.sleep(0.5)
    report = await cluster.staleness("node-c")
    # Reachable now, but missing the pre-failover history: never IN_SYNC.
    assert report["node-a"]["status"] not in ("IN_SYNC", "UNREACHABLE"), report["node-a"]
    assert await cluster.command("node-a", "GET", "before") == b"$-1", "history not synced yet"
    assert not cluster.in_sync("node-a", "node-c")

    cluster.faults.heal()
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 10, "node-a catches up once RPCs recover")
    report = await cluster.staleness("node-c")
    assert report["node-a"]["status"] == "IN_SYNC"
    assert await cluster.command("node-a", "GET", "k") == b"v"
    assert await cluster.command("node-a", "GET", "before") == b"failover"
    assert cluster.max_concurrent_primaries == 1


async def test_partitioned_primary_cannot_resume_after_heal(cluster_factory) -> None:
    """A primary cut off in a minority must follow the newer leader after the partition heals."""
    cluster = cluster_factory()
    await _bootstrap(cluster)
    cluster.faults.isolate("node-a")
    await _wait_until(
        lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"),
        timeout=15,
        message="majority side elects node-c",
    )
    assert (await cluster.command("node-a", "SET", "k", "minority")).startswith(b"-ERR")
    assert await cluster.command("node-c", "SET", "k", "majority") == b"+OK"

    cluster.faults.heal()
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 10, "old primary follows node-c")
    assert cluster.primaries() == ["node-c"]
    assert await cluster.command("node-a", "GET", "k") == b"majority"
    assert cluster.max_concurrent_primaries == 1


@pytest.mark.parametrize("round_", range(3))
async def test_concurrent_startup_produces_single_primary(cluster_factory, round_: int) -> None:
    cluster = cluster_factory(election_algo="ring" if round_ == 2 else "bully")
    order = [NODE_IDS[(i + round_) % 3] for i in range(3)]
    await asyncio.gather(*(cluster.start(nid) for nid in order))
    await _wait_until(
        lambda: len(cluster.primaries()) == 1
        and all(cluster.follows(n, cluster.primaries()[0]) for n in NODE_IDS if n != cluster.primaries()[0]),
        timeout=10,
        message="exactly one primary that every other node follows",
    )
    assert cluster.max_concurrent_primaries == 1


async def test_late_configured_primary_joins_elected_leader(cluster_factory) -> None:
    """If replicas elect a leader before node-a ever starts, node-a must follow it."""
    cluster = cluster_factory()
    await cluster.start("node-b", "node-c")
    await _wait_until(
        lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"),
        timeout=15,
        message="replicas elect node-c when the configured primary never appears",
    )
    assert await cluster.command("node-c", "SET", "k", "v") == b"+OK"

    await cluster.start("node-a")
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 10, "late node-a follows node-c")
    assert cluster.primaries() == ["node-c"]
    assert await cluster.command("node-a", "GET", "k") == b"v"
    assert cluster.max_concurrent_primaries == 1


async def test_replica_restart_rejoins_and_catches_up(cluster_factory) -> None:
    cluster = cluster_factory()
    await _bootstrap(cluster)
    assert await cluster.command("node-a", "SET", "k1", "v1") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 5, "k1 replicated")

    await cluster.stop("node-b")
    assert await cluster.command("node-a", "SET", "k2", "v2") == b"+OK"
    await cluster.start("node-b")
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 10, "restarted replica catches up")
    assert cluster.primaries() == ["node-a"]
    assert await cluster.command("node-b", "GET", "k1") == b"v1"
    assert await cluster.command("node-b", "GET", "k2") == b"v2"
    assert cluster.max_concurrent_primaries == 1


async def test_replica_repairs_gap_stalled_lag_and_divergent_digest(cluster_factory) -> None:
    """Each repair trigger of the reconciler re-syncs the replica from the leader."""
    cluster = cluster_factory()
    await _bootstrap(cluster)
    assert await cluster.command("node-a", "SET", "k1", "v1") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 5, "k1 replicated")

    # Gap: node-b misses seq 2, then receives seq 3.
    cluster.faults.block("node-a", "node-b", {"ReplicateWrite"})
    assert await cluster.command("node-a", "SET", "k2", "v2") == b"+OK"
    cluster.faults.heal()
    assert await cluster.command("node-a", "SET", "k3", "v3") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 10, "gap repaired")
    assert await cluster.command("node-b", "GET", "k2") == b"v2"

    # Stalled lag: node-b misses the last write and nothing follows it.
    cluster.faults.block("node-a", "node-b", {"ReplicateWrite"})
    assert await cluster.command("node-a", "SET", "k4", "v4") == b"+OK"
    assert not cluster.in_sync("node-b", "node-a"), "node-b must report its lag while blocked"
    # Lag alone is normal for async replication: no re-sync before it has
    # persisted for the stalled threshold (3 checks at 0.2 s).
    node_b_seq = cluster.nodes["node-b"].progress.snapshot().last_applied_sequence
    await asyncio.sleep(0.25)
    assert cluster.nodes["node-b"].progress.snapshot().last_applied_sequence == node_b_seq
    cluster.faults.heal()
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 10, "stalled lag repaired")
    assert await cluster.command("node-b", "GET", "k4") == b"v4"

    # Divergent digest at the same sequence.
    snap = cluster.nodes["node-c"].progress.snapshot()
    cluster.nodes["node-c"].progress.restore_from_snapshot(snap.epoch, snap.last_applied_sequence, "0" * 64)
    assert not cluster.in_sync("node-c", "node-a")
    await _wait_until(lambda: cluster.in_sync("node-c", "node-a"), 10, "divergent digest repaired")
    assert cluster.max_concurrent_primaries == 1


@pytest.mark.parametrize("write_mode", ["async", "quorum"])
async def test_asymmetric_partition_fences_stale_primary(cluster_factory, write_mode: str) -> None:
    """Only the a<->c link fails: node-a still reaches node-b (a majority) while node-c is elected.

    This is the one case where two nodes are briefly write-authorized at once
    (in different epochs): node-a has no lease, and keeps its majority via
    node-b until it learns of node-c's epoch from node-b (a rejected write or
    the next reconciler survey). What is guaranteed and asserted here:
    - the two authorized primaries never share an epoch, and the overlap is bounded;
    - quorum mode: once node-b follows the newer epoch node-a commits nothing;
    - async mode: node-a may acknowledge writes in that window, but none of
      them reach the new leader and they are discarded when node-a rejoins.
    """
    cluster = cluster_factory(write_mode=write_mode)
    await _bootstrap(cluster)
    cluster.faults.cut("node-a", "node-c")
    await _wait_until(
        lambda: "node-c" in cluster.primaries() and cluster.follows("node-b", "node-c"),
        timeout=15,
        message="node-c elected with node-b's vote",
    )

    stale_acks: list[str] = []
    for i in range(20):
        if cluster.nodes["node-a"].election.role != "primary":
            break
        if await cluster.command("node-a", "SET", f"stale{i}", "x") == b"+OK":
            stale_acks.append(f"stale{i}")
        await asyncio.sleep(0.05)
    await _wait_until(
        lambda: "node-a" not in cluster.write_authorized(), 5, "stale primary fences itself"
    )
    await _wait_until(lambda: cluster.nodes["node-a"].election.role == "replica", 5, "stale primary steps down")
    if write_mode == "quorum":
        assert stale_acks == [], "a minority-epoch primary must not commit quorum writes"

    cluster.faults.heal()
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 10, "node-a follows node-c after heal")
    assert cluster.primaries() == ["node-c"]
    for key in stale_acks:
        assert await cluster.command("node-c", "GET", key) == b"$-1", "stale write must not reach the leader"
        assert await cluster.command("node-a", "GET", key) == b"$-1", "stale write discarded on rejoin"
    assert not cluster.same_epoch_overlap
    assert cluster.overlap_seconds < 3.0, cluster.overlap_seconds


async def test_rejoin_converges_after_write_burst(cluster_factory) -> None:
    """Concurrent writes during rejoin cause gaps; the replica converges once they stop."""
    cluster = cluster_factory()
    await _bootstrap(cluster)
    await cluster.stop("node-a")
    await _wait_until(lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"), 15, "failover")

    async def burst() -> list[str]:
        keys = []
        for i in range(60):
            key = f"burst{i}"
            if await cluster.command("node-c", "SET", key, str(i)) == b"+OK":
                keys.append(key)
            await asyncio.sleep(0.02)
        return keys

    writer = asyncio.create_task(burst())
    await cluster.start("node-a")
    keys = await writer
    assert len(keys) == 60
    await _wait_until(
        lambda: cluster.in_sync("node-a", "node-c") and cluster.in_sync("node-b", "node-c"),
        15,
        "replicas converge after the burst",
    )
    for i, key in enumerate(keys):
        assert await cluster.command("node-a", "GET", key) == str(i).encode()
    assert cluster.max_concurrent_primaries == 1


async def test_ring_concurrent_circuits_never_lower_the_epoch() -> None:
    """Two initiators complete two circuits for the same winner; the older epoch must lose.

    Both circuits pass the initial obsolete-epoch check before either sets the
    epoch, because the pre-leadership catch-up awaits a peer survey. The newer
    circuit then promotes first; the older one must not move the epoch back.
    """
    from miniredis.election.ring import RingElection

    dead_port = _free_ports(1)[0]
    gate = WriteAdmissionGate(can_serve_writes=False)
    progress = ReplicationProgress("node-c", role="replica", epoch=0)
    election = RingElection(
        node_id="node-c",
        tcp_address="127.0.0.1:1",
        grpc_address="127.0.0.1:2",
        peers={"node-a": PeerEndpoint("node-a", f"127.0.0.1:{dead_port}", "127.0.0.1:3")},
        dispatcher=Dispatcher(Store(), PubSub(pubsub_message_framer), writes_enabled=False, write_gate=gate),
        total_cluster_size=1,
        write_gate=gate,
        rpc_timeout_seconds=0.2,
        progress=progress,
    )

    async def _no_sync(grpc_address: str, *, require_primary: bool = True) -> bool:
        return True

    election.attach_state_syncer(_no_sync)
    newer, older = 2_000, 1_000

    async def complete_circuit(epoch: int, delay: float) -> None:
        await asyncio.sleep(delay)
        await election.handle_ring_election_message(
            sender_id="node-b",
            election_id=f"ring-{epoch}",
            candidate_id="node-c",
            election_epoch=epoch,
            visited_nodes=["node-c"],
            lamport_timestamp=1,
        )

    try:
        await asyncio.gather(complete_circuit(newer, 0.0), complete_circuit(older, 0.0))
        assert election.role == "primary"
        assert election.election_epoch == newer
        assert progress.epoch == newer
    finally:
        replicator = election.dispatcher.replicator
        if replicator is not None:
            await replicator.aclose()
        await election.aclose()
