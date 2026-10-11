"""
M17 tests: Chandy–Lamport global snapshots over FIFO gRPC links.

Unit tests drive ``SnapshotManager`` instances through a hand-delivered fake
transport, so marker/message interleavings are exact. Integration tests run
three real ``MiniRedisNode`` processes in-process (dynamic ports) and use the
``SNAPSHOT`` command over RESP.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from typing import Any

import pytest

from miniredis.election.election import PeerEndpoint
from miniredis.links import LinkGuard
from miniredis.replication.proto import replication_pb2
from miniredis.snapshot import SnapshotManager, valid_snapshot_id
from tests.test_leader_recovery import NODE_IDS, LocalCluster, _wait_until

# ──────────────────────────────────────────────────────────────────────
# Unit tests with a hand-driven transport
# ──────────────────────────────────────────────────────────────────────


class FakeNet:
    """Per-channel FIFO queues that the test delivers explicitly."""

    def __init__(self, ids: list[str]) -> None:
        self.queues: dict[tuple[str, str], deque] = defaultdict(deque)
        self.managers: dict[str, SnapshotManager] = {}
        self.captures: dict[str, int] = defaultdict(int)
        peers = {i: PeerEndpoint(i, f"{i}:1", f"{i}:2") for i in ids}
        for node in ids:
            self.managers[node] = SnapshotManager(
                node_id=node,
                peers={p: e for p, e in peers.items() if p != node},
                links=_FakeLinks(self, node),
                capture_state=self._capture(node),
                epoch_provider=lambda: 1,
            )

    def _capture(self, node: str):
        def capture() -> dict[str, Any]:
            self.captures[node] += 1
            return {"node": node}

        return capture

    async def deliver(self, sender: str, receiver: str) -> str:
        request, future = self.queues[(sender, receiver)].popleft()
        status = await self.managers[receiver].handle_marker(request.snapshot_id, request.initiator_id, sender)
        future.set_result(replication_pb2.SnapshotMarkerAck(status=status))
        return status


class _FakeLinks:
    def __init__(self, net: FakeNet, sender: str) -> None:
        self._net, self._sender = net, sender

    def get(self, peer_id: str):
        net, sender = self._net, self._sender

        class _Link:
            def submit(self, rpc_name, request, timeout_seconds=0, delay_seconds=0):
                future = asyncio.get_running_loop().create_future()
                net.queues[(sender, peer_id)].append((request, future))
                return future

        return _Link()


SID = "snap-a-0123456789ab"


async def _start(net: FakeNet, node: str = "a") -> None:
    manager = net.managers[node]
    await manager._record(manager._new_snapshot(SID, node))


async def test_marker_rules_channel_state_and_single_recording() -> None:
    net = FakeNet(["a", "b", "c"])
    await _start(net, "a")
    assert net.captures["a"] == 1

    # b's first marker comes from a: b records, channel a->b is empty.
    assert await net.deliver("a", "b") == "recorded"
    b = net.managers["b"].local_snapshot(SID)
    assert net.captures["b"] == 1
    assert b.channels["a"].marker_received and not b.channels["a"].recording
    assert b.channels["c"].recording

    # A message on c->b after b recorded and before c's marker is channel state.
    net.managers["b"].on_channel_message("c", {"sequence": 41})
    assert await net.deliver("a", "c") == "recorded"     # c records and sends markers
    assert await net.deliver("c", "b") == "closed"
    net.managers["b"].on_channel_message("c", {"sequence": 42})  # after the marker: not recorded
    assert [m["sequence"] for m in b.channels["c"].messages] == [41]
    assert b.done.is_set()

    # Duplicate markers are idempotent: no second recording, counted once.
    net.queues[("c", "b")].append((replication_pb2.SnapshotMarkerRequest(snapshot_id=SID, initiator_id="a"),
                                   asyncio.get_running_loop().create_future()))
    assert await net.deliver("c", "b") == "duplicate"
    assert net.captures["b"] == 1 and b.channels["c"].duplicate_markers == 1

    # The rest of the markers complete every node.
    for sender, receiver in (("b", "a"), ("b", "c"), ("c", "a")):
        await net.deliver(sender, receiver)
    assert all(net.managers[n].local_snapshot(SID).done.is_set() for n in "abc")
    assert all(count == 1 for count in net.captures.values())


async def test_messages_before_recording_are_state_not_channel() -> None:
    net = FakeNet(["a", "b"])
    net.managers["b"].on_channel_message("a", {"sequence": 1})  # no snapshot yet
    await _start(net, "a")
    await net.deliver("a", "b")
    assert net.managers["b"].local_snapshot(SID).channels["a"].messages == []


async def test_invalid_unknown_stale_and_busy_markers() -> None:
    net = FakeNet(["a", "b"])
    b = net.managers["b"]
    assert await b.handle_marker("../etc/passwd", "a", "a") == "invalid_id"
    assert await b.handle_marker(SID, "a", "intruder") == "unknown_sender"
    b.release(SID)
    assert await b.handle_marker(SID, "a", "a") == "stale"
    assert not valid_snapshot_id("snap-a-xyz")

    limited = SnapshotManager("b", {"a": PeerEndpoint("a", "a:1", "a:2")}, _FakeLinks(net, "b"),
                              lambda: {}, lambda: 1, max_active=1)
    other = SnapshotManager("c", {"x": PeerEndpoint("x", "x:1", "x:2"), "a": PeerEndpoint("a", "a:1", "a:2")},
                            _FakeLinks(net, "c"), lambda: {}, lambda: 1, max_active=1)
    assert await other.handle_marker("snap-a-aaaaaaaaaaaa", "a", "a") == "recorded"  # still waiting for x
    assert await other.handle_marker("snap-a-bbbbbbbbbbbb", "a", "a") == "busy"
    del limited


def test_link_guard_enforces_per_sender_order() -> None:
    guard = LinkGuard()
    assert guard.accept("a", "inc1", 1) and guard.accept("a", "inc1", 2)
    assert not guard.accept("a", "inc1", 2), "duplicate"
    assert not guard.accept("a", "inc1", 1), "late, out of order"
    assert guard.accept("b", "inc9", 1), "independent channel"
    assert guard.accept("a", "inc2", 1), "sender restarted: new incarnation"
    assert guard.accept("", "", 0), "unstamped messages are not ordered"
    assert guard.rejected == 2


# ──────────────────────────────────────────────────────────────────────
# Integration tests on real nodes
# ──────────────────────────────────────────────────────────────────────


class SnapCluster(LocalCluster):
    def __init__(self, latency_ms: dict[str, float] | None = None, snapshot_timeout_ms: int = 5000, **kw) -> None:
        super().__init__(**kw)
        self.latency_ms = latency_ms or {}
        self.snapshot_timeout_ms = snapshot_timeout_ms

    def config(self, node_id: str):
        config = super().config(node_id)
        config.simulate_latency_ms = self.latency_ms.get(node_id, 0.0)
        config.snapshot_timeout_ms = self.snapshot_timeout_ms
        return config


@pytest.fixture
async def snap_cluster():
    clusters: list[SnapCluster] = []

    async def make(**kwargs) -> SnapCluster:
        cluster = SnapCluster(**kwargs)
        clusters.append(cluster)
        await cluster.start(*NODE_IDS)
        await _wait_until(
            lambda: cluster.primaries() == ["node-a"]
            and cluster.follows("node-b", "node-a")
            and cluster.follows("node-c", "node-a"),
            10,
            "bootstrap",
        )
        return cluster

    yield make
    for cluster in clusters:
        await cluster.close()


async def _snapshot(cluster: SnapCluster, node: str) -> dict:
    reply = await cluster.command(node, "SNAPSHOT")
    assert not reply.startswith(b"-"), reply
    return json.loads(reply)


def assert_consistent_cut(result: dict) -> None:
    """Every write the primary had applied at its recording point is either in a
    replica's recorded state or in flight on primary->replica; nothing else."""
    assert result["status"] == "COMPLETED", result.get("reason")
    nodes = result["nodes"]
    primaries = [n for n, v in nodes.items() if v["role"] == "primary"]
    assert len(primaries) == 1
    primary = nodes[primaries[0]]
    expected = {k: v["value"] for k, v in primary["keys"].items()}
    for node_id, state in nodes.items():
        assert state["epoch"] == primary["epoch"]
        if node_id == primaries[0]:
            continue
        keys = {k: v["value"] for k, v in state["keys"].items()}
        seq = state["last_applied_sequence"]
        assert seq <= primary["last_applied_sequence"], "replica recorded a write the primary had not sent"
        channel = next(c for c in result["channels"] if c["from"] == primaries[0] and c["to"] == node_id)
        for message in channel["messages"]:
            assert message["sequence"] == seq + 1, "channel state must continue the replica state in order"
            seq = message["sequence"]
            if message["op"] == "SET":
                keys[message["key"]] = message["value"]
            elif message["op"] == "DEL":
                keys.pop(message["key"], None)
        assert seq == primary["last_applied_sequence"], f"{node_id}: writes lost or duplicated in the cut"
        assert keys == expected, f"{node_id}: state + channel != primary state"
    for channel in result["channels"]:
        if channel["from"] != primaries[0]:
            assert channel["messages"] == [], "only the primary sends state-changing messages"


async def test_snapshot_of_healthy_cluster(snap_cluster) -> None:
    cluster = await snap_cluster()
    for i in range(5):
        assert await cluster.command("node-a", "SET", f"k{i}", str(i)) == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 5, "sync")

    result = await _snapshot(cluster, "node-a")
    assert result["algorithm"] == "chandy-lamport"
    assert result["participants"] == list(NODE_IDS)
    assert result["summary"] == {"channels": 6, "channels_closed": 6, "in_flight_messages": 0}
    for node_id, state in result["nodes"].items():
        progress = cluster.nodes[node_id].progress.snapshot()
        assert state["status"] == "COMPLETE"
        assert (state["epoch"], state["last_applied_sequence"], state["history_digest"]) == (
            progress.epoch, 5, progress.history_digest)
        assert state["key_count"] == 5
    assert result["epoch"] == cluster.nodes["node-a"].election.election_epoch
    assert_consistent_cut(result)

    # Every node took part in this one snapshot id and released its state.
    assert json.loads(await cluster.command("node-a", "SNAPSHOT", "GET", result["snapshot_id"])) == result
    for node in cluster.nodes.values():
        assert node.snapshots.local_snapshot(result["snapshot_id"]) is None
        assert not node.snapshots.recording_active()


async def test_in_flight_writes_are_recorded_as_channel_state(snap_cluster) -> None:
    """node-a's writes are slow on the wire; node-c's markers reach node-b first,
    so node-b records before those writes arrive and must capture them on a->b."""
    cluster = await snap_cluster(latency_ms={"node-a": 150})
    for i in range(3):
        assert await cluster.command("node-a", "SET", f"k{i}", str(i)) == b"+OK"  # async: acked before delivery

    result = await _snapshot(cluster, "node-c")
    assert_consistent_cut(result)
    assert result["nodes"]["node-a"]["last_applied_sequence"] == 3
    in_flight = {(c["from"], c["to"]): [m["sequence"] for m in c["messages"]] for c in result["channels"]}
    assert in_flight[("node-a", "node-b")] == [1, 2, 3]
    assert in_flight[("node-a", "node-c")] == [1, 2, 3]
    assert result["summary"]["in_flight_messages"] == 6


async def test_snapshots_during_concurrent_writes_are_consistent_cuts(snap_cluster) -> None:
    cluster = await snap_cluster(latency_ms={"node-a": 5})
    stop = asyncio.Event()

    async def writer() -> int:
        i = 0
        while not stop.is_set():
            assert await cluster.command("node-a", "SET", f"w{i % 7}", str(i)) == b"+OK"
            if i % 5 == 4:
                await cluster.command("node-a", "DEL", f"w{(i + 3) % 7}")
            i += 1
        return i

    task = asyncio.create_task(writer())
    try:
        results = []
        for initiator in ("node-a", "node-b", "node-c", "node-a", "node-c"):
            results.append(await _snapshot(cluster, initiator))
    finally:
        stop.set()
        writes = await task
    assert writes > 20
    for result in results:
        assert_consistent_cut(result)
    assert any(r["summary"]["in_flight_messages"] for r in results), "the test should exercise channel state"


async def test_fifo_links_keep_concurrent_writes_in_sequence(snap_cluster) -> None:
    """Before FIFO links, concurrent fire-and-forget RPCs could arrive out of order and
    open replication gaps. Each link now delivers in sequence order."""
    cluster = await snap_cluster(latency_ms={"node-a": 10})
    replies = await asyncio.gather(*(cluster.command("node-a", "SET", f"c{i}", str(i)) for i in range(40)))
    assert all(r == b"+OK" for r in replies)
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 10, "sync")
    for node_id in ("node-b", "node-c"):
        node = cluster.nodes[node_id]
        assert not node.progress.snapshot().gap_detected
        assert node.replica_servicer._link_guard.rejected == 0  # noqa: SLF001
    assert cluster.nodes["node-a"].progress.snapshot().last_applied_sequence == 40
    assert not cluster.nodes["node-a"].progress.snapshot().gap_detected


async def test_snapshot_fails_fast_when_a_node_is_down(snap_cluster) -> None:
    cluster = await snap_cluster()
    await cluster.stop("node-b")
    started = asyncio.get_running_loop().time()
    reply = await cluster.command("node-a", "SNAPSHOT")
    assert reply.startswith(b"-ERR SNAPSHOT") and b"FAILED" in reply and b"node-b" in reply, reply
    assert asyncio.get_running_loop().time() - started < 4.5, "an undeliverable marker fails the snapshot early"
    for node in cluster.nodes.values():
        assert not node.snapshots.recording_active()
        assert not node.snapshots._snapshots, "coordination state cleaned up"  # noqa: SLF001


async def test_snapshot_times_out_when_a_marker_is_lost(snap_cluster) -> None:
    cluster = await snap_cluster(snapshot_timeout_ms=1500)
    cluster.faults.block("node-b", "node-c", {"SnapshotMarker"})
    reply = await cluster.command("node-a", "SNAPSHOT")
    assert reply.startswith(b"-ERR SNAPSHOT") and b"node-b->node-c" in reply, reply
    snapshot_id = reply.split()[2].decode()
    failed = json.loads(await cluster.command("node-a", "SNAPSHOT", "GET", snapshot_id))
    assert failed["status"] == "FAILED"
    closed = {(c["from"], c["to"]): c["marker_received"] for c in failed["channels"]}
    assert closed[("node-b", "node-c")] is False
    assert sum(closed.values()) == 5
    for node in cluster.nodes.values():
        assert not node.snapshots.recording_active()
    cluster.faults.heal()
    assert_consistent_cut(await _snapshot(cluster, "node-a"))  # later snapshots unaffected


async def test_snapshot_command_errors(snap_cluster) -> None:
    cluster = await snap_cluster()
    assert await cluster.command("node-a", "SNAPSHOT", "GET", "nope") == b"-ERR invalid snapshot id"
    assert await cluster.command("node-a", "SNAPSHOT", "GET", "snap-node-a-0123456789ab") == b"-ERR unknown snapshot id"
    assert (await cluster.command("node-a", "SNAPSHOT", "BOGUS")).startswith(b"-ERR syntax error")


async def test_concurrent_snapshots_are_independent(snap_cluster) -> None:
    cluster = await snap_cluster()
    assert await cluster.command("node-a", "SET", "k", "v") == b"+OK"
    first, second = await asyncio.gather(_snapshot(cluster, "node-a"), _snapshot(cluster, "node-b"))
    assert first["snapshot_id"] != second["snapshot_id"]
    assert_consistent_cut(first)
    assert_consistent_cut(second)
