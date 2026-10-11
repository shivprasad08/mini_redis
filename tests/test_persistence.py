"""
M20 integration tests: real nodes with persistent hash-chained ledgers.

Every node gets its own directory under pytest's tmp_path; nothing outside the
temporary directory is read or written.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from miniredis.election.election import PeerEndpoint
from miniredis.ledger import LedgerCorruptionError, verify_file
from miniredis.node import MiniRedisNode, NodeConfig
from miniredis.replication.proto import replication_pb2
from tests.test_leader_recovery import NODE_IDS, LocalCluster, _free_ports, _wait_until


class PersistentCluster(LocalCluster):
    def __init__(self, base: Path, fsync_policy: str = "always", **kw) -> None:
        super().__init__(**kw)
        self.base = base
        self.fsync_policy = fsync_policy

    def config(self, node_id: str):
        config = super().config(node_id)
        config.data_dir = str(self.base / node_id)
        config.fsync_policy = self.fsync_policy
        return config

    def ledger_path(self, node_id: str) -> Path:
        return self.base / node_id / "ledger.jsonl"


@pytest.fixture
async def pcluster(tmp_path: Path):
    clusters: list[PersistentCluster] = []

    async def make(**kwargs) -> PersistentCluster:
        cluster = PersistentCluster(tmp_path, **kwargs)
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


async def _single_node(tmp_path: Path, port: int, grpc_port: int, **overrides) -> MiniRedisNode:
    node = MiniRedisNode(NodeConfig(
        node_id="solo", port=port, grpc_port=grpc_port, role="primary",
        data_dir=str(tmp_path / "solo"), reconcile_interval_seconds=0.2, **overrides,
    ))
    await node.start()
    await node.wait_established(10)
    return node


async def _cmd(port: int, *args: str) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(f"*{len(args)}\r\n".encode() + b"".join(
            f"${len(a.encode())}\r\n".encode() + a.encode() + b"\r\n" for a in args))
        await writer.drain()
        header = await asyncio.wait_for(reader.readline(), 5)
        if header.startswith(b"$") and header != b"$-1\r\n":
            return (await reader.readexactly(int(header[1:-2]) + 2))[:-2]
        return header.rstrip(b"\r\n")
    finally:
        writer.close()


async def test_single_node_restart_replays_writes_deletes_and_ttls(tmp_path: Path) -> None:
    port, grpc_port = _free_ports(2)
    node = await _single_node(tmp_path, port, grpc_port)
    assert await _cmd(port, "SET", "keep", "1") == b"+OK"
    assert await _cmd(port, "SET", "short", "2", "EX", "1") == b"+OK"
    assert await _cmd(port, "SET", "doomed", "3") == b"+OK"
    assert await _cmd(port, "DEL", "doomed") == b":1"
    assert await _cmd(port, "EXPIRE", "keep", "300") == b":1"
    keep_deadline = node.store._expiries[b"keep"]  # noqa: SLF001
    short_deadline = node.store._expiries[b"short"]  # noqa: SLF001
    head = node.ledger.head_hash
    epoch = node.election.election_epoch
    digest = node.progress.snapshot().history_digest
    await node.stop()

    await asyncio.sleep(max(0.0, short_deadline - time.time() + 0.1))  # let "short" expire while down
    node = await _single_node(tmp_path, port, grpc_port)
    try:
        assert await _cmd(port, "GET", "keep") == b"1"
        assert await _cmd(port, "GET", "short") == b"$-1", "TTL must not be extended by a restart"
        assert await _cmd(port, "GET", "doomed") == b"$-1", "deletes survive restart"
        assert node.store._expiries[b"keep"] == keep_deadline  # noqa: SLF001
        report = json.loads(await _cmd(port, "VERIFY"))
        assert report["ok"] and report["records"] == 5 and report["head_hash"] == head
        assert report["last_epoch"] == epoch and report["last_sequence"] == 5
        # Replay restored the history fingerprint; the node then bootstrapped a new
        # epoch (single node), which keeps the fingerprint and restarts sequences.
        progress = node.progress.snapshot()
        assert progress.history_digest == digest
        assert progress.epoch == node.election.election_epoch > epoch and progress.last_applied_sequence == 0

        # Restart did not append duplicates; new writes continue the chain (new epoch).
        assert await _cmd(port, "SET", "after", "x") == b"+OK"
        report = json.loads(await _cmd(port, "VERIFY"))
        assert report["records"] == 6 and report["last_sequence"] == 1
        assert report["last_epoch"] == node.election.election_epoch > epoch
    finally:
        await node.stop()


async def test_verify_detects_tampering_and_corrupted_ledger_refuses_start(tmp_path: Path) -> None:
    port, grpc_port = _free_ports(2)
    node = await _single_node(tmp_path, port, grpc_port)
    for i in range(3):
        assert await _cmd(port, "SET", f"k{i}", str(i)) == b"+OK"
    assert json.loads(await _cmd(port, "VERIFY"))["ok"]

    path = tmp_path / "solo" / "ledger.jsonl"
    lines = path.read_bytes().split(b"\n")[:-1]
    forged = json.loads(lines[1])
    forged["value"] = "SEFDS0VE"  # base64("HACKED")
    path.write_bytes(lines[0] + b"\n" + json.dumps(forged).encode() + b"\n" + lines[2] + b"\n")
    reply = await _cmd(port, "VERIFY")
    assert reply.startswith(b"-ERR VERIFY FAILED: record 2:") and b"modified" in reply, reply
    await node.stop()

    tampered = path.read_bytes()
    with pytest.raises(LedgerCorruptionError):
        await _single_node(tmp_path, port, grpc_port)
    assert path.read_bytes() == tampered, "corrupted ledger preserved for investigation"


async def test_replicated_writes_build_identical_chains(pcluster) -> None:
    cluster = await pcluster()
    for i in range(10):
        assert await cluster.command("node-a", "SET", f"k{i}", str(i)) == b"+OK"
    assert await cluster.command("node-a", "DEL", "k3") == b":1"
    assert await cluster.command("node-a", "EXPIRE", "k4", "600") == b":1"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 10, "sync")

    heads = {n: cluster.nodes[n].ledger.head_hash for n in NODE_IDS}
    assert len(set(heads.values())) == 1, heads
    # Replicas may start with a checkpoint (catch-up at join); every logical write
    # record is byte-for-byte the same on all nodes.
    ops = {
        n: [line for line in cluster.ledger_path(n).read_bytes().splitlines() if b'"kind":"op"' in line]
        for n in NODE_IDS
    }
    assert len(ops["node-a"]) == 12
    assert ops["node-a"] == ops["node-b"] == ops["node-c"]
    for n in NODE_IDS:
        report = json.loads(await cluster.command(n, "VERIFY"))
        assert report["ok"] and report["last_sequence"] == 12


async def test_duplicate_replication_message_is_not_logged_twice(pcluster) -> None:
    cluster = await pcluster()
    assert await cluster.command("node-a", "SET", "k", "v") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a"), 5, "sync")
    replica = cluster.nodes["node-b"]
    before = replica.ledger.record_count

    class _Ctx:
        def peer(self) -> str:
            return "test"

    retry = replication_pb2.WriteOp(
        op_type="SET", key="k", value=b"v", sequence=1, epoch=cluster.nodes["node-a"].election.election_epoch,
    )
    ack = await replica.replica_servicer.ReplicateWrite(retry, _Ctx())
    assert not ack.success
    assert replica.ledger.record_count == before
    assert verify_file(cluster.ledger_path("node-b")).records == before


async def test_concurrent_clients_get_contiguous_logged_sequences(pcluster) -> None:
    cluster = await pcluster(fsync_policy="batch")
    replies = await asyncio.gather(*(cluster.command("node-a", "SET", f"c{i}", str(i)) for i in range(50)))
    assert all(r == b"+OK" for r in replies)
    records = [json.loads(line) for line in cluster.ledger_path("node-a").read_bytes().splitlines()]
    assert [r["seq"] for r in records] == list(range(1, 51))
    assert not cluster.nodes["node-a"].progress.snapshot().gap_detected
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 10, "sync")
    assert json.loads(await cluster.command("node-c", "VERIFY"))["last_sequence"] == 50


async def test_failover_rejoin_and_continued_writes_keep_ledgers_consistent(pcluster) -> None:
    cluster = await pcluster()
    for i in range(3):
        assert await cluster.command("node-a", "SET", f"pre{i}", str(i)) == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-c", "node-a"), 5, "sync")

    await cluster.stop("node-a")
    await _wait_until(lambda: cluster.primaries() == ["node-c"] and cluster.follows("node-b", "node-c"), 15, "failover")
    for i in range(2):
        assert await cluster.command("node-c", "SET", f"post{i}", str(i)) == b"+OK"

    await cluster.start("node-a")  # restarts from its own (older) ledger
    await _wait_until(lambda: cluster.in_sync("node-a", "node-c"), 15, "node-a rejoins")
    assert await cluster.command("node-c", "SET", "later", "x") == b"+OK"
    await _wait_until(
        lambda: cluster.in_sync("node-a", "node-c") and cluster.in_sync("node-b", "node-c")
        and cluster.nodes["node-c"].ledger.head_hash == cluster.nodes["node-a"].ledger.head_hash
        == cluster.nodes["node-b"].ledger.head_hash,
        10,
        "chains converge",
    )
    for n in NODE_IDS:
        assert json.loads(await cluster.command(n, "VERIFY"))["ok"]
        assert await cluster.command(n, "GET", "pre0") == b"0"
        assert await cluster.command(n, "GET", "post1") == b"1"
    # node-a's own log shows the post-failover epoch.
    epochs = [json.loads(line)["epoch"] for line in cluster.ledger_path("node-a").read_bytes().splitlines()]
    assert epochs[-1] == cluster.nodes["node-c"].election.election_epoch


async def test_full_cluster_restart_recovers_replicated_data(pcluster) -> None:
    cluster = await pcluster()
    for i in range(5):
        assert await cluster.command("node-a", "SET", f"k{i}", str(i)) == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 10, "sync")
    for n in NODE_IDS:
        await cluster.stop(n)

    await cluster.start(*NODE_IDS)
    await _wait_until(lambda: len(cluster.primaries()) == 1, 20, "a primary is elected from persisted state")
    leader = cluster.primaries()[0]
    await _wait_until(
        lambda: all(cluster.in_sync(n, leader) for n in NODE_IDS if n != leader), 15, "replicas follow")
    for n in NODE_IDS:
        assert [await cluster.command(n, "GET", f"k{i}") for i in range(5)] == [str(i).encode() for i in range(5)]
        assert json.loads(await cluster.command(n, "VERIFY"))["ok"]
    assert await cluster.command(leader, "SET", "after", "restart") == b"+OK"
    assert cluster.max_concurrent_primaries == 1


async def test_snapshot_includes_ledger_heads(pcluster) -> None:
    cluster = await pcluster()
    assert await cluster.command("node-a", "SET", "k", "v") == b"+OK"
    await _wait_until(lambda: cluster.in_sync("node-b", "node-a") and cluster.in_sync("node-c", "node-a"), 5, "sync")
    result = json.loads(await cluster.command("node-b", "SNAPSHOT"))
    assert result["status"] == "COMPLETED"
    heads = {n: v["ledger"]["hash"] for n, v in result["nodes"].items()}
    assert len(set(heads.values())) == 1
