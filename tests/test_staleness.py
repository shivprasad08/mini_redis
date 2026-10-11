"""
Tests for M15 staleness classification, RPC, and STALENESS command.

Covers:
- Classification logic (all statuses).
- GetProgress RPC (request/response, Lamport timestamps, timeout).
- STALENESS command (registered, JSON output, error cases).
- Partition/failover scenarios (LAGGING, DIVERGENT_GAP, UNREACHABLE).
"""

from __future__ import annotations

import asyncio
import json

import grpc
import pytest

from miniredis.logical_clock import LamportClock
from miniredis.protocol import Command, encode_array, encode_bulk_string
from miniredis.pubsub import PubSub
from miniredis.replication.progress import ReplicationProgress, ProgressSnapshot
from miniredis.replication.proto import replication_pb2
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.staleness import (
    STATUS_DIVERGENT_GAP,
    STATUS_DIVERGENT_HISTORY,
    STATUS_FUTURE_EPOCH,
    STATUS_IN_SYNC,
    STATUS_LAGGING,
    STATUS_LOCAL_PRIMARY,
    STATUS_STALE_EPOCH,
    STATUS_UNREACHABLE,
    classify_replica,
    build_cluster_report,
)
from miniredis.storage import Store
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.cluster import WriteAdmissionGate


# ── helpers ──────────────────────────────────────────────────────────

def _make_progress_response(
    node_id="replica",
    role="replica",
    epoch=1,
    last_applied_sequence=10,
    history_digest="abc123",
    gap_detected=False,
    gap_expected_sequence=0,
    gap_observed_sequence=0,
    lamport_timestamp=5,
):
    return replication_pb2.ProgressResponse(
        node_id=node_id,
        role=role,
        epoch=epoch,
        last_applied_sequence=last_applied_sequence,
        history_digest=history_digest,
        gap_detected=gap_detected,
        gap_expected_sequence=gap_expected_sequence,
        gap_observed_sequence=gap_observed_sequence,
        lamport_timestamp=lamport_timestamp,
    )


def _make_primary_snapshot(
    epoch=1,
    last_applied_sequence=10,
    history_digest="abc123",
    node_id="primary",
    gap_detected=False,
) -> ProgressSnapshot:
    return ProgressSnapshot(
        node_id=node_id,
        role="primary",
        epoch=epoch,
        last_applied_sequence=last_applied_sequence,
        history_digest=history_digest,
        gap_detected=gap_detected,
        gap_expected_sequence=0,
        gap_observed_sequence=0,
    )


# ── classify_replica tests ────────────────────────────────────────────

class TestClassifyReplica:
    def test_unreachable_replica(self):
        primary = _make_primary_snapshot()
        result = classify_replica(primary, None)
        assert result["status"] == STATUS_UNREACHABLE

    def test_in_sync(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=10, history_digest="same")
        response = _make_progress_response(epoch=1, last_applied_sequence=10, history_digest="same")
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_IN_SYNC
        assert result["lag"] == 0

    def test_lagging(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=10)
        response = _make_progress_response(epoch=1, last_applied_sequence=7)
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_LAGGING
        assert result["lag"] == 3

    def test_stale_epoch(self):
        primary = _make_primary_snapshot(epoch=5)
        response = _make_progress_response(epoch=3)
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_STALE_EPOCH

    def test_future_epoch(self):
        primary = _make_primary_snapshot(epoch=2)
        response = _make_progress_response(epoch=4)
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_FUTURE_EPOCH

    def test_divergent_gap(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=10)
        response = _make_progress_response(
            epoch=1, last_applied_sequence=5,
            gap_detected=True, gap_expected_sequence=3, gap_observed_sequence=5,
        )
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_DIVERGENT_GAP
        assert result["gap_expected_sequence"] == 3
        assert result["gap_observed_sequence"] == 5

    def test_divergent_history(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=10, history_digest="digest-A")
        response = _make_progress_response(epoch=1, last_applied_sequence=10, history_digest="digest-B")
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_DIVERGENT_HISTORY
        assert result["primary_history_digest"] == "digest-A"
        assert result["replica_history_digest"] == "digest-B"

    def test_in_sync_when_both_digests_empty(self):
        """Empty digest (no tracker) should not be reported as DIVERGENT_HISTORY."""
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=0, history_digest="")
        response = _make_progress_response(epoch=1, last_applied_sequence=0, history_digest="")
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_IN_SYNC

    def test_gap_takes_priority_over_future_epoch(self):
        """A gap detected flag beats FUTURE_EPOCH check."""
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=10)
        response = _make_progress_response(
            epoch=5, last_applied_sequence=10,
            gap_detected=True, gap_expected_sequence=2, gap_observed_sequence=5,
        )
        result = classify_replica(primary, response)
        assert result["status"] == STATUS_DIVERGENT_GAP

    def test_unreachable_status_has_expected_keys(self):
        primary = _make_primary_snapshot()
        result = classify_replica(primary, None)
        assert "node_id" in result
        assert "status" in result
        assert "message" in result


# ── build_cluster_report tests ────────────────────────────────────────

class TestBuildClusterReport:
    def test_primary_is_always_first_node(self):
        primary = _make_primary_snapshot(node_id="p", epoch=1, last_applied_sequence=5)
        report = build_cluster_report(primary, [], [])
        assert report["nodes"][0]["status"] == STATUS_LOCAL_PRIMARY
        assert report["nodes"][0]["node_id"] == "p"

    def test_report_contains_all_replicas(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=5)
        r1 = _make_progress_response(node_id="r1", epoch=1, last_applied_sequence=5, history_digest="")
        r2 = _make_progress_response(node_id="r2", epoch=1, last_applied_sequence=3, history_digest="")
        report = build_cluster_report(primary, ["addr1", "addr2"], [r1, r2])
        node_ids = [n["node_id"] for n in report["nodes"]]
        assert "r1" in node_ids
        assert "r2" in node_ids

    def test_unreachable_replica_included(self):
        primary = _make_primary_snapshot(epoch=1, last_applied_sequence=5)
        report = build_cluster_report(primary, ["dead:9999"], [None])
        assert any(n["status"] == STATUS_UNREACHABLE for n in report["nodes"])

    def test_stale_view_warning_added(self):
        primary = _make_primary_snapshot(epoch=1)
        report = build_cluster_report(primary, [], [], stale_view=True)
        assert "warning" in report
        assert "STALE_VIEW" in report["warning"]

    def test_report_keys(self):
        primary = _make_primary_snapshot(node_id="p", epoch=2, last_applied_sequence=100)
        report = build_cluster_report(primary, [], [])
        assert report["primary_node_id"] == "p"
        assert report["primary_epoch"] == 2
        assert report["primary_last_applied_sequence"] == 100


# ── GetProgress RPC tests ─────────────────────────────────────────────

async def _start_replica_server(
    node_id="test-replica",
    epoch=1,
    seq=0,
) -> tuple[Store, str, grpc.aio.Server, ReplicationProgress]:
    store = Store()
    progress = ReplicationProgress(node_id=node_id, role="replica", epoch=epoch)
    clock = LamportClock()
    server = grpc.aio.server()
    servicer = ReplicaServicer(store, node_id=node_id, lamport_clock=clock, progress=progress)
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return store, f"127.0.0.1:{port}", server, progress


@pytest.mark.asyncio
async def test_get_progress_returns_tracker_state():
    store, addr, server, progress = await _start_replica_server(node_id="rn1", epoch=2)
    try:
        channel = grpc.aio.insecure_channel(addr)
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)
        # Apply a write so the tracker has non-zero state.
        context_mock = type("Ctx", (), {"peer": lambda s: "test"})()
        await ReplicaServicer(store, node_id="rn1", lamport_clock=LamportClock(), progress=progress).ReplicateWrite(
            replication_pb2.WriteOp(op_type="SET", key="k", value=b"v", sequence=1, epoch=2),
            context_mock,
        )
        req = replication_pb2.ProgressRequest(requester_node_id="primary", lamport_timestamp=1)
        resp = await asyncio.wait_for(stub.GetProgress(req), timeout=2.0)
        assert resp.node_id == "rn1"
        assert resp.epoch == 2
        assert resp.lamport_timestamp > 0
        await channel.close()
    finally:
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_get_progress_merges_lamport_timestamp():
    """The servicer must receive+merge the request's Lamport timestamp."""
    store, addr, server, progress = await _start_replica_server(node_id="rn2", epoch=1)
    try:
        channel = grpc.aio.insecure_channel(addr)
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)
        req = replication_pb2.ProgressRequest(requester_node_id="p", lamport_timestamp=100)
        resp = await asyncio.wait_for(stub.GetProgress(req), timeout=2.0)
        # Response clock must be > 100 (merged + ticked).
        assert resp.lamport_timestamp > 100
        await channel.close()
    finally:
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_get_progress_timeout():
    """GetProgress to an unreachable target must time out without crashing."""
    channel = grpc.aio.insecure_channel("127.0.0.1:19999")
    stub = replication_pb2_grpc.ReplicationServiceStub(channel)
    try:
        with pytest.raises((asyncio.TimeoutError, grpc.aio.AioRpcError)):
            await asyncio.wait_for(
                stub.GetProgress(
                    replication_pb2.ProgressRequest(requester_node_id="p", lamport_timestamp=1)
                ),
                timeout=0.1,
            )
    finally:
        await channel.close()


@pytest.mark.asyncio
async def test_collect_progress_returns_none_for_unreachable():
    """collect_progress must return None for an unreachable replica."""
    replicator = Replicator(
        ["127.0.0.1:19998"],
        total_cluster_size=2,
        write_timeout_seconds=0.1,
    )
    try:
        results = await replicator.collect_progress(timeout_seconds=0.2)
        assert results == [None]
    finally:
        await replicator.aclose()


@pytest.mark.asyncio
async def test_collect_progress_live_replica():
    store, addr, server, progress = await _start_replica_server(node_id="rp1", epoch=1)
    try:
        replicator = Replicator([addr], total_cluster_size=2)
        results = await replicator.collect_progress(timeout_seconds=2.0)
        assert len(results) == 1
        assert results[0] is not None
        assert results[0].node_id == "rp1"
        await replicator.aclose()
    finally:
        await server.stop(grace=0)


# ── STALENESS command tests ───────────────────────────────────────────

class TestStalenessCommand:
    def _make_dispatcher(self, writes_enabled=True, replicator=None, progress=None):
        store = Store()
        pubsub = PubSub(pubsub_message_framer)
        gate = WriteAdmissionGate(can_serve_writes=writes_enabled)
        d = Dispatcher(
            store, pubsub,
            replicator=replicator,
            writes_enabled=writes_enabled,
            write_gate=gate,
            progress=progress,
        )
        return d

    def _cmd(self, *args: bytes) -> Command:
        return Command(name="STALENESS", args=list(args))

    @pytest.mark.asyncio
    async def test_staleness_registered_in_dispatcher(self):
        progress = ReplicationProgress(node_id="n1", role="primary")
        d = self._make_dispatcher(writes_enabled=True, progress=progress)
        resp = await d.handle_command(self._cmd(), writer=object())
        # Should be a bulk string (starts with $), not an unknown-command error.
        assert not resp.startswith(b"-ERR unknown command")

    @pytest.mark.asyncio
    async def test_staleness_on_follower_returns_error(self):
        d = self._make_dispatcher(writes_enabled=False)
        resp = await d.handle_command(self._cmd(), writer=object())
        assert resp.startswith(b"-ERR")
        assert b"primary" in resp.lower()

    @pytest.mark.asyncio
    async def test_staleness_extra_args_returns_error(self):
        progress = ReplicationProgress(node_id="n1", role="primary")
        d = self._make_dispatcher(writes_enabled=True, progress=progress)
        resp = await d.handle_command(self._cmd(b"extra"), writer=object())
        assert resp.startswith(b"-ERR")

    @pytest.mark.asyncio
    async def test_staleness_no_replicator_returns_local_report(self):
        progress = ReplicationProgress(node_id="n1", role="primary")
        d = self._make_dispatcher(writes_enabled=True, replicator=None, progress=progress)
        resp = await d.handle_command(self._cmd(), writer=object())
        # Bulk string: $len\r\n<json>\r\n
        assert resp.startswith(b"$")
        # Extract payload: split on first \r\n, take everything after it, strip trailing \r\n
        body = resp.split(b"\r\n", 1)[1].rstrip(b"\r\n")
        report = json.loads(body)
        assert report["primary_node_id"] == "n1"
        assert len(report["nodes"]) == 1
        assert report["nodes"][0]["status"] == STATUS_LOCAL_PRIMARY

    @pytest.mark.asyncio
    async def test_staleness_with_live_replica(self):
        store, addr, server, rep_progress = await _start_replica_server(node_id="replica-1", epoch=1)
        primary_progress = ReplicationProgress(node_id="primary-1", role="primary", epoch=1)
        # Apply one write on the primary.
        primary_progress.record_applied(1, 1, "SET", "k", b"v")

        replicator = Replicator(
            [addr],
            total_cluster_size=2,
            epoch=1,
            node_id="primary-1",
            progress=primary_progress,
        )
        d = self._make_dispatcher(writes_enabled=True, replicator=replicator, progress=primary_progress)
        try:
            resp = await d.handle_command(self._cmd(), writer=object())
            assert resp.startswith(b"$")
            body = resp.split(b"\r\n", 1)[1].rstrip(b"\r\n")
            report = json.loads(body)
            assert report["primary_node_id"] == "primary-1"
            assert len(report["nodes"]) == 2
            # First node is the primary.
            assert report["nodes"][0]["status"] == STATUS_LOCAL_PRIMARY
            # Second is the replica — should be LAGGING (replica applied 0, primary 1).
            assert report["nodes"][1]["status"] in (
                STATUS_LAGGING, STATUS_IN_SYNC, STATUS_UNREACHABLE
            )
        finally:
            await replicator.aclose()
            await server.stop(grace=0)


# ── LAGGING / GAP / UNREACHABLE partition tests ───────────────────────

@pytest.mark.asyncio
async def test_lagging_replica_detected_via_staleness():
    """A replica that has fewer applied writes appears as LAGGING."""
    store_r, addr_r, server_r, progress_r = await _start_replica_server(
        node_id="replica-lag", epoch=1
    )
    primary_progress = ReplicationProgress(node_id="primary-lag", role="primary", epoch=1)

    # Apply 5 writes on the primary tracker, 0 on the replica.
    for i in range(1, 6):
        primary_progress.record_applied(1, i, "SET", f"k{i}", f"v{i}".encode())

    replicator = Replicator([addr_r], total_cluster_size=2, epoch=1, node_id="primary-lag")
    try:
        results = await replicator.collect_progress(timeout_seconds=2.0)
        assert results[0] is not None
        result = classify_replica(primary_progress.snapshot(), results[0])
        assert result["status"] == STATUS_LAGGING
        assert result["lag"] == 5
    finally:
        await replicator.aclose()
        await server_r.stop(grace=0)


@pytest.mark.asyncio
async def test_gap_detected_on_replica_is_reported():
    """A replica with a gap flag reports DIVERGENT_GAP regardless of sequence."""
    store_r, addr_r, server_r, progress_r = await _start_replica_server(
        node_id="replica-gap", epoch=1
    )
    # Manually inject a gap into the replica's tracker.
    progress_r.record_applied(1, 1, "SET", "k", b"v")
    progress_r.record_gap(epoch=1, expected_sequence=2, observed_sequence=5)

    primary_progress = ReplicationProgress(node_id="prim", role="primary", epoch=1)
    for i in range(1, 6):
        primary_progress.record_applied(1, i, "SET", f"k{i}", f"v{i}".encode())

    replicator = Replicator([addr_r], total_cluster_size=2, epoch=1, node_id="prim")
    try:
        results = await replicator.collect_progress(timeout_seconds=2.0)
        assert results[0] is not None
        result = classify_replica(primary_progress.snapshot(), results[0])
        assert result["status"] == STATUS_DIVERGENT_GAP
    finally:
        await replicator.aclose()
        await server_r.stop(grace=0)


@pytest.mark.asyncio
async def test_unreachable_replica_does_not_prevent_report():
    """An unreachable replica must not crash the STALENESS collection."""
    store_r, addr_r, server_r, progress_r = await _start_replica_server(
        node_id="replica-live", epoch=1
    )
    primary_progress = ReplicationProgress(node_id="prim2", role="primary", epoch=1)
    primary_progress.record_applied(1, 1, "SET", "k", b"v")

    # dead_addr is definitely unreachable.
    replicator = Replicator(
        [addr_r, "127.0.0.1:19997"],
        total_cluster_size=3,
        epoch=1,
        node_id="prim2",
    )
    try:
        results = await replicator.collect_progress(timeout_seconds=0.3)
        assert len(results) == 2
        assert results[0] is not None  # live replica responds
        assert results[1] is None      # dead replica → None
    finally:
        await replicator.aclose()
        await server_r.stop(grace=0)


@pytest.mark.asyncio
async def test_divergent_history_classification():
    """Same epoch and sequence but different fingerprints → DIVERGENT_HISTORY."""
    primary = _make_primary_snapshot(epoch=1, last_applied_sequence=3, history_digest="aaaaaa")
    response = _make_progress_response(epoch=1, last_applied_sequence=3, history_digest="bbbbbb")
    result = classify_replica(primary, response)
    assert result["status"] == STATUS_DIVERGENT_HISTORY


@pytest.mark.asyncio
async def test_stale_epoch_classification():
    """Replica in an older epoch is STALE_EPOCH."""
    primary = _make_primary_snapshot(epoch=5)
    response = _make_progress_response(epoch=2)
    result = classify_replica(primary, response)
    assert result["status"] == STATUS_STALE_EPOCH


@pytest.mark.asyncio
async def test_epoch_transition_reflected_in_report():
    """After epoch transition on the primary, classify_replica uses new epoch."""
    progress_p = ReplicationProgress(node_id="p", role="primary", epoch=1)
    progress_p.record_applied(1, 1, "SET", "k", b"v")
    progress_p.advance_epoch(2)
    snap = progress_p.snapshot()
    assert snap.epoch == 2

    # Replica is still on epoch 1 → STALE_EPOCH
    response = _make_progress_response(epoch=1, last_applied_sequence=1)
    result = classify_replica(snap, response)
    assert result["status"] == STATUS_STALE_EPOCH


@pytest.mark.asyncio
async def test_partition_filter_causes_unreachable():
    """When rpc_filter blocks GetProgress, the result is None (unreachable)."""
    store_r, addr_r, server_r, progress_r = await _start_replica_server(
        node_id="replica-filtered", epoch=1
    )
    # Filter blocks all RPCs.
    replicator = Replicator(
        [addr_r],
        total_cluster_size=2,
        epoch=1,
        node_id="prim",
        rpc_filter=lambda rpc_name, target: False,
    )
    try:
        results = await replicator.collect_progress(timeout_seconds=1.0)
        assert results == [None]
    finally:
        await replicator.aclose()
        await server_r.stop(grace=0)
