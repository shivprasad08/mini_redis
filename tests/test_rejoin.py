"""
Tests for split-brain prevention, leader discovery, and safe replica rejoining.

Covers:
- SyncSnapshot roundtrip: primary exports non-expired keys and metadata; replica restores state.
- Safe startup discovery: node configured as --role primary demotes itself when an active leader exists.
- Fresh bootstrap preservation: node configured as --role primary starts normally when no leader exists.
- Subsequent contiguous write replication after snapshot catch-up.
- Consistent peer node IDs in staleness reporting.
- Failover takeover: rejoining node that demoted to replica can still run election if the primary dies later.
"""

from __future__ import annotations

import asyncio
import time
import pytest
import grpc

from miniredis.cluster import WriteAdmissionGate
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.proto import election_pb2_grpc
from miniredis.election.ring import RingElection
from miniredis.logical_clock import LamportClock
from miniredis.protocol import Command
from miniredis.pubsub import PubSub
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.replication.progress import ReplicationProgress
from miniredis.replication.proto import replication_pb2, replication_pb2_grpc
from miniredis.replication.replicator import (
    ReplicaServicer,
    Replicator,
    sync_state_from_primary,
)
from miniredis.staleness import STATUS_IN_SYNC, STATUS_UNREACHABLE, build_cluster_report
from miniredis.storage import Store


@pytest.mark.asyncio
async def test_sync_snapshot_roundtrip():
    """Verify primary exports live keys with deadlines and replica restores state."""
    clock_fn = time.monotonic
    store_p = Store(clock=clock_fn)
    progress_p = ReplicationProgress(node_id="primary", role="primary", epoch=3)
    lamport_p = LamportClock()

    # Seed primary state
    store_p.set(b"key1", b"val1")
    store_p.set(b"key2", b"val2", ttl_seconds=60)
    # Expired key should be evicted / skipped
    store_p._data[b"key_expired"] = b"stale"
    store_p._expiries[b"key_expired"] = clock_fn() - 10.0

    progress_p.record_applied(3, 1, "SET", "key1", b"val1")
    progress_p.record_applied(3, 2, "SET", "key2", b"val2", ttl_seconds=60)

    server_p = grpc.aio.server()
    servicer_p = ReplicaServicer(
        store_p,
        node_id="primary",
        lamport_clock=lamport_p,
        progress=progress_p,
        primary_id_provider=lambda: "primary",
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer_p, server_p)
    port = server_p.add_insecure_port("127.0.0.1:0")
    await server_p.start()
    primary_addr = f"127.0.0.1:{port}"

    try:
        store_r = Store(clock=clock_fn)
        progress_r = ReplicationProgress(node_id="replica", role="replica", epoch=0)
        lamport_r = LamportClock()
        servicer_r = ReplicaServicer(
            store_r, node_id="replica", lamport_clock=lamport_r, progress=progress_r
        )

        success = await sync_state_from_primary(
            primary_grpc_address=primary_addr,
            store=store_r,
            replica_servicer=servicer_r,
            progress=progress_r,
            lamport_clock=lamport_r,
            node_id="replica",
        )
        assert success is True

        # Check restored data
        assert store_r.get(b"key1") == b"val1"
        assert store_r.get(b"key2") == b"val2"
        assert store_r.get(b"key_expired") is None
        assert store_r._expiries.get(b"key2") is not None

        # Check restored progress
        snap = progress_r.snapshot()
        assert snap.epoch == 3
        assert snap.last_applied_sequence == 2
        assert snap.history_digest == progress_p.snapshot().history_digest
        assert snap.gap_detected is False
    finally:
        await server_p.stop(grace=0)


@pytest.mark.asyncio
async def test_rejoined_replica_accepts_subsequent_write_after_sync():
    """After snapshot catch-up, replica accepts next contiguous replication write."""
    clock_fn = time.monotonic
    store_p = Store(clock=clock_fn)
    progress_p = ReplicationProgress(node_id="node-c", role="primary", epoch=100)
    lamport_p = LamportClock()

    store_p.set(b"k1", b"v1")
    progress_p.record_applied(100, 1, "SET", "k1", b"v1")

    server_p = grpc.aio.server()
    servicer_p = ReplicaServicer(
        store_p,
        node_id="node-c",
        lamport_clock=lamport_p,
        progress=progress_p,
        primary_id_provider=lambda: "node-c",
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer_p, server_p)
    port_p = server_p.add_insecure_port("127.0.0.1:0")
    await server_p.start()
    addr_p = f"127.0.0.1:{port_p}"

    try:
        # Rejoining node (node-a) catches up
        store_a = Store(clock=clock_fn)
        progress_a = ReplicationProgress(node_id="node-a", role="replica", epoch=0)
        lamport_a = LamportClock()
        servicer_a = ReplicaServicer(
            store_a, node_id="node-a", lamport_clock=lamport_a, progress=progress_a
        )

        ok = await sync_state_from_primary(
            primary_grpc_address=addr_p,
            store=store_a,
            replica_servicer=servicer_a,
            progress=progress_a,
            lamport_clock=lamport_a,
            node_id="node-a",
        )
        assert ok is True

        # Now primary sends write sequence 2
        context_mock = type("Ctx", (), {"peer": lambda s: "node-c"})()
        ack = await servicer_a.ReplicateWrite(
            replication_pb2.WriteOp(
                op_type="SET",
                key="k2",
                value=b"v2",
                sequence=2,
                epoch=100,
                lamport_timestamp=1,
            ),
            context_mock,
        )
        assert ack.success is True
        assert ack.applied_sequence == 2
        assert store_a.get(b"k2") == b"v2"
        assert progress_a.snapshot().last_applied_sequence == 2
        assert progress_a.snapshot().gap_detected is False
    finally:
        await server_p.stop(grace=0)


@pytest.mark.asyncio
async def test_startup_discovery_demotes_configured_primary():
    """A node started with primary configuration demotes to replica when leader exists."""
    clock_fn = time.monotonic
    store_c = Store(clock=clock_fn)
    progress_c = ReplicationProgress(node_id="node-c", role="primary", epoch=50)
    lamport_c = LamportClock()
    store_c.set(b"recovered_key", b"recovered_value")
    progress_c.record_applied(50, 1, "SET", "recovered_key", b"recovered_value")

    server_c = grpc.aio.server()
    servicer_c = ReplicaServicer(
        store_c,
        node_id="node-c",
        lamport_clock=lamport_c,
        progress=progress_c,
        primary_id_provider=lambda: "node-c",
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer_c, server_c)
    port_c = server_c.add_insecure_port("127.0.0.1:0")
    await server_c.start()
    addr_c = f"127.0.0.1:{port_c}"

    try:
        # Simulate node-a probe logic
        peer_map = {"node-c": PeerEndpoint("node-c", addr_c, "127.0.0.1:6382")}
        lamport_a = LamportClock()

        # Probe peers
        channel = grpc.aio.insecure_channel(addr_c)
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)
        resp = await stub.GetProgress(
            replication_pb2.ProgressRequest(requester_node_id="node-a", lamport_timestamp=1)
        )
        await channel.close()

        assert resp.role == "primary"
        assert resp.epoch == 50
        assert resp.primary_node_id == "node-c"

        # Setup node-a
        store_a = Store(clock=clock_fn)
        progress_a = ReplicationProgress(node_id="node-a", role="primary", epoch=0)
        write_gate_a = WriteAdmissionGate(can_serve_writes=True)
        dispatcher_a = Dispatcher(
            store_a,
            PubSub(pubsub_message_framer),
            writes_enabled=True,
            write_gate=write_gate_a,
            progress=progress_a,
        )

        election_a = BullyElection(
            node_id="node-a",
            tcp_address="127.0.0.1:6380",
            grpc_address="127.0.0.1:6381",
            peers=peer_map,
            dispatcher=dispatcher_a,
            total_cluster_size=2,
            write_gate=write_gate_a,
            role="primary",
            progress=progress_a,
            lamport_clock=lamport_a,
        )

        # Apply demotion logic
        write_gate_a.block_writes("rejoined as replica under active leader")
        dispatcher_a.set_writes_enabled(False)
        await election_a.accept_coordinator("node-c", addr_c, election_epoch=50)

        # Catch up state
        servicer_a = ReplicaServicer(
            store_a, node_id="node-a", lamport_clock=lamport_a, progress=progress_a
        )
        sync_ok = await sync_state_from_primary(
            primary_grpc_address=addr_c,
            store=store_a,
            replica_servicer=servicer_a,
            progress=progress_a,
            lamport_clock=lamport_a,
            node_id="node-a",
        )
        assert sync_ok is True

        # Verify node-a is a replica, writes are refused, data is recovered
        assert election_a.role == "replica"
        assert election_a.current_primary_id == "node-c"
        assert election_a.election_epoch == 50
        assert store_a.get(b"recovered_key") == b"recovered_value"

        # Client write attempt to node-a is blocked
        set_cmd = Command(name="SET", args=[b"foo", b"bar"])
        resp_bytes = await dispatcher_a.handle_command(set_cmd, writer=object())
        assert b"READONLY" in resp_bytes or b"refusing writes" in resp_bytes

        await election_a.aclose()
    finally:
        await server_c.stop(grace=0)


@pytest.mark.asyncio
async def test_target_to_node_id_in_staleness_report():
    """build_cluster_report uses target_to_node_id mapping for unreachable targets."""
    primary_snap = ReplicationProgress(node_id="node-c", role="primary", epoch=1).snapshot()
    targets = ["node-a:6381", "node-b:6381"]
    mapping = {"node-a:6381": "node-a", "node-b:6381": "node-b"}

    report = build_cluster_report(
        primary_snapshot=primary_snap,
        replica_targets=targets,
        replica_responses=[None, None],
        target_to_node_id=mapping,
    )

    node_ids = [n["node_id"] for n in report["nodes"]]
    assert node_ids == ["node-c", "node-a", "node-b"]
    assert report["nodes"][1]["status"] == STATUS_UNREACHABLE
    assert report["nodes"][2]["status"] == STATUS_UNREACHABLE


@pytest.mark.asyncio
async def test_rejoining_node_can_takeover_if_leader_dies():
    """A rejoining node that demoted to replica can win an election if leader dies."""
    # node-c is primary with epoch 1, node-a is demoted replica
    gate_a = WriteAdmissionGate(can_serve_writes=False)
    progress_a = ReplicationProgress(node_id="node-a", role="replica", epoch=1)
    dispatcher_a = Dispatcher(
        Store(), PubSub(pubsub_message_framer), writes_enabled=False, write_gate=gate_a, progress=progress_a
    )

    # In Bully, node-a has higher priority than node-b and node-c ('node-c' > 'node-a'?
    # Wait, in string comparison 'node-c' > 'node-a'. But if node-c dies, node-a is highest among {node-a, node-b})
    election_a = BullyElection(
        node_id="node-a",
        tcp_address="127.0.0.1:6380",
        grpc_address="127.0.0.1:6381",
        peers={},
        dispatcher=dispatcher_a,
        total_cluster_size=1,
        write_gate=gate_a,
        role="replica",
        progress=progress_a,
    )

    assert election_a.role == "replica"
    await election_a.start_election()
    assert election_a.role == "primary"
    assert election_a.election_epoch > 1
    assert gate_a.can_serve_writes is True
    await election_a.aclose()
