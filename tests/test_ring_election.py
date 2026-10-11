"""
Unit and integration tests for Chang-Roberts Ring Election (M18).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

import grpc
import pytest

from miniredis.cluster import WriteAdmissionGate
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.election.election import PeerEndpoint
from miniredis.election.proto import election_pb2_grpc
from miniredis.election.ring import RingElection
from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.logical_clock import LamportClock
from miniredis.protocol import encode_array, encode_bulk_string
from miniredis.pubsub import PubSub
from miniredis.replication.progress import ReplicationProgress
from miniredis.replication.replicator import ReplicaServicer
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.server import run_server
from miniredis.storage import Store


def _make_command(*args: bytes) -> bytes:
    return encode_array([encode_bulk_string(arg) for arg in args])


@dataclass
class RingNodeHandle:
    node_id: str
    role: str
    tcp_address: str
    grpc_address: str
    store: Store
    dispatcher: Dispatcher
    election: RingElection
    grpc_server: grpc.aio.Server
    tcp_server: asyncio.AbstractServer
    progress: ReplicationProgress
    failure_detector: Optional[FailureDetector] = None
    background_tasks: list[asyncio.Task] = field(default_factory=list)

    async def stop(self) -> None:
        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        if self.failure_detector is not None:
            await self.failure_detector.stop()
        if self.dispatcher.replicator is not None:
            await self.dispatcher.replicator.aclose()
        await self.election.aclose()
        self.tcp_server.close()
        await self.tcp_server.wait_closed()
        await self.grpc_server.stop(grace=0)


def _peer_map(nodes: dict[str, tuple[str, str]]) -> dict[str, PeerEndpoint]:
    return {
        node_id: PeerEndpoint(node_id=node_id, grpc_address=grpc_address, tcp_address=tcp_address)
        for node_id, (grpc_address, tcp_address) in nodes.items()
    }


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(interval)


async def _start_ring_node(
    *,
    node_id: str,
    role: str,
    peers: dict[str, PeerEndpoint],
    tcp_address: str,
    grpc_address: str,
    total_cluster_size: int = 3,
    rpc_filter=None,
) -> RingNodeHandle:
    store = Store()
    pubsub = PubSub(pubsub_message_framer)
    write_gate = WriteAdmissionGate(can_serve_writes=(role == "primary"))
    progress = ReplicationProgress(node_id=node_id, role=role, epoch=0)
    lamport = LamportClock()

    dispatcher = Dispatcher(
        store,
        pubsub,
        writes_enabled=(role == "primary"),
        write_gate=write_gate,
        progress=progress,
    )
    election = RingElection(
        node_id=node_id,
        tcp_address=tcp_address,
        grpc_address=grpc_address,
        peers=peers,
        dispatcher=dispatcher,
        total_cluster_size=total_cluster_size,
        write_gate=write_gate,
        role=role,
        rpc_filter=rpc_filter,
        lamport_clock=lamport,
        progress=progress,
    )

    grpc_server = grpc.aio.server()
    election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, grpc_server)
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer(node_id=node_id, role=role, lamport_clock=lamport), grpc_server
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store, node_id=node_id, lamport_clock=lamport, progress=progress),
        grpc_server,
    )

    grpc_server.add_insecure_port(grpc_address)
    await grpc_server.start()

    host, port_str = tcp_address.split(":")
    tcp_server = await run_server(
        host=host,
        port=int(port_str),
        handler=dispatcher.handle_command,
        on_disconnect=dispatcher.handle_disconnect,
    )

    return RingNodeHandle(
        node_id=node_id,
        role=role,
        tcp_address=tcp_address,
        grpc_address=grpc_address,
        store=store,
        dispatcher=dispatcher,
        election=election,
        grpc_server=grpc_server,
        tcp_server=tcp_server,
        progress=progress,
    )


# ── Unit tests for Ring Election ───────────────────────────────────

def test_ring_topology_deterministic():
    """Verify nodes form a deterministic ascending ring order."""
    peers = {
        "node3": PeerEndpoint("node3", "127.0.0.1:5003", "127.0.0.1:6003"),
        "node1": PeerEndpoint("node1", "127.0.0.1:5001", "127.0.0.1:6001"),
    }
    dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer))
    ring_election = RingElection(
        node_id="node2",
        tcp_address="127.0.0.1:6002",
        grpc_address="127.0.0.1:5002",
        peers=peers,
        dispatcher=dispatcher,
        total_cluster_size=3,
    )

    ordered = ring_election._get_ordered_ring()
    assert ordered == ["node1", "node2", "node3"]

    successors = ring_election._get_successor_candidates()
    # From node2, successor order should be node3, then node1
    assert successors == ["node3", "node1"]


@pytest.mark.asyncio
async def test_ring_minority_partition_refuses_promotion():
    """A node that completes a ring circuit with fewer than majority must fail-closed."""
    dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer), writes_enabled=False)
    gate = WriteAdmissionGate(can_serve_writes=False)
    ring_election = RingElection(
        node_id="node2",
        tcp_address="127.0.0.1:6002",
        grpc_address="127.0.0.1:5002",
        peers={},
        dispatcher=dispatcher,
        total_cluster_size=3,  # majority needed: 2
        write_gate=gate,
    )

    # Candidate circles back with only 1 visited node (minority in cluster of 3)
    await ring_election.handle_ring_election_message(
        sender_id="node2",
        election_id="el-1",
        candidate_id="node2",
        election_epoch=100,
        visited_nodes=["node2"],
        lamport_timestamp=5,
    )

    # Must refuse promotion and keep writes blocked
    assert ring_election.role == "replica"
    assert not gate.can_serve_writes
    assert not dispatcher.writes_enabled


# ── Integration tests with real gRPC nodes ─────────────────────────

@pytest.mark.asyncio
async def test_ring_election_3_nodes_highest_wins():
    """In a 3-node cluster, when election starts, the highest node wins."""
    # Free dynamic ports
    addresses = {
        "node1": ("127.0.0.1:41101", "127.0.0.1:41102"),
        "node2": ("127.0.0.1:41103", "127.0.0.1:41104"),
        "node3": ("127.0.0.1:41105", "127.0.0.1:41106"),
    }
    peers_for_all = _peer_map(addresses)

    nodes: dict[str, RingNodeHandle] = {}
    try:
        for nid, (grpc_addr, tcp_addr) in addresses.items():
            peers = {k: v for k, v in peers_for_all.items() if k != nid}
            nodes[nid] = await _start_ring_node(
                node_id=nid,
                role="replica",
                peers=peers,
                tcp_address=tcp_addr,
                grpc_address=grpc_addr,
                total_cluster_size=3,
            )

        # Node1 initiates election
        await nodes["node1"].election.start_election()

        # Wait for highest node (node3) to win and be promoted
        await _wait_for(lambda: nodes["node3"].election.role == "primary", timeout=3.0)

        assert nodes["node3"].election.role == "primary"
        assert nodes["node3"].dispatcher.writes_enabled is True
        assert nodes["node3"].progress.role == "primary"

        # node1 and node2 should accept coordinator and remain replicas
        await _wait_for(lambda: nodes["node1"].election.current_primary_id == "node3", timeout=3.0)
        await _wait_for(lambda: nodes["node2"].election.current_primary_id == "node3", timeout=3.0)

        assert nodes["node1"].election.role == "replica"
        assert nodes["node2"].election.role == "replica"
        assert nodes["node1"].dispatcher.writes_enabled is False

        # Metrics recorded on winner
        metrics = nodes["node3"].election.get_last_election_metrics()
        assert metrics is not None
        assert metrics.algorithm == "ring"
        assert metrics.elected_leader == "node3"
        assert metrics.total_messages > 0

    finally:
        for node in nodes.values():
            await node.stop()


@pytest.mark.asyncio
async def test_ring_election_skip_failed_node():
    """If the highest node is dead, the ring skips it and elects next highest."""
    addresses = {
        "node1": ("127.0.0.1:41201", "127.0.0.1:41202"),
        "node2": ("127.0.0.1:41203", "127.0.0.1:41204"),
        "node3": ("127.0.0.1:41205", "127.0.0.1:41206"),
    }
    peers_for_all = _peer_map(addresses)

    nodes: dict[str, RingNodeHandle] = {}
    try:
        # Only start node1 and node2. node3 is down!
        for nid in ["node1", "node2"]:
            grpc_addr, tcp_addr = addresses[nid]
            peers = {k: v for k, v in peers_for_all.items() if k != nid}
            nodes[nid] = await _start_ring_node(
                node_id=nid,
                role="replica",
                peers=peers,
                tcp_address=tcp_addr,
                grpc_address=grpc_addr,
                total_cluster_size=3,
            )

        # Node1 initiates election. node2 should be highest available!
        await nodes["node1"].election.start_election()

        # Node2 should win and be promoted
        await _wait_for(lambda: nodes["node2"].election.role == "primary", timeout=3.0)

        assert nodes["node2"].election.role == "primary"
        assert nodes["node2"].dispatcher.writes_enabled is True
        assert nodes["node1"].election.role == "replica"

        # Coordinator message is delivered asynchronously via create_task
        await _wait_for(lambda: nodes["node1"].election.current_primary_id == "node2", timeout=3.0)
        assert nodes["node1"].election.current_primary_id == "node2"

    finally:
        for node in nodes.values():
            await node.stop()


@pytest.mark.asyncio
async def test_ring_election_ignores_obsolete_epoch():
    """Messages from older epochs are ignored by ring nodes."""
    dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer))
    ring_election = RingElection(
        node_id="node2",
        tcp_address="127.0.0.1:6002",
        grpc_address="127.0.0.1:5002",
        peers={},
        dispatcher=dispatcher,
        total_cluster_size=3,
    )
    ring_election._election_epoch = 500

    # Message with obsolete epoch 100
    await ring_election.handle_ring_election_message(
        sender_id="node1",
        election_id="el-old",
        candidate_id="node3",
        election_epoch=100,
        visited_nodes=["node1"],
        lamport_timestamp=1,
    )

    # State unchanged
    assert ring_election.election_epoch == 500
    assert ring_election.current_primary_id == ""
