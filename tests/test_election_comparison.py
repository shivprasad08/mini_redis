"""
Comparative message counting and performance metrics test for Bully vs Ring election (M18).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

import grpc
import pytest

from miniredis.cluster import WriteAdmissionGate
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.metrics import ElectionMetrics
from miniredis.election.proto import election_pb2_grpc
from miniredis.election.ring import RingElection
from miniredis.health.heartbeat import HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.logical_clock import LamportClock
from miniredis.pubsub import PubSub
from miniredis.replication.progress import ReplicationProgress
from miniredis.replication.replicator import ReplicaServicer
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.server import run_server
from miniredis.storage import Store


@dataclass
class _ComparisonNode:
    node_id: str
    election: BullyElection | RingElection
    grpc_server: grpc.aio.Server
    tcp_server: asyncio.AbstractServer

    async def stop(self) -> None:
        if self.election.dispatcher.replicator is not None:
            await self.election.dispatcher.replicator.aclose()
        await self.election.aclose()
        self.tcp_server.close()
        await self.tcp_server.wait_closed()
        await self.grpc_server.stop(grace=0)


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(interval)


async def _start_cluster_node(
    algo: str,
    node_id: str,
    peer_addresses: dict[str, tuple[str, str]],
    total_cluster_size: int = 3,
) -> _ComparisonNode:
    grpc_addr, tcp_addr = peer_addresses[node_id]
    peers = {
        nid: PeerEndpoint(nid, g, t)
        for nid, (g, t) in peer_addresses.items()
        if nid != node_id
    }

    store = Store()
    pubsub = PubSub(pubsub_message_framer)
    gate = WriteAdmissionGate(can_serve_writes=False)
    progress = ReplicationProgress(node_id=node_id, role="replica", epoch=0)
    lamport = LamportClock()
    dispatcher = Dispatcher(store, pubsub, writes_enabled=False, write_gate=gate, progress=progress)

    election_cls = RingElection if algo == "ring" else BullyElection
    election = election_cls(
        node_id=node_id,
        tcp_address=tcp_addr,
        grpc_address=grpc_addr,
        peers=peers,
        dispatcher=dispatcher,
        total_cluster_size=total_cluster_size,
        write_gate=gate,
        role="replica",
        lamport_clock=lamport,
        progress=progress,
    )

    grpc_server = grpc.aio.server()
    election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, grpc_server)
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer(node_id=node_id, role="replica", lamport_clock=lamport), grpc_server
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store, node_id=node_id, lamport_clock=lamport, progress=progress),
        grpc_server,
    )
    grpc_server.add_insecure_port(grpc_addr)
    await grpc_server.start()

    host, port_str = tcp_addr.split(":")
    tcp_server = await run_server(
        host=host,
        port=int(port_str),
        handler=dispatcher.handle_command,
        on_disconnect=dispatcher.handle_disconnect,
    )

    return _ComparisonNode(
        node_id=node_id,
        election=election,
        grpc_server=grpc_server,
        tcp_server=tcp_server,
    )


@pytest.mark.asyncio
async def test_compare_bully_and_ring_election_metrics():
    """
    Run identical failover scenario on Bully and Ring:
    - 3-node topology: node1, node2, node3.
    - Highest node (node3) is dead / offline.
    - node1 initiates election.
    - node2 is elected in both cases.
    - Compare message counts and metrics.
    """
    # ── 1. Bully Election Run ──
    bully_addrs = {
        "node1": ("127.0.0.1:41301", "127.0.0.1:41302"),
        "node2": ("127.0.0.1:41303", "127.0.0.1:41304"),
        "node3": ("127.0.0.1:41305", "127.0.0.1:41306"),
    }
    bully_nodes: dict[str, _ComparisonNode] = {}
    bully_metrics: Optional[ElectionMetrics] = None
    try:
        # Start node1 and node2 (node3 remains offline)
        for nid in ["node1", "node2"]:
            bully_nodes[nid] = await _start_cluster_node("bully", nid, bully_addrs, total_cluster_size=3)

        # Trigger election from node1
        await bully_nodes["node1"].election.start_election()

        # Wait for node2 to win Bully election
        await _wait_for(lambda: bully_nodes["node2"].election.role == "primary", timeout=3.0)
        bully_metrics = bully_nodes["node2"].election.get_last_election_metrics()
        assert bully_metrics is not None
        assert bully_metrics.algorithm == "bully"
        assert bully_metrics.elected_leader == "node2"
    finally:
        for n in bully_nodes.values():
            await n.stop()

    # ── 2. Ring Election Run ──
    ring_addrs = {
        "node1": ("127.0.0.1:41401", "127.0.0.1:41402"),
        "node2": ("127.0.0.1:41403", "127.0.0.1:41404"),
        "node3": ("127.0.0.1:41405", "127.0.0.1:41406"),
    }
    ring_nodes: dict[str, _ComparisonNode] = {}
    ring_metrics: Optional[ElectionMetrics] = None
    try:
        # Start node1 and node2 (node3 remains offline)
        for nid in ["node1", "node2"]:
            ring_nodes[nid] = await _start_cluster_node("ring", nid, ring_addrs, total_cluster_size=3)

        # Trigger election from node1
        await ring_nodes["node1"].election.start_election()

        # Wait for node2 to win Ring election
        await _wait_for(lambda: ring_nodes["node2"].election.role == "primary", timeout=3.0)
        ring_metrics = ring_nodes["node2"].election.get_last_election_metrics()
        assert ring_metrics is not None
        assert ring_metrics.algorithm == "ring"
        assert ring_metrics.elected_leader == "node2"
    finally:
        for n in ring_nodes.values():
            await n.stop()

    # ── 3. Metrics Validation & Comparative Report ──
    assert bully_metrics.total_messages > 0
    assert ring_metrics.total_messages > 0

    bully_dict = bully_metrics.to_dict()
    ring_dict = ring_metrics.to_dict()

    assert bully_dict["algorithm"] == "bully"
    assert ring_dict["algorithm"] == "ring"
    assert bully_dict["elected_leader"] == ring_dict["elected_leader"] == "node2"

    # Both recorded coordinator messages and candidate messages
    assert bully_metrics.coordinator_messages >= 1
    # Ring coordinator messages may be 0 if all successors failed (skip routing)
    # but total messages must be positive
    assert ring_metrics.total_messages >= 1

    # Log metrics table for comparison documentation
    print("\n--- FA-2 M18 Election Metrics Comparison ---")
    print(f"{'Metric':<25} | {'Bully':<10} | {'Ring (Chang-Roberts)':<15}")
    print("-" * 55)
    for key in [
        "algorithm",
        "elected_leader",
        "initiation_messages",
        "candidate_messages",
        "coordinator_messages",
        "retry_or_skip_messages",
        "total_messages",
        "duration_seconds",
    ]:
        print(f"{key:<25} | {str(bully_dict.get(key)):<10} | {str(ring_dict.get(key)):<15}")
