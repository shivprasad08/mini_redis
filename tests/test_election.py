"""
Integration tests for Bully-based leader election.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

import grpc
import pytest

from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.cluster import WriteAdmissionGate
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.proto import election_pb2_grpc
from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.protocol import encode_array, encode_bulk_string
from miniredis.pubsub import PubSub
from miniredis.replication.replicator import ReplicaServicer
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.server import run_server
from miniredis.storage import Store


def _make_command(*args: bytes) -> bytes:
    return encode_array([encode_bulk_string(arg) for arg in args])


@dataclass
class NodeHandle:
    node_id: str
    role: str
    tcp_address: str
    grpc_address: str
    store: Store
    dispatcher: Dispatcher
    election: BullyElection
    grpc_server: grpc.aio.Server
    tcp_server: asyncio.AbstractServer
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


async def _open_client(host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection(host, port)


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(interval)


async def _start_node(
    *,
    node_id: str,
    role: str,
    peers: dict[str, PeerEndpoint],
    tcp_address: str,
    grpc_address: str,
    primary_node_id: str | None = None,
    primary_grpc_address: str | None = None,
    write_mode: str = "async",
    enable_detector: bool = True,
) -> NodeHandle:
    store = Store()
    background_tasks: list[asyncio.Task] = []
    pubsub = PubSub(pubsub_message_framer)
    write_gate = WriteAdmissionGate(can_serve_writes=(role == "primary"))
    dispatcher = Dispatcher(store, pubsub, writes_enabled=(role == "primary"), write_gate=write_gate)
    election = BullyElection(
        node_id=node_id,
        tcp_address="",
        grpc_address="",
        peers=peers,
        dispatcher=dispatcher,
        total_cluster_size=len(peers),
        failure_detector=None,
        write_gate=write_gate,
        role=role,
        write_mode=write_mode,
        rpc_timeout_seconds=0.2,
    )

    grpc_server = grpc.aio.server()
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer(node_id=node_id, role=role), grpc_server
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store), grpc_server
    )
    election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, grpc_server)

    grpc_host, grpc_port_text = grpc_address.split(":", 1)
    grpc_port = grpc_server.add_insecure_port(f"{grpc_host}:{grpc_port_text}")
    if grpc_port == 0:
        raise RuntimeError(f"failed to bind gRPC server to {grpc_address}")
    await grpc_server.start()

    tcp_host, tcp_port_text = tcp_address.split(":", 1)
    tcp_server = await run_server(
        host=tcp_host,
        port=int(tcp_port_text),
        handler=dispatcher.handle_command,
        on_disconnect=dispatcher.handle_disconnect,
    )

    failure_detector: Optional[FailureDetector] = None
    if role == "replica" and enable_detector:
        if primary_node_id is None or primary_grpc_address is None:
            raise ValueError("replica nodes need primary ids and addresses when the detector is enabled")

        failure_detector = FailureDetector(
            peer_node_id=primary_node_id,
            peer_address=primary_grpc_address,
            heartbeat_interval_seconds=0.05,
            heartbeat_timeout_seconds=0.02,
            failure_threshold=3,
            sender_id=node_id,
        )
        election.attach_failure_detector(failure_detector)

        #background_tasks: list[asyncio.Task] = []

        def _on_state_change(_: str, __: str, new_state: str) -> None:
            if new_state == "dead":
                background_tasks.append(asyncio.create_task(election.start_election()))

        failure_detector.on_state_change(_on_state_change)
        await failure_detector.start()

    # Fill in the live addresses now that the servers are bound.
    election._tcp_address = tcp_address  # noqa: SLF001 - test harness wiring
    election._grpc_address = grpc_address  # noqa: SLF001 - test harness wiring

    return NodeHandle(
        node_id=node_id,
        role=role,
        tcp_address=tcp_address,
        grpc_address=grpc_address,
        store=store,
        dispatcher=dispatcher,
        election=election,
        grpc_server=grpc_server,
        tcp_server=tcp_server,
        failure_detector=failure_detector,
        background_tasks=background_tasks,
    )


@pytest.mark.asyncio
async def test_primary_death_triggers_highest_priority_replacement() -> None:
    topology = {
        "node-a": ("127.0.0.1:41001", "127.0.0.1:31001"),
        "node-b": ("127.0.0.1:41002", "127.0.0.1:31002"),
        "node-c": ("127.0.0.1:41003", "127.0.0.1:31003"),
    }
    peers = _peer_map(topology)

    primary = await _start_node(
        node_id="node-a",
        role="primary",
        peers=peers,
        tcp_address=topology["node-a"][1],
        grpc_address=topology["node-a"][0],
        write_mode="async",
        enable_detector=False,
    )
    replica_b = await _start_node(
        node_id="node-b",
        role="replica",
        peers=peers,
        tcp_address=topology["node-b"][1],
        grpc_address=topology["node-b"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
    )
    replica_c = await _start_node(
        node_id="node-c",
        role="replica",
        peers=peers,
        tcp_address=topology["node-c"][1],
        grpc_address=topology["node-c"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
    )

    try:
        await primary.stop()

        await _wait_for(lambda: replica_c.election.role == "primary")
        await _wait_for(lambda: replica_b.election.current_primary_id == "node-c")

        assert replica_b.election.current_primary_address == replica_c.tcp_address
        assert replica_b.failure_detector is not None
        assert replica_b.failure_detector.peer_address == replica_c.grpc_address

        reader, writer = await _open_client("127.0.0.1", int(replica_c.tcp_address.split(":")[1]))
        try:
            writer.write(_make_command(b"SET", b"winner-key", b"winner-val"))
            await writer.drain()
            response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            assert response == b"+OK\r\n"
            assert replica_c.store.get(b"winner-key") == b"winner-val"
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        await replica_b.stop()
        await replica_c.stop()


@pytest.mark.asyncio
async def test_concurrent_elections_converge_on_one_winner() -> None:
    topology = {
        "node-a": ("127.0.0.1:42001", "127.0.0.1:52001"),
        "node-b": ("127.0.0.1:42002", "127.0.0.1:52002"),
        "node-c": ("127.0.0.1:42003", "127.0.0.1:52003"),
    }
    peers = _peer_map(topology)

    primary = await _start_node(
        node_id="node-a",
        role="primary",
        peers=peers,
        tcp_address=topology["node-a"][1],
        grpc_address=topology["node-a"][0],
        enable_detector=False,
    )
    replica_b = await _start_node(
        node_id="node-b",
        role="replica",
        peers=peers,
        tcp_address=topology["node-b"][1],
        grpc_address=topology["node-b"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
        enable_detector=False,
    )
    replica_c = await _start_node(
        node_id="node-c",
        role="replica",
        peers=peers,
        tcp_address=topology["node-c"][1],
        grpc_address=topology["node-c"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
        enable_detector=False,
    )

    try:
        await primary.stop()

        await asyncio.gather(
            replica_b.election.start_election(),
            replica_c.election.start_election(),
        )

        await _wait_for(lambda: replica_c.election.role == "primary")
        await _wait_for(lambda: replica_b.election.current_primary_id == "node-c")

        assert replica_b.election.role == "replica"
        assert replica_c.election.current_primary_id == "node-c"
    finally:
        await replica_b.stop()
        await replica_c.stop()


@pytest.mark.asyncio
async def test_late_rejoin_does_not_self_promote_from_stale_state() -> None:
    topology = {
        "node-a": ("127.0.0.1:43001", "127.0.0.1:53001"),
        "node-b": ("127.0.0.1:43002", "127.0.0.1:53002"),
        "node-c": ("127.0.0.1:43003", "127.0.0.1:53003"),
        "node-d": ("127.0.0.1:43004", "127.0.0.1:53004"),
    }
    # node-d hasn't joined the cluster yet at election time — only a/b/c
    # form the real cluster whose majority math must be satisfied.
    core_topology = {k: v for k, v in topology.items() if k != "node-d"}
    core_peers = _peer_map(core_topology)
    peers = _peer_map(topology)

    primary = await _start_node(
        node_id="node-a",
        role="primary",
        peers=core_peers,
        tcp_address=topology["node-a"][1],
        grpc_address=topology["node-a"][0],
        enable_detector=False,
    )
    replica_b = await _start_node(
        node_id="node-b",
        role="replica",
        peers=core_peers,
        tcp_address=topology["node-b"][1],
        grpc_address=topology["node-b"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
    )
    replica_c = await _start_node(
        node_id="node-c",
        role="replica",
        peers=core_peers,
        tcp_address=topology["node-c"][1],
        grpc_address=topology["node-c"][0],
        primary_node_id="node-a",
        primary_grpc_address=primary.grpc_address,
    )

    try:
        await primary.stop()
        await _wait_for(lambda: replica_c.election.role == "primary")

        late_node = await _start_node(
            node_id="node-d",
            role="replica",
            peers=peers,
            tcp_address=topology["node-d"][1],
            grpc_address=topology["node-d"][0],
            primary_node_id="node-a",
            primary_grpc_address=replica_c.grpc_address,
        )
        late_node.election._current_primary_id = "node-a"  # noqa: SLF001 - stale local state for the test
        late_node.election._current_primary_address = primary.tcp_address  # noqa: SLF001

        try:
            assert late_node.failure_detector is not None
            await _wait_for(lambda: late_node.election.role == "replica")
            await asyncio.sleep(0.15)
            assert late_node.election.role == "replica"
            assert late_node.election.current_primary_id == "node-a"
        finally:
            await late_node.stop()
    finally:
        await replica_b.stop()
        await replica_c.stop()