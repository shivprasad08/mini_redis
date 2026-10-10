"""
Split-brain integration tests and demo harness for mini-redis.

This file uses a small 5-node in-process cluster and a lightweight
partition filter that cuts outgoing gRPC RPCs by node pair. The same
scenario can be executed manually via the module's ``__main__`` block
for a live evaluator demo.
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass
from typing import Callable, Optional

import grpc
import pytest

from miniredis.cluster import ClusterMajorityMonitor, WriteAdmissionGate
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.proto import election_pb2_grpc
from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.protocol import encode_array, encode_bulk_string
from miniredis.pubsub import PubSub
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.server import run_server
from miniredis.storage import Store


def _make_command(*args: bytes) -> bytes:
    return encode_array([encode_bulk_string(arg) for arg in args])


@dataclass(frozen=True, slots=True)
class NodeInfo:
    node_id: str
    grpc_address: str
    tcp_address: str


@dataclass
class NodeHandle:
    node_id: str
    role: str
    info: NodeInfo
    store: Store
    dispatcher: Dispatcher
    election: BullyElection
    majority_monitor: ClusterMajorityMonitor
    grpc_server: grpc.aio.Server
    tcp_server: asyncio.AbstractServer
    failure_detector: Optional[FailureDetector] = None

    async def stop(self) -> None:
        if self.failure_detector is not None:
            await self.failure_detector.stop()
        await self.majority_monitor.stop()
        if self.dispatcher.replicator is not None:
            await self.dispatcher.replicator.aclose()
        await self.election.aclose()
        self.tcp_server.close()
        await self.tcp_server.wait_closed()
        await self.grpc_server.stop(grace=0)


class PartitionHarness:
    """Test/demo-only partition filter for cutting and restoring links."""

    def __init__(self, node_infos: dict[str, NodeInfo]) -> None:
        self._node_infos = dict(node_infos)
        self._blocked_pairs: set[frozenset[str]] = set()

    @property
    def node_infos(self) -> dict[str, NodeInfo]:
        return dict(self._node_infos)

    def cut(self, group_a: set[str], group_b: set[str]) -> None:
        for left in group_a:
            for right in group_b:
                self._blocked_pairs.add(frozenset({left, right}))

    def heal(self) -> None:
        self._blocked_pairs.clear()

    def allows(self, source_node_id: str, target_address: str, rpc_name: str) -> bool:
        target_node_id = self._node_id_for_address(target_address)
        if target_node_id is None:
            return True
        return frozenset({source_node_id, target_node_id}) not in self._blocked_pairs

    def _node_id_for_address(self, address: str) -> Optional[str]:
        for node_id, info in self._node_infos.items():
            if info.grpc_address == address or info.tcp_address == address:
                return node_id
        return None


async def _open_client(host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection(host, port)


async def _wait_for(predicate: Callable[[], bool], timeout: float = 4.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(interval)


def _split_address(address: str) -> tuple[str, int]:
    host, port_text = address.split(":", 1)
    return host, int(port_text)


def _free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _make_node_infos() -> dict[str, NodeInfo]:
    return {
        node_id: NodeInfo(
            node_id,
            f"127.0.0.1:{_free_local_port()}",
            f"127.0.0.1:{_free_local_port()}",
        )
        for node_id in ("node-a", "node-b", "node-c", "node-d", "node-e")
    }


async def _start_node(
    *,
    node_id: str,
    role: str,
    info: NodeInfo,
    peers: dict[str, PeerEndpoint],
    node_infos: dict[str, NodeInfo],
    partition: PartitionHarness,
    primary_node_id: str | None = None,
    primary_grpc_address: str | None = None,
) -> NodeHandle:
    store = Store()
    pubsub = PubSub(pubsub_message_framer)
    write_gate = WriteAdmissionGate(can_serve_writes=(role == "primary"))
    dispatcher = Dispatcher(store, pubsub, writes_enabled=(role == "primary"), write_gate=write_gate)
    rpc_filter = lambda rpc_name, target_address: partition.allows(node_id, target_address, rpc_name)

    election = BullyElection(
        node_id=node_id,
        tcp_address=info.tcp_address,
        grpc_address=info.grpc_address,
        peers=peers,
        dispatcher=dispatcher,
        total_cluster_size=len(node_infos),
        failure_detector=None,
        write_gate=write_gate,
        role=role,
        write_mode="quorum",
        rpc_timeout_seconds=0.2,
        rpc_filter=rpc_filter,
    )

    grpc_server = grpc.aio.server()
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer(node_id=node_id, role=role), grpc_server
    )
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store), grpc_server
    )
    election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, grpc_server)
    grpc_port = grpc_server.add_insecure_port(info.grpc_address)
    if grpc_port == 0:
        raise RuntimeError(f"failed to bind gRPC server to {info.grpc_address}")
    await grpc_server.start()

    majority_monitor = ClusterMajorityMonitor(
        node_id=node_id,
        peer_health_addresses={peer_id: peer.grpc_address for peer_id, peer in peers.items()},
        total_cluster_size=len(node_infos),
        gate=write_gate,
        heartbeat_interval_seconds=0.05,
        heartbeat_timeout_seconds=0.02,
        rpc_filter=rpc_filter,
    )
    if role == "primary":
        majority_monitor.on_state_change(lambda state: election.handle_majority_change(state == "majority"))
        await majority_monitor.start()

    failure_detector: Optional[FailureDetector] = None
    if role == "replica" and primary_node_id is not None and primary_grpc_address is not None:
        failure_detector = FailureDetector(
            peer_node_id=primary_node_id,
            peer_address=primary_grpc_address,
            heartbeat_interval_seconds=0.05,
            heartbeat_timeout_seconds=0.02,
            failure_threshold=3,
            sender_id=node_id,
            rpc_filter=rpc_filter,
        )
        failure_detector.on_state_change(
            lambda _peer_id, _old_state, new_state: asyncio.create_task(election.start_election())
            if new_state == "dead"
            else None
        )
        await failure_detector.start()

    if role == "primary":
        replicator = Replicator(
            [peer.grpc_address for peer_id, peer in peers.items() if peer_id != node_id],
            total_cluster_size=len(node_infos),
            write_mode="quorum",
            write_gate=write_gate,
            rpc_filter=rpc_filter,
        )
        dispatcher.set_replicator(replicator)

    tcp_host, tcp_port = _split_address(info.tcp_address)
    tcp_server = await run_server(
        host=tcp_host,
        port=tcp_port,
        handler=dispatcher.handle_command,
        on_disconnect=dispatcher.handle_disconnect,
    )

    return NodeHandle(
        node_id=node_id,
        role=role,
        info=info,
        store=store,
        dispatcher=dispatcher,
        election=election,
        majority_monitor=majority_monitor,
        grpc_server=grpc_server,
        tcp_server=tcp_server,
        failure_detector=failure_detector,
    )


async def _start_cluster(partition: PartitionHarness) -> dict[str, NodeHandle]:
    node_infos = dict(partition.node_infos)
    peers = {
        node_id: PeerEndpoint(node_id=node_id, grpc_address=info.grpc_address, tcp_address=info.tcp_address)
        for node_id, info in node_infos.items()
    }

    handles: dict[str, NodeHandle] = {}
    for node_id, info in node_infos.items():
        role = "primary" if node_id == "node-a" else "replica"
        primary_node_id = "node-a" if node_id != "node-a" else None
        primary_grpc_address = node_infos["node-a"].grpc_address if node_id != "node-a" else None
        handles[node_id] = await _start_node(
            node_id=node_id,
            role=role,
            info=info,
            peers=peers,
            node_infos=node_infos,
            partition=partition,
            primary_node_id=primary_node_id,
            primary_grpc_address=primary_grpc_address,
        )

    return handles


async def _stop_cluster(handles: dict[str, NodeHandle]) -> None:
    for handle in handles.values():
        await handle.stop()


async def _partition_majority_cluster() -> tuple[dict[str, NodeHandle], PartitionHarness]:
    node_infos = _make_node_infos()
    partition = PartitionHarness(node_infos)
    handles = await _start_cluster(partition)
    return handles, partition


@pytest.mark.asyncio
async def test_majority_side_elects_and_minority_refuses_writes() -> None:
    handles, partition = await _partition_majority_cluster()
    try:
        partition.cut({"node-a", "node-b"}, {"node-c", "node-d", "node-e"})
        await handles["node-a"].stop()

        await handles["node-e"].election.start_election()
        await _wait_for(lambda: handles["node-e"].election.role == "primary")
        await handles["node-c"].election.handle_majority_change(True)
        await handles["node-d"].election.handle_majority_change(True)
        await handles["node-e"].election.handle_majority_change(True)
        await _wait_for(lambda: handles["node-c"].election.current_primary_id == "node-e")
        await _wait_for(lambda: handles["node-b"].dispatcher.write_gate.can_serve_writes is False)

        majority_host, majority_port = _split_address(handles["node-e"].info.tcp_address)
        majority_reader, majority_writer = await _open_client(majority_host, majority_port)
        try:
            majority_writer.write(_make_command(b"SET", b"majority-key", b"majority-val"))
            await majority_writer.drain()
            response = await asyncio.wait_for(majority_reader.read(1024), timeout=2.0)
            assert response == b"+OK\r\n"
        finally:
            majority_writer.close()
            await majority_writer.wait_closed()

        minority_host, minority_port = _split_address(handles["node-b"].info.tcp_address)
        minority_reader, minority_writer = await _open_client(minority_host, minority_port)
        try:
            minority_writer.write(_make_command(b"SET", b"minority-key", b"minority-val"))
            await minority_writer.drain()
            response = await asyncio.wait_for(minority_reader.read(1024), timeout=2.0)
            assert response == b"-ERR minority partition, refusing writes\r\n"
        finally:
            minority_writer.close()
            await minority_writer.wait_closed()
    finally:
        await _stop_cluster({k: v for k, v in handles.items() if k != "node-a"})


@pytest.mark.asyncio
async def test_heal_partition_rejoins_minority_nodes_as_followers() -> None:
    handles, partition = await _partition_majority_cluster()
    try:
        partition.cut({"node-a", "node-b"}, {"node-c", "node-d", "node-e"})
        await handles["node-a"].stop()

        await handles["node-e"].election.start_election()
        await _wait_for(lambda: handles["node-e"].election.role == "primary")

        partition.heal()
        await handles["node-e"].election.start_election()

        await _wait_for(lambda: handles["node-b"].election.current_primary_id == "node-e")
        assert handles["node-b"].election.role == "replica"
        assert handles["node-b"].dispatcher.write_gate.can_serve_writes is False
    finally:
        await _stop_cluster({k: v for k, v in handles.items() if k != "node-a"})


@pytest.mark.asyncio
async def test_isolated_primary_demotes_itself_when_majority_is_lost() -> None:
    handles, partition = await _partition_majority_cluster()
    try:
        partition.cut({"node-a", "node-b"}, {"node-c", "node-d", "node-e"})

        await handles["node-b"].election.start_election()
        await _wait_for(lambda: handles["node-a"].dispatcher.write_gate.can_serve_writes is False)
        await _wait_for(lambda: handles["node-a"].election.role == "replica")

        host, port = _split_address(handles["node-a"].info.tcp_address)
        reader, writer = await _open_client(host, port)
        try:
            writer.write(_make_command(b"SET", b"cut-key", b"cut-val"))
            await writer.drain()
            response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            assert response == b"-ERR minority partition, refusing writes\r\n"
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        await _stop_cluster(handles)


@pytest.mark.asyncio
async def test_highest_priority_minority_node_refuses_promotion() -> None:
    handles, partition = await _partition_majority_cluster()
    try:
        # node-e is the highest-priority node, but it is isolated with only
        # node-d. Priority cannot override the fixed five-node majority rule.
        partition.cut({"node-a", "node-b", "node-c"}, {"node-d", "node-e"})
        await handles["node-e"].election.start_election()

        assert handles["node-e"].election.role == "replica"
        assert handles["node-e"].dispatcher.write_gate.can_serve_writes is False
        assert handles["node-e"].election.current_primary_id == ""
    finally:
        await _stop_cluster(handles)


async def demo_partition_flow() -> None:
    """Run the split-brain scenario manually for a live demo."""
    handles, partition = await _partition_majority_cluster()
    try:
        print("Partitioning into {node-a,node-b} and {node-c,node-d,node-e}")
        partition.cut({"node-a", "node-b"}, {"node-c", "node-d", "node-e"})
        await handles["node-a"].stop()
        await _wait_for(lambda: handles["node-e"].election.role == "primary")
        print("Majority leader:", handles["node-e"].election.node_id)
        print("Minority writes blocked:", not handles["node-b"].dispatcher.write_gate.can_serve_writes)

        print("Healing partition and rebroadcasting coordinator")
        partition.heal()
        await handles["node-e"].election.start_election()
        await _wait_for(lambda: handles["node-b"].election.current_primary_id == "node-e")
        print("Node-b now follows:", handles["node-b"].election.current_primary_id)
    finally:
        await _stop_cluster({k: v for k, v in handles.items() if k != "node-a"})


if __name__ == "__main__":
    asyncio.run(demo_partition_flow())
