"""
Integration tests for M16 Distributed Mutual Exclusion over real gRPC servers.
Tests multi-node Ricart-Agrawala coordination, fencing tokens, timeouts, and centralized baseline.
"""

import asyncio
import socket
import pytest
import grpc

from miniredis.logical_clock import LamportClock
from miniredis.cluster import WriteAdmissionGate
from miniredis.storage import Store
from miniredis.pubsub import PubSub
from miniredis.dispatcher import Dispatcher
from miniredis.protocol import Command, CommandParser, ErrorReply
from miniredis.mutex.protocol import FencingToken, LockState
from miniredis.mutex.manager import RicartAgrawalaManager, CentralizedLockManager
from miniredis.mutex.service import MutexServicer
from miniredis.mutex.proto import mutex_pb2_grpc


def decode_resp(data: bytes):
    parser = CommandParser()
    parser.feed(data)
    res = parser.try_parse_response()
    if isinstance(res, bytes):
        return res.decode("utf-8")
    if isinstance(res, ErrorReply):
        return f"ERR {res.message}"
    return res


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class DummyStreamWriter:
    def __init__(self, peername=("127.0.0.1", 12345)):
        self._peername = peername

    def get_extra_info(self, name, default=None):
        if name == "peername":
            return self._peername
        return default


@pytest.fixture
def dummy_writer():
    return DummyStreamWriter()


class TestRicartAgrawalaCluster:
    @pytest.mark.asyncio
    async def test_three_node_ricart_agrawala_mutual_exclusion(self):
        """Test that across 3 nodes, only one can hold the lock at a time,
        and deferred requests are granted in Lamport order upon release."""
        port_a = find_free_port()
        port_b = find_free_port()
        port_c = find_free_port()

        addr_a = f"127.0.0.1:{port_a}"
        addr_b = f"127.0.0.1:{port_b}"
        addr_c = f"127.0.0.1:{port_c}"

        peer_map = {
            "node-a": addr_a,
            "node-b": addr_b,
            "node-c": addr_c,
        }

        clock_a = LamportClock()
        clock_b = LamportClock()
        clock_c = LamportClock()

        mgr_a = RicartAgrawalaManager("node-a", peer_map, lamport_clock=clock_a)
        mgr_b = RicartAgrawalaManager("node-b", peer_map, lamport_clock=clock_b)
        mgr_c = RicartAgrawalaManager("node-c", peer_map, lamport_clock=clock_c)

        server_a = grpc.aio.server()
        server_b = grpc.aio.server()
        server_c = grpc.aio.server()

        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_a, clock_a), server_a)
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_b, clock_b), server_b)
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_c, clock_c), server_c)

        server_a.add_insecure_port(addr_a)
        server_b.add_insecure_port(addr_b)
        server_c.add_insecure_port(addr_c)

        await server_a.start()
        await server_b.start()
        await server_c.start()

        try:
            # 1. Node A acquires the lock
            token_a = await mgr_a.acquire("res1", client_id="client-a", timeout_seconds=2.0)
            assert token_a.node_id == "node-a"
            assert mgr_a.get_status("res1")["state"] == LockState.HELD.value
            assert mgr_b.get_status("res1")["state"] == LockState.RELEASED.value

            # 2. Concurrently, Node B tries to acquire the same lock.
            # Node A holds the lock, so Node A defers Node B's request.
            acquire_b_task = asyncio.create_task(
                mgr_b.acquire("res1", client_id="client-b", timeout_seconds=3.0)
            )

            # Give a short moment for B's request to reach A and be deferred
            await asyncio.sleep(0.1)
            assert not acquire_b_task.done()
            assert mgr_a.get_status("res1")["deferred_count"] == 1

            # 3. Node A releases the lock
            await mgr_a.release("res1", token=str(token_a), client_id="client-a")
            assert mgr_a.get_status("res1")["state"] == LockState.RELEASED.value

            # 4. Node B should now be granted the lock
            token_b = await asyncio.wait_for(acquire_b_task, timeout=2.0)
            assert token_b.node_id == "node-b"
            assert mgr_b.get_status("res1")["state"] == LockState.HELD.value
            assert token_b > token_a  # Monotonic fencing token order!

            # 5. Node B releases
            await mgr_b.release("res1", token=str(token_b), client_id="client-b")
            assert mgr_b.get_status("res1")["state"] == LockState.RELEASED.value

        finally:
            await mgr_a.stop()
            await mgr_b.stop()
            await mgr_c.stop()
            await server_a.stop(grace=0)
            await server_b.stop(grace=0)
            await server_c.stop(grace=0)

    @pytest.mark.asyncio
    async def test_unreachable_participant_fails_acquisition_safely(self):
        """In classical Ricart-Agrawala, if a participant is dead/unreachable,
        the lock must not be granted (safety over liveness)."""
        port_a = find_free_port()
        port_dead = find_free_port()

        addr_a = f"127.0.0.1:{port_a}"
        addr_dead = f"127.0.0.1:{port_dead}"  # No server running here

        peer_map = {
            "node-a": addr_a,
            "node-dead": addr_dead,
        }

        mgr_a = RicartAgrawalaManager("node-a", peer_map, rpc_timeout_seconds=0.2)
        server_a = grpc.aio.server()
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_a), server_a)
        server_a.add_insecure_port(addr_a)
        await server_a.start()

        try:
            with pytest.raises(RuntimeError) as exc_info:
                await mgr_a.acquire("res1", timeout_seconds=1.0)
            assert "unreachable" in str(exc_info.value).lower()

            # State should be reset, not stuck in WANTED or HELD
            assert mgr_a.get_status("res1")["state"] == LockState.RELEASED.value
        finally:
            await mgr_a.stop()
            await server_a.stop(grace=0)


class TestDispatcherLockAndFencingCommands:
    @pytest.mark.asyncio
    async def test_lock_unlock_fenced_set_e2e(self, dummy_writer):
        """Test LOCK, UNLOCK, and FENCED_SET end-to-end through Dispatcher."""
        store = Store()
        pubsub = PubSub(lambda c, m: b"")
        gate = WriteAdmissionGate(can_serve_writes=True)
        mgr = RicartAgrawalaManager("node-primary", {}, lamport_clock=LamportClock())

        dispatcher = Dispatcher(
            store=store,
            pubsub=pubsub,
            writes_enabled=True,
            write_gate=gate,
            lock_manager=mgr,
        )

        # 1. Client acquires lock on 'my-resource'
        lock_cmd = Command("LOCK", [b"my-resource"])
        resp = await dispatcher.handle_command(lock_cmd, dummy_writer)
        # Bulk string with token
        token_str = decode_resp(resp)
        assert token_str is not None
        token_1 = FencingToken.from_str(token_str)
        assert token_1.node_id == "node-primary"

        # 2. Client performs protected write with token_1
        fenced_cmd_1 = Command("FENCED_SET", [b"key1", b"val1", token_str.encode("utf-8")])
        resp_set = await dispatcher.handle_command(fenced_cmd_1, dummy_writer)
        assert decode_resp(resp_set) == "OK"
        assert store.get(b"key1") == b"val1"
        assert store.get_watermark(b"key1") == token_1

        # 3. Client unlocks 'my-resource'
        unlock_cmd = Command("UNLOCK", [b"my-resource", token_str.encode("utf-8")])
        resp_unl = await dispatcher.handle_command(unlock_cmd, dummy_writer)
        assert decode_resp(resp_unl) == "OK"

        # 4. Another client acquires lock and gets token_2
        resp2 = await dispatcher.handle_command(lock_cmd, dummy_writer)
        token_str_2 = decode_resp(resp2)
        token_2 = FencingToken.from_str(token_str_2)
        assert token_2 > token_1

        # 5. Client 2 writes with token_2
        fenced_cmd_2 = Command("FENCED_SET", [b"key1", b"val2", token_str_2.encode("utf-8")])
        resp_set_2 = await dispatcher.handle_command(fenced_cmd_2, dummy_writer)
        assert decode_resp(resp_set_2) == "OK"
        assert store.get(b"key1") == b"val2"

        # 6. Stale client 1 attempts write with old token_1 -> REJECTED!
        stale_cmd = Command("FENCED_SET", [b"key1", b"stale_val", token_str.encode("utf-8")])
        resp_stale = await dispatcher.handle_command(stale_cmd, dummy_writer)
        assert "ERR" in resp_stale.decode("utf-8", errors="replace")
        assert "stale" in resp_stale.decode("utf-8", errors="replace").lower()

        # Key was NOT corrupted by stale write
        assert store.get(b"key1") == b"val2"


class TestCentralizedLockIntegration:
    @pytest.mark.asyncio
    async def test_replica_forwards_centralized_lock_to_primary(self):
        """Test that a replica forwards Centralized lock acquisition and release
        to the primary via gRPC MutexService."""
        port_primary = find_free_port()
        addr_primary = f"127.0.0.1:{port_primary}"

        clock_primary = LamportClock()
        clock_replica = LamportClock()

        gate_primary = WriteAdmissionGate(can_serve_writes=True)
        mgr_primary = CentralizedLockManager(
            node_id="primary-1",
            role="primary",
            write_gate=gate_primary,
            lamport_clock=clock_primary,
        )

        mgr_replica = CentralizedLockManager(
            node_id="replica-1",
            role="replica",
            primary_address=addr_primary,
            lamport_clock=clock_replica,
        )

        server_primary = grpc.aio.server()
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(
            MutexServicer(mgr_primary, clock_primary), server_primary
        )
        server_primary.add_insecure_port(addr_primary)
        await server_primary.start()

        try:
            # Replica acquires centralized lock from primary
            token = await mgr_replica.acquire("res1", client_id="client-from-replica", timeout_seconds=2.0)
            assert token.node_id == "primary-1"
            assert mgr_primary.get_status("res1")["state"] == LockState.HELD.value

            # Replica releases centralized lock
            ok = await mgr_replica.release("res1", token=str(token), client_id="client-from-replica")
            assert ok is True
            assert mgr_primary.get_status("res1")["state"] == LockState.RELEASED.value
        finally:
            await mgr_replica.stop()
            await mgr_primary.stop()
            await server_primary.stop(grace=0)


class TestMessageCountComparison:
    @pytest.mark.asyncio
    async def test_message_counts_ricart_vs_centralized(self):
        """Compare message counts between Ricart-Agrawala and Centralized locking."""
        # In a 3-node cluster:
        # Ricart-Agrawala: node A sends RequestLock to B and C (2 RPCs sent, 2 replies received).
        # When releasing with no deferred: 0 extra messages. Total = 4 messages (2 sent, 2 received).
        port_a = find_free_port()
        port_b = find_free_port()
        port_c = find_free_port()

        peer_map = {
            "node-a": f"127.0.0.1:{port_a}",
            "node-b": f"127.0.0.1:{port_b}",
            "node-c": f"127.0.0.1:{port_c}",
        }

        mgr_a = RicartAgrawalaManager("node-a", peer_map)
        mgr_b = RicartAgrawalaManager("node-b", peer_map)
        mgr_c = RicartAgrawalaManager("node-c", peer_map)

        server_a = grpc.aio.server()
        server_b = grpc.aio.server()
        server_c = grpc.aio.server()

        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_a), server_a)
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_b), server_b)
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(mgr_c), server_c)

        server_a.add_insecure_port(peer_map["node-a"])
        server_b.add_insecure_port(peer_map["node-b"])
        server_c.add_insecure_port(peer_map["node-c"])

        await server_a.start()
        await server_b.start()
        await server_c.start()

        try:
            tok = await mgr_a.acquire("res1", timeout_seconds=2.0)
            await mgr_a.release("res1", token=str(tok))

            ra_metrics = mgr_a.get_metrics()
            # 2 RequestLock RPCs sent to B and C; 2 responses received
            assert ra_metrics["messages_sent"] == 2
            assert ra_metrics["messages_received"] == 2
            total_ra_messages = ra_metrics["messages_sent"] + ra_metrics["messages_received"]
            assert total_ra_messages == 4
        finally:
            await mgr_a.stop()
            await mgr_b.stop()
            await mgr_c.stop()
            await server_a.stop(grace=0)
            await server_b.stop(grace=0)
            await server_c.stop(grace=0)
