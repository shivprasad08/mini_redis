"""
Tests for internal node-to-node gRPC replication.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

import pytest
import grpc

from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.cluster import WriteAdmissionGate
from miniredis.protocol import Command
from miniredis.pubsub import PubSub
from miniredis.storage import Store
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.replication.proto import replication_pb2
from miniredis.replication.proto import replication_pb2_grpc


class SlowReplicaServicer(ReplicaServicer):
    """Replica that intentionally responds after a delay."""

    def __init__(self, store: Store, delay_seconds: float) -> None:
        super().__init__(store)
        self._delay_seconds = delay_seconds

    async def ReplicateWrite(  # noqa: N802 - gRPC method name is fixed by proto.
        self,
        request,
        context,
    ):
        await asyncio.sleep(self._delay_seconds)
        return await super().ReplicateWrite(request, context)


class _GrpcContext:
    """Minimal context for direct ReplicaServicer unit tests."""

    def peer(self) -> str:
        return "test-peer"


async def _start_replica_server(
    servicer_factory: Callable[[Store], ReplicaServicer] | None = None,
) -> tuple[Store, str, grpc.aio.Server]:
    store = Store()
    server = grpc.aio.server()
    servicer = servicer_factory(store) if servicer_factory is not None else ReplicaServicer(store)
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return store, f"127.0.0.1:{port}", server


@pytest.fixture
async def replica_server():
    """Start a real gRPC server on an ephemeral port for testing."""
    store = Store()
    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    
    yield store, f"127.0.0.1:{port}"
    
    await server.stop(grace=0)


@pytest.mark.asyncio
async def test_replicate_set(replica_server: tuple[Store, str]) -> None:
    store, target = replica_server
    
    replicator = Replicator([target], total_cluster_size=2)
    
    # Fire and forget propagate
    await replicator.propagate("SET", b"mykey", b"myval", 60)
    
    # Wait briefly for background network task to reach replica
    await asyncio.sleep(0.1)
    
    assert store.get(b"mykey") == b"myval"


@pytest.mark.asyncio
async def test_replicate_del(replica_server: tuple[Store, str]) -> None:
    store, target = replica_server
    store.set(b"todel", b"val")
    
    replicator = Replicator([target], total_cluster_size=2)
    await replicator.propagate("DEL", b"todel")
    
    await asyncio.sleep(0.1)
    assert store.get(b"todel") is None


@pytest.mark.asyncio
async def test_replicate_expire(replica_server: tuple[Store, str]) -> None:
    store, target = replica_server
    store.set(b"mykey", b"val")
    
    replicator = Replicator([target], total_cluster_size=2)
    await replicator.propagate("EXPIRE", b"mykey", ttl_seconds=10)
    
    await asyncio.sleep(0.1)
    
    # Access private _expiries just to verify it was set, 
    # since public get() doesn't expose the absolute TTL.
    assert b"mykey" in store._expiries


@pytest.mark.asyncio
async def test_unreachable_replica_doesnt_raise(caplog: pytest.LogCaptureFixture) -> None:
    """Test that a dead replica doesn't crash the primary's write path."""
    replicator = Replicator(["127.0.0.1:9999"], total_cluster_size=2)
    
    # This should return immediately and not raise
    await replicator.propagate("SET", b"k", b"v")
    
    # Let the background task run and timeout (timeout is 2.0s)
    await asyncio.sleep(2.2)
    
    # We should have logged a warning about the unreachable replica
    assert any("Failed to replicate sequence" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_quorum_write_succeeds_with_majority() -> None:
    stores: list[Store] = []
    servers: list[grpc.aio.Server] = []
    targets: list[str] = []

    try:
        for _ in range(3):
            store, target, server = await _start_replica_server()
            stores.append(store)
            targets.append(target)
            servers.append(server)

        replicator = Replicator(targets, total_cluster_size=4, write_mode="quorum", write_timeout_seconds=0.5)
        assert await replicator.propagate_quorum("SET", b"quorum-key", b"quorum-val", 30)

        await asyncio.sleep(0.05)

        assert all(store.get(b"quorum-key") == b"quorum-val" for store in stores)
    finally:
        await asyncio.gather(*(server.stop(grace=0) for server in servers))


@pytest.mark.asyncio
async def test_quorum_write_fails_without_majority() -> None:
    live_store, live_target, live_server = await _start_replica_server()
    store = Store()
    pubsub = PubSub(pubsub_message_framer)
    replicator = Replicator(
        [live_target, "127.0.0.1:65530", "127.0.0.1:65531"],
        total_cluster_size=4,
        write_mode="quorum",
        write_timeout_seconds=0.2,
    )
    dispatcher = Dispatcher(store, pubsub, replicator)

    try:
        response = await dispatcher.handle_command(
            Command(name="SET", args=[b"lost-key", b"lost-val"]),
            writer=object(),
        )

        assert response == b"-ERR quorum not reached, write not applied\r\n"
        assert store.get(b"lost-key") is None
        assert live_store.get(b"lost-key") == b"lost-val"
    finally:
        await live_server.stop(grace=0)


@pytest.mark.asyncio
async def test_slow_replica_timeout_counts_as_non_ack() -> None:
    fast_store_1, fast_target_1, fast_server_1 = await _start_replica_server()
    fast_store_2, fast_target_2, fast_server_2 = await _start_replica_server()
    slow_store, slow_target, slow_server = await _start_replica_server(
        lambda store: SlowReplicaServicer(store, delay_seconds=0.2)
    )

    replicator = Replicator(
        [fast_target_1, fast_target_2, slow_target],
        total_cluster_size=4,
        write_mode="quorum",
        write_timeout_seconds=0.05,
    )

    try:
        assert await replicator.propagate_quorum("SET", b"timeout-key", b"timeout-val")

        assert fast_store_1.get(b"timeout-key") == b"timeout-val"
        assert fast_store_2.get(b"timeout-key") == b"timeout-val"
        assert slow_store.get(b"timeout-key") in (None, b"timeout-val")
    finally:
        await asyncio.gather(
            fast_server_1.stop(grace=0),
            fast_server_2.stop(grace=0),
            slow_server.stop(grace=0),
        )


@pytest.mark.asyncio
async def test_quorum_mode_waits_while_async_returns_immediately() -> None:
    async_store, async_target, async_server = await _start_replica_server(
        lambda replica_store: SlowReplicaServicer(replica_store, delay_seconds=0.15)
    )
    quorum_store, quorum_target, quorum_server = await _start_replica_server(
        lambda replica_store: SlowReplicaServicer(replica_store, delay_seconds=0.15)
    )

    async_replicator = Replicator([async_target], total_cluster_size=2, write_mode="async", write_timeout_seconds=0.5)
    quorum_replicator = Replicator([quorum_target], total_cluster_size=2, write_mode="quorum", write_timeout_seconds=0.5)

    try:
        async_start = time.perf_counter()
        await async_replicator.propagate_write("SET", b"async-key", b"async-val")
        async_duration = time.perf_counter() - async_start

        quorum_start = time.perf_counter()
        assert await quorum_replicator.propagate_quorum("SET", b"quorum-latency-key", b"quorum-val")
        quorum_duration = time.perf_counter() - quorum_start

        assert async_duration < 0.05
        assert quorum_duration >= 0.12
        assert quorum_store.get(b"quorum-latency-key") == b"quorum-val"
    finally:
        await async_server.stop(grace=0)
        await quorum_server.stop(grace=0)


def test_replicator_rejects_invalid_simulated_latency() -> None:
    for latency in (-1.0, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            Replicator([], total_cluster_size=1, simulate_latency_ms=latency)


@pytest.mark.asyncio
async def test_zero_simulated_latency_preserves_normal_replication() -> None:
    store, target, server = await _start_replica_server()
    replicator = Replicator([target], total_cluster_size=2, simulate_latency_ms=0)

    try:
        await replicator.propagate("SET", b"latency-zero", b"ok")
        await asyncio.sleep(0.1)
        assert store.get(b"latency-zero") == b"ok"
    finally:
        await replicator.aclose()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_nonzero_simulated_latency_delays_quorum_rpc() -> None:
    store, target, server = await _start_replica_server()
    replicator = Replicator(
        [target],
        total_cluster_size=2,
        write_mode="quorum",
        write_timeout_seconds=0.5,
        simulate_latency_ms=60,
    )

    try:
        started = time.perf_counter()
        assert await replicator.propagate_quorum("SET", b"delayed", b"ok")
        elapsed = time.perf_counter() - started

        assert elapsed >= 0.045
        assert store.get(b"delayed") == b"ok"
    finally:
        await replicator.aclose()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_simulated_latency_counts_against_quorum_timeout() -> None:
    store, target, server = await _start_replica_server()
    replicator = Replicator(
        [target],
        total_cluster_size=2,
        write_mode="quorum",
        write_timeout_seconds=0.02,
        simulate_latency_ms=80,
    )

    try:
        assert await replicator.propagate_quorum("SET", b"too-slow", b"nope") is False
        assert store.get(b"too-slow") is None
    finally:
        await replicator.aclose()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_failed_quorum_recovers_after_connectivity_returns() -> None:
    stores: list[Store] = []
    servers: list[grpc.aio.Server] = []
    targets: list[str] = []
    blocked = True

    try:
        for _ in range(2):
            store, target, server = await _start_replica_server()
            stores.append(store)
            targets.append(target)
            servers.append(server)

        def rpc_filter(_: str, target: str) -> bool:
            return not blocked

        gate = WriteAdmissionGate()
        replicator = Replicator(
            targets,
            total_cluster_size=3,
            write_mode="quorum",
            write_timeout_seconds=0.2,
            write_gate=gate,
            rpc_filter=rpc_filter,
        )
        dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer), replicator, write_gate=gate)

        failed = await dispatcher.handle_command(
            Command(name="SET", args=[b"recover-key", b"first-attempt"]), object()
        )
        assert failed == b"-ERR quorum not reached, write not applied\r\n"
        assert gate.can_serve_writes is True

        blocked = False
        succeeded = await dispatcher.handle_command(
            Command(name="SET", args=[b"recover-key", b"second-attempt"]), object()
        )
        assert succeeded == b"+OK\r\n"
        await asyncio.sleep(0.05)
        values = [store.get(b"recover-key") for store in stores]
        assert values == [b"second-attempt", b"second-attempt"], values
    finally:
        await asyncio.gather(*(server.stop(grace=0) for server in servers))


@pytest.mark.asyncio
async def test_replica_rejects_duplicates_gaps_and_obsolete_epochs() -> None:
    store = Store()
    servicer = ReplicaServicer(store)
    context = _GrpcContext()

    first = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(
            op_type="SET",
            key="epoch-key",
            value=b"epoch-one",
            sequence=1,
            epoch=10,
        ),
        context,
    )
    duplicate = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(
            op_type="SET",
            key="epoch-key",
            value=b"duplicate",
            sequence=1,
            epoch=10,
        ),
        context,
    )
    gap = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(
            op_type="SET",
            key="epoch-key",
            value=b"gap",
            sequence=3,
            epoch=10,
        ),
        context,
    )
    new_epoch = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(
            op_type="SET",
            key="epoch-key",
            value=b"epoch-two",
            sequence=1,
            epoch=11,
        ),
        context,
    )
    obsolete = await servicer.ReplicateWrite(
        replication_pb2.WriteOp(
            op_type="SET",
            key="epoch-key",
            value=b"obsolete",
            sequence=2,
            epoch=10,
        ),
        context,
    )

    assert first.success is True
    assert duplicate.success is False
    assert gap.success is False
    assert new_epoch.success is True
    assert obsolete.success is False
    assert store.get(b"epoch-key") == b"epoch-two"
