from __future__ import annotations

import asyncio

import grpc
import pytest

from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.protocol import Command
from miniredis.pubsub import PubSub
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.server import finite_float
from miniredis.storage import Store
from miniredis.time_sync.clock import SynchronizedClock
from miniredis.time_sync.service import TimeServicer, TimeSynchronizer
from miniredis.time_sync.proto import time_pb2_grpc


class ManualSeconds:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_cristian_offset_calculation_positive_and_negative() -> None:
    clock = SynchronizedClock(source=lambda: 0.0)

    sample = clock.apply_sample(client_send_ms=1000, server_time_ms=1200, client_receive_ms=1100)
    assert sample.rtt_ms == 100
    assert sample.offset_ms == 150
    assert clock.synchronized is True

    sample = clock.apply_sample(client_send_ms=2000, server_time_ms=1800, client_receive_ms=2100)
    assert sample.offset_ms == -250


def test_clock_skew_validation_and_corrected_time() -> None:
    source = ManualSeconds(10.0)
    clock = SynchronizedClock(skew_ms=500, source=source)

    assert clock.raw_time_ms() == 10500
    assert clock.corrected_time_ms() == 10500
    clock.apply_sample(client_send_ms=10500, server_time_ms=10000, client_receive_ms=10520)
    assert clock.corrected_time_ms() == 9990

    assert finite_float("-25.5") == -25.5
    with pytest.raises(ValueError):
        SynchronizedClock(skew_ms=float("inf"))


def test_invalid_sync_sample_is_rejected() -> None:
    clock = SynchronizedClock()
    with pytest.raises(ValueError):
        clock.apply_sample(client_send_ms=20, server_time_ms=10, client_receive_ms=19)


@pytest.mark.asyncio
async def test_time_service_sync_once_updates_replica_offset() -> None:
    primary_clock = SynchronizedClock(source=lambda: 100.0)
    replica_clock = SynchronizedClock(skew_ms=500, source=lambda: 100.0)
    server = grpc.aio.server()
    time_pb2_grpc.add_TimeServiceServicer_to_server(
        TimeServicer(node_id="primary", clock=primary_clock), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    synchronizer = TimeSynchronizer(
        node_id="replica",
        peer_address=f"127.0.0.1:{port}",
        clock=replica_clock,
        timeout_seconds=1.0,
    )

    try:
        sample = await synchronizer.sync_once()
        assert sample.synchronized is True
        assert sample.offset_ms < 0
        assert replica_clock.synchronized is True
    finally:
        await synchronizer.stop()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_time_sync_failure_marks_clock_unsynchronized() -> None:
    clock = SynchronizedClock()
    synchronizer = TimeSynchronizer(
        node_id="replica",
        peer_address="127.0.0.1:65530",
        clock=clock,
        timeout_seconds=0.02,
    )

    try:
        sample = await synchronizer.sync_once()
        assert sample.synchronized is False
        assert clock.synchronized is False
    finally:
        await synchronizer.stop()


@pytest.mark.asyncio
async def test_ttl_replication_uses_common_deadline_not_arrival_ttl() -> None:
    primary_time = ManualSeconds(100.0)
    replica_time = ManualSeconds(100.0)
    primary_store = Store(clock=primary_time)
    replica_store = Store(clock=replica_time)
    dispatcher = Dispatcher(
        primary_store,
        PubSub(pubsub_message_framer),
        clock=primary_time,
    )

    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(replica_store), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    replicator = Replicator([f"127.0.0.1:{port}"], total_cluster_size=2, write_mode="quorum")
    dispatcher.set_replicator(replicator)

    try:
        response = await dispatcher.handle_command(
            Command(name="SET", args=[b"ttl-key", b"value", b"EX", b"5"]),
            object(),
        )
        assert response == b"+OK\r\n"
        assert primary_store._expiries[b"ttl-key"] == pytest.approx(105.0)  # noqa: SLF001
        assert replica_store._expiries[b"ttl-key"] == pytest.approx(105.0)  # noqa: SLF001

        replica_time.advance(5.0)
        assert replica_store.get(b"ttl-key") is None
    finally:
        await replicator.aclose()
        await server.stop(grace=0)
