from __future__ import annotations

import asyncio

import grpc
import pytest

from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.proto import election_pb2, election_pb2_grpc
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.health.heartbeat import HealthServicer
from miniredis.health.proto import health_pb2, health_pb2_grpc
from miniredis.logical_clock import LamportClock
from miniredis.pubsub import PubSub
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.storage import Store
from miniredis.timeline import merge_events, parse_lamport_events


@pytest.mark.asyncio
async def test_lamport_clock_tick_receive_and_invalid_values() -> None:
    clock = LamportClock()
    assert await clock.tick() == 1
    assert await clock.receive(7) == 8
    assert await clock.receive(2) == 9

    with pytest.raises(ValueError):
        LamportClock(-1)
    with pytest.raises(ValueError):
        await clock.receive(-1)


@pytest.mark.asyncio
async def test_lamport_clock_is_safe_under_async_concurrency() -> None:
    clock = LamportClock()
    values = await asyncio.gather(*(clock.tick() for _ in range(25)))

    assert sorted(values) == list(range(1, 26))
    assert await clock.current() == 25


@pytest.mark.asyncio
async def test_replication_messages_are_timestamped_and_merged() -> None:
    server_clock = LamportClock()
    client_clock = LamportClock()
    store = Store()
    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
        ReplicaServicer(store, node_id="replica-1", lamport_clock=server_clock), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    replicator = Replicator(
        [f"127.0.0.1:{port}"],
        total_cluster_size=2,
        write_mode="quorum",
        node_id="primary-1",
        lamport_clock=client_clock,
    )

    try:
        assert await replicator.propagate_quorum("SET", b"k", b"v")
        assert store.get(b"k") == b"v"
        assert await client_clock.current() >= 3
        assert await server_clock.current() >= 2
    finally:
        await replicator.aclose()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_heartbeat_request_and_response_carry_lamport_timestamp() -> None:
    server_clock = LamportClock()
    server = grpc.aio.server()
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer("node-a", "primary", lamport_clock=server_clock), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = health_pb2_grpc.HealthServiceStub(channel)

    try:
        response = await stub.Heartbeat(
            health_pb2.Ping(sender_id="node-b", timestamp_ms=1, lamport_timestamp=5)
        )
        assert response.lamport_timestamp >= 7
        assert await server_clock.current() == response.lamport_timestamp
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_election_servicer_updates_clock_and_returns_timestamp() -> None:
    clock = LamportClock()
    store = Store()
    election = BullyElection(
        node_id="node-b",
        tcp_address="127.0.0.1:0",
        grpc_address="127.0.0.1:0",
        peers={"node-a": PeerEndpoint("node-a", "127.0.0.1:1", "127.0.0.1:2")},
        dispatcher=Dispatcher(store, PubSub(pubsub_message_framer), writes_enabled=False),
        total_cluster_size=2,
        role="replica",
        lamport_clock=clock,
    )
    server = grpc.aio.server()
    election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = election_pb2_grpc.ElectionServiceStub(channel)

    try:
        response = await stub.Election(
            election_pb2.ElectionMessage(sender_id="node-a", sender_priority=0, lamport_timestamp=10)
        )
        assert response.lamport_timestamp >= 12
        assert response.acknowledged is True
    finally:
        await channel.close()
        await election.aclose()
        await server.stop(grace=0)


def test_timeline_parses_and_orders_real_log_lines() -> None:
    text = "\n".join(
        [
            "INFO lamport_event ts=3 node=node-c event=replication_recv message=1:1 description=received_SET",
            "INFO lamport_event ts=1 node=node-a event=replication_send message=1:1 description=sent_SET",
            "INFO lamport_event ts=3 node=node-b event=heartbeat_send message=node-a description=probe",
        ]
    )

    events = merge_events(parse_lamport_events(text))

    assert [(event.timestamp, event.node_id) for event in events] == [
        (1, "node-a"),
        (3, "node-b"),
        (3, "node-c"),
    ]
