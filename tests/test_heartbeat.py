"""
Tests for heartbeat-based health monitoring.
"""

from __future__ import annotations

import asyncio
import time

import grpc
import pytest

from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2, health_pb2_grpc


async def _start_health_server(node_id: str = "node-a", role: str = "primary") -> tuple[grpc.aio.Server, str]:
    server = grpc.aio.server()
    health_pb2_grpc.add_HealthServiceServicer_to_server(
        HealthServicer(node_id=node_id, role=role), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_health_servicer_responds_to_heartbeat() -> None:
    server, address = await _start_health_server(node_id="primary-1", role="primary")
    channel = grpc.aio.insecure_channel(address)
    stub = health_pb2_grpc.HealthServiceStub(channel)

    try:
        response = await asyncio.wait_for(
            stub.Heartbeat(health_pb2.Ping(sender_id="replica-1", timestamp_ms=123)),
            timeout=2.0,
        )

        assert response.responder_id == "primary-1"
        assert response.role == "primary"
        assert response.timestamp_ms > 0
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_failure_detector_stays_alive_with_reachable_peer() -> None:
    server, address = await _start_health_server(node_id="primary-1", role="primary")
    detector = FailureDetector(
        peer_node_id="primary-1",
        peer_address=address,
        heartbeat_interval_seconds=0.05,
        heartbeat_timeout_seconds=0.05,
        failure_threshold=3,
        sender_id="replica-1",
    )

    try:
        await detector.start()
        await asyncio.sleep(0.18)
        assert detector.state == "alive"
    finally:
        await detector.stop()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_failure_detector_transitions_to_dead_for_unreachable_peer() -> None:
    detector = FailureDetector(
        peer_node_id="missing-primary",
        peer_address="127.0.0.1:65530",
        heartbeat_interval_seconds=0.05,
        heartbeat_timeout_seconds=0.02,
        failure_threshold=3,
        sender_id="replica-1",
    )
    states: list[str] = []

    detector.on_state_change(lambda peer_id, old_state, new_state: states.append(new_state))

    try:
        await detector.start()
        await asyncio.sleep(0.35)
        assert detector.state == "dead"
        assert "suspected" in states
        assert "dead" in states
    finally:
        await detector.stop()


@pytest.mark.asyncio
async def test_failure_detector_detects_live_then_dead_transition() -> None:
    server, address = await _start_health_server(node_id="primary-1", role="primary")
    detector = FailureDetector(
        peer_node_id="primary-1",
        peer_address=address,
        heartbeat_interval_seconds=0.05,
        heartbeat_timeout_seconds=0.05,
        failure_threshold=3,
        sender_id="replica-1",
    )
    states: list[str] = []

    detector.on_state_change(lambda peer_id, old_state, new_state: states.append(new_state))

    try:
        await detector.start()
        await asyncio.sleep(0.14)
        assert detector.state == "alive"

        await server.stop(grace=0)
        await asyncio.sleep(0.45)

        assert detector.state == "dead"
        assert states == ["suspected", "dead"]
    finally:
        await detector.stop()


@pytest.mark.asyncio
async def test_state_change_callback_fires_only_on_transitions() -> None:
    server, address = await _start_health_server(node_id="primary-1", role="primary")
    detector = FailureDetector(
        peer_node_id="primary-1",
        peer_address=address,
        heartbeat_interval_seconds=0.04,
        heartbeat_timeout_seconds=0.04,
        failure_threshold=3,
        sender_id="replica-1",
    )
    events: list[tuple[str, str]] = []

    detector.on_state_change(
        lambda peer_id, old_state, new_state: events.append((old_state, new_state))
    )

    try:
        await detector.start()
        await asyncio.sleep(0.16)
        assert events == []
    finally:
        await detector.stop()
        await server.stop(grace=0)