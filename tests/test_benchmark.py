from __future__ import annotations

import asyncio
import json

import pytest

from miniredis.benchmark import (
    AcknowledgedWrite,
    BenchmarkResult,
    percentile,
    render_table,
    run_write_benchmark,
    verify_acknowledged_writes,
)
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.pubsub import PubSub
from miniredis.server import run_server
from miniredis.storage import Store


@pytest.fixture
async def redis_server():
    dispatcher = Dispatcher(Store(), PubSub(pubsub_message_framer))
    server = await run_server(
        host="127.0.0.1",
        port=0,
        handler=dispatcher.handle_command,
        on_disconnect=dispatcher.handle_disconnect,
    )
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield host, port, dispatcher
    finally:
        server.close()
        await server.wait_closed()


def test_percentile_handles_empty_and_small_samples() -> None:
    assert percentile([], 99) == 0.0
    assert percentile([7.0], 50) == 7.0
    assert percentile([1.0, 3.0], 50) == 2.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 99) == pytest.approx(3.97)


def test_percentile_rejects_invalid_percent() -> None:
    with pytest.raises(ValueError):
        percentile([1.0], 101)


@pytest.mark.asyncio
async def test_write_benchmark_records_acknowledged_writes(redis_server) -> None:
    host, port, dispatcher = redis_server

    result, acknowledged = await run_write_benchmark(
        host=host,
        port=port,
        operations=3,
        value_size=4,
        key_prefix="bench",
        mode="async",
        seed=42,
    )

    assert result.attempted_writes == 3
    assert result.successful_writes == 3
    assert result.failed_writes == 0
    assert len(acknowledged) == 3
    assert dispatcher._store.exists(b"bench:0")  # noqa: SLF001


@pytest.mark.asyncio
async def test_write_benchmark_does_not_count_failed_request_as_ack(redis_server) -> None:
    host, port, _ = redis_server

    result, acknowledged = await run_write_benchmark(
        host=host,
        port=port,
        operations=1,
        value_size=1,
        key_prefix="bad",
        mode="quorum",
        seed=1,
        timeout=0.000001,
    )

    assert result.successful_writes == 0
    assert result.failed_writes == 1
    assert acknowledged == []


@pytest.mark.asyncio
async def test_post_failover_verification_counts_missing_and_mismatched(redis_server) -> None:
    host, port, dispatcher = redis_server
    dispatcher._store.set(b"present", b"ok")  # noqa: SLF001
    dispatcher._store.set(b"wrong", b"old")  # noqa: SLF001

    result = await verify_acknowledged_writes(
        host=host,
        port=port,
        acknowledged=[
            AcknowledgedWrite("present", "ok"),
            AcknowledgedWrite("missing", "value"),
            AcknowledgedWrite("wrong", "new"),
        ],
    )

    assert result.checked == 3
    assert result.missing == 1
    assert result.mismatched == 1


def test_result_manifest_is_json_serializable() -> None:
    result = BenchmarkResult(
        mode="async",
        attempted_writes=1,
        successful_writes=1,
        failed_writes=0,
        elapsed_seconds=0.1,
        throughput_writes_per_second=10.0,
        mean_latency_ms=1.0,
        p50_latency_ms=1.0,
        p99_latency_ms=1.0,
    )

    rendered = render_table(result)
    manifest = json.dumps(result.__dict__)

    assert "throughput_writes_s" in rendered
    assert '"successful_writes": 1' in manifest
