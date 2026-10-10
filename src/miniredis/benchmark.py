"""Benchmark helpers for mini-redis client-visible write latency.

The runner talks to a live mini-redis node through the normal TCP RESP-like
protocol. It does not mock replication; simulated network delay belongs in
``Replicator`` and is configured on the server.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

from miniredis.protocol import encode_array, encode_bulk_string


def _command(*args: bytes) -> bytes:
    return encode_array([encode_bulk_string(arg) for arg in args])


def percentile(samples: Sequence[float], pct: float) -> float:
    """Return a nearest-rank percentile for latency samples."""
    if not samples:
        return 0.0
    if pct < 0 or pct > 100:
        raise ValueError("pct must be between 0 and 100")
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


@dataclass(frozen=True)
class AcknowledgedWrite:
    key: str
    value: str


@dataclass(frozen=True)
class VerificationResult:
    checked: int
    missing: int
    mismatched: int


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    attempted_writes: int
    successful_writes: int
    failed_writes: int
    elapsed_seconds: float
    throughput_writes_per_second: float
    mean_latency_ms: float
    p50_latency_ms: float
    p99_latency_ms: float
    acknowledged_writes_checked: int = 0
    acknowledged_writes_missing: int = 0
    acknowledged_writes_mismatched: int = 0


async def _round_trip(host: str, port: int, payload: bytes, timeout: float) -> bytes:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(payload)
        await writer.drain()
        return await asyncio.wait_for(reader.read(1024 * 64), timeout=timeout)
    finally:
        writer.close()
        await writer.wait_closed()


async def run_write_benchmark(
    *,
    host: str,
    port: int,
    operations: int,
    value_size: int,
    key_prefix: str,
    mode: str,
    seed: int,
    timeout: float = 2.0,
) -> tuple[BenchmarkResult, list[AcknowledgedWrite]]:
    if operations < 0:
        raise ValueError("operations must be non-negative")
    if value_size < 0:
        raise ValueError("value_size must be non-negative")

    rng = random.Random(seed)
    latencies_ms: list[float] = []
    acknowledged: list[AcknowledgedWrite] = []
    failures = 0
    started = time.perf_counter()

    for index in range(operations):
        key = f"{key_prefix}:{index}"
        value = "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(value_size))
        payload = _command(b"SET", key.encode("utf-8"), value.encode("utf-8"))

        op_started = time.perf_counter()
        try:
            response = await _round_trip(host, port, payload, timeout)
        except (TimeoutError, OSError, asyncio.TimeoutError):
            failures += 1
            continue

        latencies_ms.append((time.perf_counter() - op_started) * 1000.0)
        if response == b"+OK\r\n":
            acknowledged.append(AcknowledgedWrite(key=key, value=value))
        else:
            failures += 1

    elapsed = time.perf_counter() - started
    successes = len(acknowledged)
    throughput = successes / elapsed if elapsed > 0 else 0.0
    result = BenchmarkResult(
        mode=mode,
        attempted_writes=operations,
        successful_writes=successes,
        failed_writes=failures,
        elapsed_seconds=elapsed,
        throughput_writes_per_second=throughput,
        mean_latency_ms=statistics.fmean(latencies_ms) if latencies_ms else 0.0,
        p50_latency_ms=percentile(latencies_ms, 50),
        p99_latency_ms=percentile(latencies_ms, 99),
    )
    return result, acknowledged


async def verify_acknowledged_writes(
    *,
    host: str,
    port: int,
    acknowledged: Iterable[AcknowledgedWrite],
    timeout: float = 2.0,
) -> VerificationResult:
    checked = 0
    missing = 0
    mismatched = 0
    for write in acknowledged:
        checked += 1
        response = await _round_trip(host, port, _command(b"GET", write.key.encode("utf-8")), timeout)
        if response == b"$-1\r\n":
            missing += 1
            continue
        expected = f"${len(write.value)}\r\n{write.value}\r\n".encode("utf-8")
        if response != expected:
            mismatched += 1
    return VerificationResult(checked=checked, missing=missing, mismatched=mismatched)


def render_table(result: BenchmarkResult) -> str:
    rows = [
        ("mode", result.mode),
        ("attempted", result.attempted_writes),
        ("successful", result.successful_writes),
        ("failed", result.failed_writes),
        ("elapsed_s", f"{result.elapsed_seconds:.4f}"),
        ("throughput_writes_s", f"{result.throughput_writes_per_second:.2f}"),
        ("mean_latency_ms", f"{result.mean_latency_ms:.3f}"),
        ("p50_latency_ms", f"{result.p50_latency_ms:.3f}"),
        ("p99_latency_ms", f"{result.p99_latency_ms:.3f}"),
        ("checked_after_failover", result.acknowledged_writes_checked),
        ("missing_after_failover", result.acknowledged_writes_missing),
        ("mismatched_after_failover", result.acknowledged_writes_mismatched),
    ]
    width = max(len(name) for name, _ in rows)
    return "\n".join(f"{name:<{width}}  {value}" for name, value in rows)


async def _main_async(args: argparse.Namespace) -> None:
    result, acknowledged = await run_write_benchmark(
        host=args.host,
        port=args.port,
        operations=args.operations,
        value_size=args.value_size,
        key_prefix=args.key_prefix,
        mode=args.mode,
        seed=args.seed,
        timeout=args.timeout,
    )
    if args.verify_host is not None:
        verification = await verify_acknowledged_writes(
            host=args.verify_host,
            port=args.verify_port,
            acknowledged=acknowledged,
            timeout=args.timeout,
        )
        result = BenchmarkResult(
            **{
                **asdict(result),
                "acknowledged_writes_checked": verification.checked,
                "acknowledged_writes_missing": verification.missing,
                "acknowledged_writes_mismatched": verification.mismatched,
            }
        )

    print(render_table(result))
    if args.output:
        Path(args.output).write_text(json.dumps(asdict(result), indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="mini-redis write benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--value-size", type=int, default=32)
    parser.add_argument("--key-prefix", default="bench")
    parser.add_argument("--mode", choices=["async", "quorum"], required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--output")
    parser.add_argument("--verify-host")
    parser.add_argument("--verify-port", type=int, default=6381)
    asyncio.run(_main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
