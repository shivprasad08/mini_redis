"""M24 tests: smart client discovery, failover routing, retries and write safety."""

from __future__ import annotations

import asyncio
import json

import pytest

from miniredis.smart_client import (
    ClusterUnavailableError,
    CommandError,
    SmartClient,
    UncertainWriteError,
)
from tests.test_leader_recovery import NODE_IDS, LocalCluster, _free_ports, _wait_until


@pytest.fixture
async def cluster():
    c = LocalCluster()
    await c.start(*NODE_IDS)
    await _wait_until(
        lambda: c.primaries() == ["node-a"] and c.follows("node-b", "node-a") and c.follows("node-c", "node-a"),
        10, "bootstrap",
    )
    yield c
    await c.close()


def _addresses(c: LocalCluster) -> list[str]:
    return [f"127.0.0.1:{c.tcp[n]}" for n in NODE_IDS]


async def test_discovers_primary_and_reports_status(cluster) -> None:
    client = SmartClient(_addresses(cluster))
    await client.set("k", "v")
    assert client.primary == f"127.0.0.1:{cluster.tcp['node-a']}"
    assert await client.get("k") == b"v"
    assert await client.get("missing") is None
    status = await client.status()
    assert status["primary"] == "node-a" and status["available"]
    roles = {n["node_id"]: n["role"] for n in status["nodes"]}
    assert roles == {"node-a": "primary", "node-b": "replica", "node-c": "replica"}
    assert all(n["healthy"] and n["lag"] is not None for n in status["nodes"])


async def test_writes_continue_after_primary_failover(cluster) -> None:
    client = SmartClient(_addresses(cluster), max_attempts=20, backoff=0.25)
    await client.set("before", "1")
    await cluster.stop("node-a")
    await client.set("after", "2")  # retried until the new primary is elected
    assert client.primary == f"127.0.0.1:{cluster.tcp['node-c']}"
    assert client.rediscoveries > 0
    assert await client.get("before") == b"1" and await client.get("after") == b"2"
    status = await client.status()
    assert status["primary"] == "node-c"
    assert next(n for n in status["nodes"] if n["address"].endswith(str(cluster.tcp["node-a"])))["healthy"] is False


async def test_stale_cached_primary_is_rediscovered(cluster) -> None:
    client = SmartClient(_addresses(cluster))
    client.primary = f"127.0.0.1:{cluster.tcp['node-b']}"  # replica: answers "not primary"
    await client.set("k", "v")
    assert client.primary == f"127.0.0.1:{cluster.tcp['node-a']}" and client.rediscoveries == 1


async def test_unreachable_nodes_are_skipped(cluster) -> None:
    dead = f"127.0.0.1:{_free_ports(1)[0]}"
    client = SmartClient([dead] + _addresses(cluster), timeout=0.5)
    await client.set("k", "v")
    assert (await client.status())["nodes"][0] == {"address": dead, "healthy": False, "role": "unreachable"}


async def test_retry_limit_when_no_primary() -> None:
    client = SmartClient([f"127.0.0.1:{p}" for p in _free_ports(2)], timeout=0.3, max_attempts=3, backoff=0.05)
    started = asyncio.get_running_loop().time()
    with pytest.raises(ClusterUnavailableError, match="3 attempts"):
        await client.get("k")
    assert asyncio.get_running_loop().time() - started < 3


async def test_command_errors_are_not_retried(cluster) -> None:
    client = SmartClient(_addresses(cluster))
    with pytest.raises(CommandError, match="invalid expire time"):
        await client.set("k", "v", ex=-1)


class FlakyPrimary:
    """Claims to be primary; receives writes but drops the connection before replying."""

    def __init__(self) -> None:
        self.writes = 0
        self.reads = 0

    async def handle(self, reader, writer) -> None:
        data = await reader.read(65536)
        if b"ROLE" in data:
            body = json.dumps({"node_id": "flaky", "role": "primary", "epoch": 1, "accepts_writes": True}).encode()
            writer.write(b"$%d\r\n%s\r\n" % (len(body), body))
            await writer.drain()
        elif b"SET" in data:
            self.writes += 1
        else:
            self.reads += 1
        writer.close()


async def test_write_with_unknown_outcome_is_not_retried() -> None:
    flaky = FlakyPrimary()
    server = await asyncio.start_server(flaky.handle, "127.0.0.1", 0)
    address = f"127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        client = SmartClient([address], max_attempts=4, backoff=0.01)
        with pytest.raises(UncertainWriteError, match="outcome unknown"):
            await client.set("k", "v")
        assert flaky.writes == 1, "a write that may have been applied must not be re-sent"
        with pytest.raises(ClusterUnavailableError):
            await client.get("k")
        assert flaky.reads == 4, "reads are idempotent and retried up to the limit"
    finally:
        server.close()
        await server.wait_closed()
