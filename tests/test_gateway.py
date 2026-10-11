"""M25 tests: HTTP gateway endpoints and dashboard over a real in-process cluster."""

from __future__ import annotations

import asyncio
import json

import pytest

from miniredis.gateway import serve
from tests.test_leader_recovery import NODE_IDS, LocalCluster, _free_ports, _wait_until


async def http(port: int, method: str, path: str, body: bytes | None = None, headers: str = "") -> tuple[int, dict | str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    extra = f"Content-Length: {len(body)}\r\n" if body is not None else ""
    writer.write(f"{method} {path} HTTP/1.1\r\nHost: x\r\n{extra}{headers}\r\n".encode() + (body or b""))
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), 30)
    writer.close()
    head, _, payload = raw.partition(b"\r\n\r\n")
    status = int(head.split()[1])
    ctype = next(l for l in head.decode().split("\r\n") if l.lower().startswith("content-type"))
    return status, json.loads(payload) if "json" in ctype else payload.decode()


@pytest.fixture
async def gateway():
    cluster = LocalCluster()
    await cluster.start(*NODE_IDS)
    await _wait_until(
        lambda: cluster.primaries() == ["node-a"] and cluster.follows("node-b", "node-a")
        and cluster.follows("node-c", "node-a"), 10, "bootstrap")
    port = _free_ports(1)[0]
    server = await serve([f"127.0.0.1:{cluster.tcp[n]}" for n in NODE_IDS], "127.0.0.1", port)
    yield cluster, port
    server.close()
    await server.wait_closed()
    await cluster.close()


async def test_put_get_and_ttl(gateway) -> None:
    cluster, port = gateway
    status, body = await http(port, "PUT", "/kv/greeting", b'{"value": "hello"}')
    assert status == 200 and body["stored"] and body["primary"].endswith(f":{cluster.tcp['node-a']}")
    assert await http(port, "GET", "/kv/greeting") == (200, {"key": "greeting", "value": "hello"})
    status, body = await http(port, "PUT", "/kv/temp%20key", b'{"value": "x", "ttl": 60}')
    assert status == 200 and body["key"] == "temp key"


async def test_missing_key_and_invalid_input(gateway) -> None:
    _, port = gateway
    assert (await http(port, "GET", "/kv/nope"))[0] == 404
    cases = [
        ("PUT", "/kv/k", b"not json"),
        ("PUT", "/kv/k", b'{"value": 5}'),
        ("PUT", "/kv/k", b'{"value": "v", "ttl": 0}'),
        ("PUT", "/kv/k", b'{"value": "v", "ttl": true}'),
        ("PUT", "/kv/k", b'{"value": "v", "extra": 1}'),
        ("PUT", "/kv/", b'{"value": "v"}'),
        ("GET", "/kv/" + "x" * 600, None),
    ]
    for method, path, body in cases:
        assert (await http(port, method, path, body))[0] == 400, (method, path, body)
    assert (await http(port, "PUT", "/kv/k"))[0] == 411
    assert (await http(port, "DELETE", "/kv/k"))[0] == 405
    assert (await http(port, "GET", "/nowhere"))[0] == 404


async def test_status_and_dashboard(gateway) -> None:
    _, port = gateway
    status, body = await http(port, "GET", "/status")
    assert status == 200 and body["primary"] == "node-a" and body["available"]
    assert {n["node_id"]: (n["role"], n["healthy"], n["lag"]) for n in body["nodes"]} == {
        "node-a": ("primary", True, 0), "node-b": ("replica", True, 0), "node-c": ("replica", True, 0)}
    status, html = await http(port, "GET", "/")
    assert status == 200 and 'fetch("/status"' in html and "setInterval(refresh, 2000)" in html
    for section in ('id="overview"', 'id="kv"', 'id="concepts"', 'id="present"'):
        assert section in html


async def test_verify_and_snapshot_endpoints(gateway) -> None:
    _, port = gateway
    assert (await http(port, "PUT", "/kv/k", b'{"value": "v"}'))[0] == 200
    status, body = await http(port, "GET", "/verify")
    assert status == 200 and len(body["nodes"]) == 3
    # Nodes in this harness run without a data dir, so VERIFY truthfully reports that.
    assert all(n["ok"] is False and "persistence is disabled" in n["error"] for n in body["nodes"])
    status, body = await http(port, "POST", "/snapshot")
    assert status == 200 and body["status"] == "COMPLETED" and body["summary"]["channels_closed"] == 6
    assert (await http(port, "GET", "/snapshot"))[0] == 405


async def test_status_reflects_failover_and_gateway_keeps_writing(gateway) -> None:
    cluster, port = gateway
    await cluster.stop("node-a")
    status, body = await http(port, "PUT", "/kv/after", b'{"value": "failover"}')
    assert status == 200, body
    _, body = await http(port, "GET", "/status")
    assert body["primary"] == "node-c"
    assert next(n for n in body["nodes"] if n["address"].endswith(str(cluster.tcp["node-a"])))["healthy"] is False
    assert await http(port, "GET", "/kv/after") == (200, {"key": "after", "value": "failover"})


async def test_unavailable_cluster_returns_503() -> None:
    port = _free_ports(1)[0]
    server = await serve([f"127.0.0.1:{p}" for p in _free_ports(2)], "127.0.0.1", port, max_attempts=2, timeout=0.5)
    try:
        status, body = await http(port, "GET", "/status")
        assert status == 503 and body["available"] is False
        assert all(n["healthy"] is False for n in body["nodes"])
        assert (await http(port, "GET", "/kv/k"))[0] == 503
        assert (await http(port, "PUT", "/kv/k", b'{"value": "v"}'))[0] == 503
    finally:
        server.close()
        await server.wait_closed()
