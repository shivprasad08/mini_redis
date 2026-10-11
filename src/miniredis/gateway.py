"""
M25 — HTTP gateway and dashboard on top of the M24 smart client.

    python -m miniredis.gateway --nodes 127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382 --port 8080

Endpoints (JSON):

* ``GET /kv/{key}``  → 200 ``{"key", "value"}`` | 404 missing key
* ``PUT /kv/{key}``  body ``{"value": "<string>", "ttl": <optional positive int seconds>}``
                     → 200 ``{"key", "stored": true}``
* ``GET /status``    → 200 primary, per-node role, health, epoch and replication lag
                     (503 body is the same JSON when no primary is available)
* ``GET /``          → dashboard (HTML, polls /status every 2 s)

Errors: 400 invalid key/body, 404 unknown key or path, 405 wrong method,
411/413 missing or oversized body, 503 cluster unavailable, 504 write sent but
outcome unknown (not retried, see smart_client). The gateway only talks to
the cluster through the smart client; it has no access to storage,
replication or election internals.

Uses only asyncio (no extra dependency): HTTP/1.1, one request per
connection (``Connection: close``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any, Optional
from urllib.parse import unquote

from pathlib import Path

from miniredis.protocol import ErrorReply
from miniredis.smart_client import (
    ClusterUnavailableError,
    CommandError,
    SmartClient,
    UncertainWriteError,
)

logger = logging.getLogger(__name__)

MAX_BODY = 1024 * 1024
MAX_KEY = 512
_REASONS = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
            411: "Length Required", 413: "Payload Too Large", 500: "Internal Server Error",
            502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout"}


class Gateway:
    def __init__(self, client: SmartClient) -> None:
        self.client = client

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            status, ctype, body = await self._dispatch(reader)
        except Exception as exc:  # never leak a traceback to the client
            logger.exception("gateway request failed")
            status, ctype, body = _json(500, {"error": f"internal error: {type(exc).__name__}"})
        head = (
            f"HTTP/1.1 {status} {_REASONS.get(status, '')}\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"
        )
        writer.write(head.encode() + body)
        try:
            await writer.drain()
        finally:
            writer.close()

    async def _dispatch(self, reader: asyncio.StreamReader) -> tuple[int, str, bytes]:
        request_line = (await reader.readline()).decode("latin-1").strip()
        parts = request_line.split(" ")
        if len(parts) != 3:
            return _json(400, {"error": "malformed request line"})
        method, target, _ = parts
        headers: dict[str, str] = {}
        while True:
            line = (await reader.readline()).decode("latin-1")
            if line in ("\r\n", "\n", ""):
                break
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        path = target.split("?", 1)[0]

        if path == "/":
            return (200, "text/html; charset=utf-8", DASHBOARD.encode()) if method == "GET" else _json(405, {"error": "use GET"})
        if path == "/verify":  # M20: VERIFY on every node (read-only)
            return await self._verify() if method == "GET" else _json(405, {"error": "use GET"})
        if path == "/snapshot":  # M17: a real Chandy-Lamport snapshot (records, never modifies)
            return await self._snapshot() if method == "POST" else _json(405, {"error": "use POST"})
        if path == "/status":
            if method != "GET":
                return _json(405, {"error": "use GET"})
            status = await self.client.status()
            return _json(200 if status["available"] else 503, status)
        if path.startswith("/kv/"):
            key = unquote(path[len("/kv/"):])
            if not key or len(key) > MAX_KEY or any(ord(ch) < 32 for ch in key) or "/" in key:
                return _json(400, {"error": f"key must be 1-{MAX_KEY} printable characters without '/'"})
            if method == "GET":
                return await self._get(key)
            if method == "PUT":
                return await self._put(key, reader, headers)
            return _json(405, {"error": "use GET or PUT"})
        return _json(404, {"error": f"no route for {path}"})

    async def _verify(self) -> tuple[int, str, bytes]:
        async def one(address: str) -> dict[str, Any]:
            try:
                reply = await self.client._call(address, b"VERIFY")
            except Exception as exc:  # noqa: BLE001 - reported per node
                return {"address": address, "ok": False, "error": f"unreachable: {type(exc).__name__}"}
            if isinstance(reply, ErrorReply):
                return {"address": address, "ok": False, "error": reply.message}
            return {"address": address, **json.loads(reply)}

        results = await asyncio.gather(*(one(a) for a in self.client.nodes))
        return _json(200, {"nodes": results})

    async def _snapshot(self) -> tuple[int, str, bytes]:
        try:
            reply = await self.client.execute(b"SNAPSHOT", write=False)
        except ClusterUnavailableError as exc:
            return _json(503, {"error": str(exc)})
        except CommandError as exc:
            return _json(502, {"error": exc.args[0]})
        return 200, "application/json", reply

    async def _get(self, key: str) -> tuple[int, str, bytes]:
        try:
            value = await self.client.get(key)
        except ClusterUnavailableError as exc:
            return _json(503, {"error": str(exc)})
        if value is None:
            return _json(404, {"error": "key not found", "key": key})
        return _json(200, {"key": key, "value": value.decode("utf-8", errors="replace")})

    async def _put(self, key: str, reader: asyncio.StreamReader, headers: dict[str, str]) -> tuple[int, str, bytes]:
        if "content-length" not in headers:
            return _json(411, {"error": "Content-Length required"})
        try:
            length = int(headers["content-length"])
        except ValueError:
            return _json(400, {"error": "invalid Content-Length"})
        if length > MAX_BODY:
            return _json(413, {"error": f"body larger than {MAX_BODY} bytes"})
        try:
            payload = json.loads(await reader.readexactly(length))
        except (ValueError, asyncio.IncompleteReadError):
            return _json(400, {"error": "body must be JSON: {\"value\": string, \"ttl\": optional int}"})
        if not isinstance(payload, dict) or not isinstance(payload.get("value"), str):
            return _json(400, {"error": "\"value\" must be a string"})
        ttl = payload.get("ttl")
        if ttl is not None and (not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0):
            return _json(400, {"error": "\"ttl\" must be a positive integer (seconds)"})
        if set(payload) - {"value", "ttl"}:
            return _json(400, {"error": f"unknown fields {sorted(set(payload) - {'value', 'ttl'})}"})
        try:
            await self.client.set(key, payload["value"], ex=ttl)
        except UncertainWriteError as exc:
            return _json(504, {"error": str(exc), "outcome": "unknown"})
        except ClusterUnavailableError as exc:
            return _json(503, {"error": str(exc)})
        except CommandError as exc:
            return _json(400, {"error": str(exc)})
        return _json(200, {"key": key, "stored": True, "primary": self.client.primary})


def _json(status: int, payload: Any) -> tuple[int, str, bytes]:
    return status, "application/json", json.dumps(payload).encode()


async def serve(
    nodes: list[str], host: str, port: int, max_attempts: int = 10, timeout: float = 2.0,
) -> asyncio.AbstractServer:
    # Default retry budget (~10 s with backoff) rides through a primary election.
    gateway = Gateway(SmartClient(nodes, timeout=timeout, max_attempts=max_attempts, backoff=0.25))
    server = await asyncio.start_server(gateway.handle, host, port)
    logger.info("HTTP gateway on http://%s:%s (cluster: %s)", host, port, ", ".join(nodes))
    return server


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="mini-redis HTTP gateway and dashboard (M25)")
    parser.add_argument("--nodes", default="127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382",
                        help="comma-separated host:port client addresses of the cluster nodes")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--max-attempts", type=int, default=10, help="smart-client attempts per request")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    async def run() -> None:
        server = await serve(args.nodes.split(","), args.host, args.port, max_attempts=args.max_attempts)
        async with server:
            await server.serve_forever()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


# Single-page control center (plain HTML/CSS/JS, no build step).
DASHBOARD = (Path(__file__).with_name("dashboard.html")).read_text(encoding="utf-8")


if __name__ == "__main__":
    main()
