"""
M24 — Smart distributed client that follows the current primary.

Discovery: the client asks every configured node for ``ROLE`` (JSON: role,
election epoch, known primary, replication progress) and routes to the node
that reports ``role=primary`` and ``accepts_writes`` with the highest epoch.
The result is cached until a request fails, then rediscovered.

Retry rules (``max_attempts`` total tries, exponential backoff):

* **Reads** (GET/EXISTS) are idempotent: any connection failure or
  not-primary reply triggers rediscovery and a retry.
* **Writes** (SET/DEL/EXPIRE) are retried only when the server *definitely*
  did not apply them: the connection could not be opened (nothing sent), or
  the server answered "not primary" / "minority partition" / "READONLY" /
  "quorum not reached, write not applied". If the request was sent but no
  reply arrived (connection lost, timeout), the outcome is unknown: the
  server has no request-id deduplication, so the client raises
  ``UncertainWriteError`` instead of re-sending (a retry could apply the
  write twice or overwrite a newer value).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional, Sequence

from miniredis.protocol import CommandParser, ErrorReply, encode_array, encode_bulk_string

logger = logging.getLogger(__name__)

# Server errors meaning "this node did not apply the write; another may".
_RETRYABLE_ERRORS = ("node is not primary", "minority partition", "READONLY", "write not applied")


class SmartClientError(Exception):
    """Base class for smart-client failures."""


class ClusterUnavailableError(SmartClientError):
    """No reachable node currently accepts the request."""


class UncertainWriteError(SmartClientError):
    """A write was sent but its outcome is unknown; it was not retried."""


class CommandError(SmartClientError):
    """The server rejected the command (not a routing problem)."""


class _NotApplied(Exception):
    """Internal: definitely not applied, safe to retry elsewhere."""


class SmartClient:
    def __init__(
        self,
        nodes: Sequence[str],
        timeout: float = 2.0,
        max_attempts: int = 4,
        backoff: float = 0.2,
    ) -> None:
        if not nodes:
            raise ValueError("at least one node address (host:port) is required")
        self.nodes = list(nodes)
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.primary: Optional[str] = None
        self.rediscoveries = 0

    # ── low level ────────────────────────────────────────────────────

    async def _call(self, address: str, *args: bytes) -> Any:
        """Send one command; raises _NotApplied if nothing could be sent,
        ConnectionError/TimeoutError if it was sent but no reply arrived."""
        host, port = address.rsplit(":", 1)
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), self.timeout)
        except (OSError, asyncio.TimeoutError) as exc:
            raise _NotApplied(f"cannot connect to {address}: {exc!r}") from None
        try:
            writer.write(encode_array([encode_bulk_string(a) for a in args]))
            await writer.drain()
            parser = CommandParser()
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), self.timeout)
                if not chunk:
                    raise ConnectionError(f"{address} closed the connection before replying")
                parser.feed(chunk)
                reply = parser.try_parse_response()
                if reply is not CommandParser._INCOMPLETE:
                    return reply
        finally:
            writer.close()

    async def role(self, address: str) -> Optional[dict[str, Any]]:
        try:
            reply = await self._call(address, b"ROLE")
        except (_NotApplied, OSError, asyncio.TimeoutError):
            return None
        if not isinstance(reply, bytes):
            return None
        info = json.loads(reply)
        info["address"] = address
        return info

    # ── discovery ────────────────────────────────────────────────────

    async def discover(self) -> str:
        roles = await asyncio.gather(*(self.role(a) for a in self.nodes))
        primaries = [r for r in roles if r and r.get("role") == "primary" and r.get("accepts_writes")]
        if not primaries:
            self.primary = None
            raise ClusterUnavailableError("no reachable node currently accepts writes as primary")
        best = max(primaries, key=lambda r: r.get("epoch", 0))
        if best["address"] != self.primary:
            logger.info("smart client: primary is %s (%s)", best.get("node_id"), best["address"])
        self.primary = best["address"]
        return self.primary

    async def status(self) -> dict[str, Any]:
        """Per-node role, health and replication lag (used by the M25 gateway)."""
        roles = await asyncio.gather(*(self.role(a) for a in self.nodes))
        primary = max(
            (r for r in roles if r and r.get("role") == "primary" and r.get("accepts_writes")),
            key=lambda r: r.get("epoch", 0),
            default=None,
        )
        nodes = []
        for address, info in zip(self.nodes, roles):
            if info is None:
                nodes.append({"address": address, "healthy": False, "role": "unreachable"})
                continue
            entry = {"healthy": True, **info, "lag": None}
            if primary is not None and info.get("replication_epoch") == primary.get("replication_epoch"):
                entry["lag"] = primary["last_applied_sequence"] - info["last_applied_sequence"]
            nodes.append(entry)
        return {
            "primary": primary.get("node_id") if primary else None,
            "primary_address": primary["address"] if primary else None,
            "epoch": primary.get("epoch") if primary else None,
            "available": primary is not None,
            "nodes": nodes,
        }

    # ── routed execution ─────────────────────────────────────────────

    async def execute(self, *args: str | bytes, write: bool) -> Any:
        raw = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        last: Optional[Exception] = None
        for attempt in range(self.max_attempts):
            if attempt:
                await asyncio.sleep(min(self.backoff * 2 ** (attempt - 1), 2.0))
            try:
                address = self.primary or await self.discover()
            except ClusterUnavailableError as exc:
                last = exc
                continue
            try:
                reply = await self._call(address, *raw)
            except _NotApplied as exc:
                last, self.primary = exc, None
                self.rediscoveries += 1
                continue
            except (OSError, asyncio.TimeoutError) as exc:
                if write:
                    self.primary = None
                    raise UncertainWriteError(
                        f"{raw[0].decode()} sent to {address} but no reply ({exc!r}); "
                        "outcome unknown, not retried"
                    ) from None
                last, self.primary = exc, None
                self.rediscoveries += 1
                continue
            if isinstance(reply, ErrorReply):
                if any(marker in reply.message for marker in _RETRYABLE_ERRORS):
                    last, self.primary = CommandError(reply.message), None
                    self.rediscoveries += 1
                    continue
                raise CommandError(reply.message)
            return reply
        raise ClusterUnavailableError(f"gave up after {self.max_attempts} attempts: {last}")

    async def get(self, key: str | bytes) -> Optional[bytes]:
        return await self.execute(b"GET", key, write=False)

    async def set(self, key: str | bytes, value: str | bytes, ex: Optional[int] = None) -> None:
        args = [b"SET", key, value] + ([b"EX", str(ex)] if ex else [])
        await self.execute(*args, write=True)

    async def delete(self, key: str | bytes) -> int:
        return await self.execute(b"DEL", key, write=True)
