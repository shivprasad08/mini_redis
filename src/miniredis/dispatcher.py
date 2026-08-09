"""
Command dispatcher for mini-redis.

This is the thin middleware layer between the wire protocol (Module 1)
and the in-memory store (Module 3) / PubSub (Module 4). It validates
argument counts, parses command-specific options, calls into the backend
services, and encodes the result into RESP wire bytes.

Supported commands
──────────────────
* ``PING``                     → ``+PONG``
* ``GET key``                  → bulk string (or null)
* ``SET key value [EX secs]``  → ``+OK``
* ``DEL key``                  → integer (0 or 1)
* ``EXPIRE key seconds``       → integer (0 or 1)
* ``EXISTS key``               → integer (0 or 1)
* ``SUBSCRIBE channel``        → array ``["subscribe", channel, count]``
* ``PUBLISH channel message``  → integer (delivered count)
* anything else                → ``-ERR unknown command '<name>'``
"""

from __future__ import annotations

import asyncio
from typing import Optional

from miniredis.protocol import (
    Command,
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
)
from miniredis.storage import Store
from miniredis.pubsub import PubSub
from miniredis.cluster import WriteAdmissionGate
from miniredis.replication.replicator import Replicator


def pubsub_message_framer(channel: str, message: bytes) -> bytes:
    """Builds the RESP array for a published message.
    Format: *3\\r\\n$7\\r\\nmessage\\r\\n$<len>\\r\\n<channel>\\r\\n$<len>\\r\\n<payload>\\r\\n
    """
    return encode_array([
        encode_bulk_string(b"message"),
        encode_bulk_string(channel.encode("utf-8")),
        encode_bulk_string(message),
    ])


class Dispatcher:
    """Route parsed client commands to the storage and pubsub backends."""

    def __init__(
        self, 
        store: Store, 
        pubsub: PubSub, 
        replicator: Optional[Replicator] = None,
        writes_enabled: bool = True,
        write_gate: Optional[WriteAdmissionGate] = None,
    ) -> None:
        self._store = store
        self._pubsub = pubsub
        self._replicator = replicator
        self._writes_enabled = writes_enabled
        self._write_gate = write_gate or WriteAdmissionGate(can_serve_writes=writes_enabled)

    def set_replicator(self, replicator: Optional[Replicator]) -> None:
        """Swap the replication backend at runtime."""
        self._replicator = replicator

    @property
    def replicator(self) -> Optional[Replicator]:
        return self._replicator

    def set_writes_enabled(self, enabled: bool) -> None:
        """Toggle whether client write commands are accepted."""
        self._writes_enabled = enabled
        if enabled:
            self._write_gate.allow_writes()
        else:
            self._write_gate.block_writes("node is not primary")

    @property
    def write_gate(self) -> WriteAdmissionGate:
        return self._write_gate

    # ── public entry points ─────────────────────────────────────────

    async def handle_command(
        self, command: Command, writer: asyncio.StreamWriter
    ) -> bytes:
        """Execute *command* and return the RESP-encoded response bytes.
        Takes the connection's *writer* to register subscriptions.
        """
        dispatch = {
            "PING": self._handle_ping,
            "GET": self._handle_get,
            "SET": self._handle_set,
            "DEL": self._handle_del,
            "EXPIRE": self._handle_expire,
            "EXISTS": self._handle_exists,
            "SUBSCRIBE": self._handle_subscribe,
            "PUBLISH": self._handle_publish,
        }

        handler = dispatch.get(command.name)
        if handler is None:
            return encode_error(f"ERR unknown command '{command.name}'")

        res = handler(command, writer)
        if asyncio.iscoroutine(res):
            return await res
        return res  # type: ignore

    def handle_disconnect(self, writer: asyncio.StreamWriter) -> None:
        """Clean up state associated with a disconnected client."""
        self._pubsub.unsubscribe_all(writer)

    # ── per-command handlers ────────────────────────────────────────

    def _handle_ping(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        return encode_simple_string("PONG")

    def _handle_get(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'GET' command")
        value = self._store.get(command.args[0])
        return encode_bulk_string(value)

    async def _handle_set(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) not in (2, 4):
            return encode_error("ERR wrong number of arguments for 'SET' command")

        if not self._write_gate.can_serve_writes:
            return encode_error("ERR minority partition, refusing writes")

        if not self._writes_enabled:
            return encode_error("ERR node is not primary")

        key = command.args[0]
        value = command.args[1]
        ttl: int | None = None

        if len(command.args) == 4:
            option = command.args[2].upper()
            if option != b"EX":
                return encode_error(
                    f"ERR unsupported SET option '{command.args[2].decode('utf-8', errors='replace')}'"
                )
            try:
                ttl = int(command.args[3])
            except ValueError:
                return encode_error("ERR value is not an integer or out of range")
            if ttl <= 0:
                return encode_error("ERR invalid expire time in 'SET' command")

        if self._replicator:
            # Quorum mode is decided before the primary mutates local state so
            # a failed quorum leaves the store unchanged and avoids rollback.
            if not await self._replicator.propagate_write("SET", key, value, ttl or 0):
                return encode_error("ERR quorum not reached, write not applied")

        self._store.set(key, value, ttl_seconds=ttl)

        return encode_simple_string("OK")

    async def _handle_del(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'DEL' command")

        if not self._write_gate.can_serve_writes:
            return encode_error("ERR minority partition, refusing writes")

        if not self._writes_enabled:
            return encode_error("ERR node is not primary")

        key = command.args[0]
        existed = self._store.exists(key)

        if existed and self._replicator:
            # The write is only committed locally after quorum succeeds.
            if not await self._replicator.propagate_write("DEL", key):
                return encode_error("ERR quorum not reached, write not applied")

        count = self._store.delete(key) if existed else 0

        return encode_integer(count)

    async def _handle_expire(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 2:
            return encode_error("ERR wrong number of arguments for 'EXPIRE' command")

        if not self._write_gate.can_serve_writes:
            return encode_error("ERR minority partition, refusing writes")

        if not self._writes_enabled:
            return encode_error("ERR node is not primary")

        try:
            seconds = int(command.args[1])
        except ValueError:
            return encode_error("ERR value is not an integer or out of range")
        if seconds <= 0:
            return encode_error("ERR invalid expire time in 'EXPIRE' command")

        key = command.args[0]
        existed = self._store.exists(key)

        if existed and self._replicator:
            # Preserve the async-vs-quorum difference at the commit point.
            if not await self._replicator.propagate_write("EXPIRE", key, ttl_seconds=seconds):
                return encode_error("ERR quorum not reached, write not applied")

        result = self._store.expire(key, seconds) if existed else 0

        return encode_integer(result)

    def _handle_exists(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'EXISTS' command")
        result = self._store.exists(command.args[0])
        return encode_integer(1 if result else 0)

    def _handle_subscribe(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'SUBSCRIBE' command")
        
        channel = command.args[0].decode("utf-8", errors="replace")
        self._pubsub.subscribe(channel, writer)
        
        count = self._pubsub.subscription_count(writer)
        
        return encode_array([
            encode_bulk_string(b"subscribe"),
            encode_bulk_string(channel.encode("utf-8")),
            encode_integer(count)
        ])

    async def _handle_publish(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 2:
            return encode_error("ERR wrong number of arguments for 'PUBLISH' command")
            
        channel = command.args[0].decode("utf-8", errors="replace")
        message = command.args[1]
        
        delivered = await self._pubsub.publish(channel, message)
        return encode_integer(delivered)
