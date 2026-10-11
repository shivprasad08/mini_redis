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
* ``STALENESS``                → bulk string (JSON cluster report) — M15
* ``SNAPSHOT``                 → bulk string (JSON Chandy–Lamport global snapshot) — M17
* ``SNAPSHOT GET id``          → bulk string (JSON result of a recent snapshot) — M17
* ``VERIFY``                   → bulk string (JSON ledger verification) or error — M20
* ``ROLE``                     → bulk string (JSON role / epoch / primary / progress) — M24
* anything else                → ``-ERR unknown command '<name>'``

Commit path (M17/M20)
─────────────────────
Every mutating command runs its critical section under ``commit_lock``:
authorization re-check → sequence allocation and send to replicas →
write-ahead ledger append → store mutation → progress record. Holding the
lock across these steps keeps local commits in sequence order (and records the
sequence that was actually allocated), and lets a Chandy–Lamport recording
take the lock to observe a state that matches exactly what was sent. The
acknowledgement waits (outside the lock) for the ledger's durability rule.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

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
from miniredis.ledger import LedgerRecord, LedgerWriteError
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
        clock: Optional[Callable[[], float]] = None,
        progress=None,  # Optional[ReplicationProgress] — avoid circular import at module level
        lock_manager=None,  # Optional[BaseLockManager] — M16 mutual exclusion
    ) -> None:
        self._store = store
        self._pubsub = pubsub
        self._replicator = replicator
        self._writes_enabled = writes_enabled
        self._write_gate = write_gate or WriteAdmissionGate(can_serve_writes=writes_enabled)
        self._clock = clock
        self._progress = progress  # ReplicationProgress | None
        self._lock_manager = lock_manager
        self._commit_lock = asyncio.Lock()
        self._ledger = None  # WriteLedger | None (M20)
        self._snapshot_manager = None  # SnapshotManager | None (M17)
        self._status_provider = None  # Callable[[], dict] | None (M24 ROLE)

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
    def writes_enabled(self) -> bool:
        """Return whether client write commands are currently permitted."""
        return self._writes_enabled

    def set_progress(self, progress) -> None:
        """Attach or replace the shared progress tracker."""
        self._progress = progress

    def set_lock_manager(self, lock_manager) -> None:
        """Attach or replace the lock manager for M16."""
        self._lock_manager = lock_manager

    @property
    def write_gate(self) -> WriteAdmissionGate:
        return self._write_gate

    @property
    def commit_lock(self) -> asyncio.Lock:
        """Held while a write is sequenced, sent, logged and applied."""
        return self._commit_lock

    def set_ledger(self, ledger) -> None:
        self._ledger = ledger

    def set_snapshot_manager(self, manager) -> None:
        self._snapshot_manager = manager

    def set_status_provider(self, provider) -> None:
        """M24: callable returning this node's role/epoch/progress for ROLE."""
        self._status_provider = provider

    # ── commit helpers ──────────────────────────────────────────────

    def _write_refusal(self) -> Optional[bytes]:
        if not self._write_gate.can_serve_writes:
            return encode_error("ERR minority partition, refusing writes")
        if not self._writes_enabled:
            return encode_error("ERR node is not primary")
        return None

    def _log_write(
        self,
        op: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
        fencing_token: str = "",
    ) -> Optional[LedgerRecord]:
        """Append the write about to be applied (raises LedgerWriteError)."""
        if self._ledger is None:
            return None
        if self._replicator is not None:
            epoch, seq = self._replicator.epoch, self._replicator.last_sequence()
        else:
            # No replicas: the ledger itself provides the per-epoch sequence.
            epoch = self._progress.epoch if self._progress is not None else 0
            seq = self._ledger.next_local_sequence(epoch)
        return self._ledger.append_op(
            epoch=epoch, seq=seq, op=op, key=key, value=value,
            ttl_seconds=ttl_seconds, deadline_ms=expiry_deadline_ms, token=fencing_token,
        )

    def _record_progress(
        self,
        record: Optional[LedgerRecord],
        op: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> None:
        if self._progress is None:
            return
        if self._replicator is not None:
            self._replicator.record_primary_applied(
                epoch=self._replicator.epoch,
                sequence=self._replicator.last_sequence(),
                op_type=op,
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
                expiry_deadline_ms=expiry_deadline_ms,
            )
        elif record is not None:
            self._progress.record_applied(
                epoch=record.epoch,
                sequence=record.seq,
                op_type=op,
                key=key.decode("utf-8", errors="replace"),
                value=value,
                ttl_seconds=ttl_seconds,
                expiry_deadline_ms=expiry_deadline_ms,
            )

    async def _acknowledge(self, record: Optional[LedgerRecord], reply: bytes) -> bytes:
        """Return *reply* only once the ledger's durability rule is satisfied."""
        if record is not None and self._ledger is not None:
            try:
                await self._ledger.wait_durable(record)
            except LedgerWriteError as exc:
                return encode_error(f"ERR write applied but not durable: {exc}")
        return reply

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
            "STALENESS": self._handle_staleness,
            "LOCK": self._handle_lock,
            "UNLOCK": self._handle_unlock,
            "FENCED_SET": self._handle_fenced_set,
            "SNAPSHOT": self._handle_snapshot,
            "VERIFY": self._handle_verify,
            "ROLE": self._handle_role,
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

        async with self._commit_lock:
            # Re-checked under the lock: the node may have stepped down while
            # this write waited for an earlier one.
            refusal = self._write_refusal()
            if refusal is not None:
                return refusal

            expiry_deadline_ms = (
                int(self._clock() * 1000) + (ttl * 1000)
                if ttl is not None and self._clock is not None
                else 0
            )

            if self._replicator:
                # Quorum mode is decided before the primary mutates local state so
                # a failed quorum leaves the store unchanged and avoids rollback.
                if not await self._replicator.propagate_write(
                    "SET",
                    key,
                    value,
                    ttl or 0,
                    expiry_deadline_ms=expiry_deadline_ms,
                ):
                    return encode_error("ERR quorum not reached, write not applied")

            try:
                record = self._log_write("SET", key, value, ttl or 0, expiry_deadline_ms)
            except LedgerWriteError as exc:
                return encode_error(f"ERR write not persisted: {exc}")
            self._store.set(
                key,
                value,
                ttl_seconds=ttl,
                expires_at=(expiry_deadline_ms / 1000.0 if expiry_deadline_ms else None),
            )
            # Record the primary's local application in the progress tracker.
            self._record_progress(record, "SET", key, value, ttl or 0, expiry_deadline_ms)

        return await self._acknowledge(record, encode_simple_string("OK"))

    async def _handle_del(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'DEL' command")

        key = command.args[0]
        record = None
        async with self._commit_lock:
            refusal = self._write_refusal()
            if refusal is not None:
                return refusal

            existed = self._store.exists(key)
            if existed and self._replicator:
                # The write is only committed locally after quorum succeeds.
                if not await self._replicator.propagate_write("DEL", key):
                    return encode_error("ERR quorum not reached, write not applied")

            if existed:
                try:
                    record = self._log_write("DEL", key)
                except LedgerWriteError as exc:
                    return encode_error(f"ERR write not persisted: {exc}")
            count = self._store.delete(key) if existed else 0
            if existed and count:
                self._record_progress(record, "DEL", key)

        return await self._acknowledge(record, encode_integer(count))

    async def _handle_expire(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        if len(command.args) != 2:
            return encode_error("ERR wrong number of arguments for 'EXPIRE' command")

        try:
            seconds = int(command.args[1])
        except ValueError:
            return encode_error("ERR value is not an integer or out of range")
        if seconds <= 0:
            return encode_error("ERR invalid expire time in 'EXPIRE' command")

        key = command.args[0]
        record = None
        async with self._commit_lock:
            refusal = self._write_refusal()
            if refusal is not None:
                return refusal

            existed = self._store.exists(key)
            expiry_deadline_ms = (
                int(self._clock() * 1000) + (seconds * 1000)
                if self._clock is not None
                else 0
            )

            if existed and self._replicator:
                # Preserve the async-vs-quorum difference at the commit point.
                if not await self._replicator.propagate_write(
                    "EXPIRE",
                    key,
                    ttl_seconds=seconds,
                    expiry_deadline_ms=expiry_deadline_ms,
                ):
                    return encode_error("ERR quorum not reached, write not applied")

            if existed:
                try:
                    record = self._log_write("EXPIRE", key, b"", seconds, expiry_deadline_ms)
                except LedgerWriteError as exc:
                    return encode_error(f"ERR write not persisted: {exc}")
            result = (
                self._store.expire_at(key, expiry_deadline_ms / 1000.0)
                if existed and expiry_deadline_ms
                else self._store.expire(key, seconds) if existed else 0
            )
            if existed and result:
                self._record_progress(record, "EXPIRE", key, b"", seconds, expiry_deadline_ms)

        return await self._acknowledge(record, encode_integer(result))

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

    async def _handle_staleness(
        self, command: Command, writer: asyncio.StreamWriter
    ) -> bytes:
        """Return a JSON cluster staleness report.

        Only the current primary can serve this command.  A replica or
        unconfigured node returns an informative error.
        """
        if len(command.args) != 0:
            return encode_error(
                "ERR wrong number of arguments for 'STALENESS' command"
            )

        if not self._writes_enabled:
            return encode_error(
                "ERR STALENESS must be run on the primary node"
            )

        if self._replicator is None:
            # Primary with no replica configuration — return local-only report.
            if self._progress is not None:
                snap = self._progress.snapshot()
            else:
                return encode_error(
                    "ERR no replication configured on this node"
                )
            report = {
                "primary_node_id": snap.node_id,
                "primary_epoch": snap.epoch,
                "primary_last_applied_sequence": snap.last_applied_sequence,
                "nodes": [
                    {
                        "node_id": snap.node_id,
                        "role": "primary",
                        "epoch": snap.epoch,
                        "last_applied_sequence": snap.last_applied_sequence,
                        "history_digest": snap.history_digest,
                        "status": "LOCAL_PRIMARY",
                        "lag": 0,
                    }
                ],
            }
            return encode_bulk_string(json.dumps(report, indent=2).encode("utf-8"))

        if self._progress is None:
            return encode_error(
                "ERR progress tracking is not initialised on this node"
            )

        from miniredis.staleness import collect_staleness_report

        # Take a snapshot of primary progress before any RPC (consistent baseline).
        primary_snapshot = self._progress.snapshot()

        try:
            report = await collect_staleness_report(
                primary_snapshot=primary_snapshot,
                replicator=self._replicator,
            )
        except Exception as exc:
            logger.error("Failed to collect staleness report: %s", exc, exc_info=True)
            return encode_error(f"ERR staleness collection failed: {exc}")

        return encode_bulk_string(json.dumps(report, indent=2).encode("utf-8"))

    async def _handle_snapshot(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """M17: take (``SNAPSHOT``) or fetch (``SNAPSHOT GET id``) a global snapshot."""
        if self._snapshot_manager is None:
            return encode_error("ERR snapshots are not enabled on this node")
        args = [a.decode("utf-8", errors="replace") for a in command.args]
        if not args:
            result = await self._snapshot_manager.initiate()
            if result["status"] != "COMPLETED":
                return encode_error(
                    f"ERR SNAPSHOT {result.get('snapshot_id')} FAILED: {result.get('reason')}"
                )
            return encode_bulk_string(json.dumps(result, indent=2).encode("utf-8"))
        if len(args) == 2 and args[0].upper() == "GET":
            from miniredis.snapshot import valid_snapshot_id

            if not valid_snapshot_id(args[1]):
                return encode_error("ERR invalid snapshot id")
            result = self._snapshot_manager.get_result(args[1])
            if result is None:
                return encode_error("ERR unknown snapshot id")
            return encode_bulk_string(json.dumps(result, indent=2).encode("utf-8"))
        return encode_error("ERR syntax error: SNAPSHOT [GET <snapshot-id>]")

    def _handle_role(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """M24: JSON with this node's role, epoch, known primary and replication progress."""
        if command.args:
            return encode_error("ERR wrong number of arguments for 'ROLE' command")
        if self._status_provider is not None:
            status = self._status_provider()
        else:
            status = {"role": "primary" if self._writes_enabled else "replica"}
        status["accepts_writes"] = self._writes_enabled and self._write_gate.can_serve_writes
        return encode_bulk_string(json.dumps(status).encode("utf-8"))

    def _handle_verify(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """M20: re-read the persisted ledger from disk and verify its hash chain."""
        if command.args:
            return encode_error("ERR wrong number of arguments for 'VERIFY' command")
        if self._ledger is None:
            return encode_error("ERR persistence is disabled on this node")
        report = self._ledger.verify()
        if not report.ok:
            location = f"record {report.first_invalid_line}: " if report.first_invalid_line else ""
            return encode_error(f"ERR VERIFY FAILED: {location}{report.error}")
        return encode_bulk_string(json.dumps(report.as_dict(), indent=2).encode("utf-8"))

    async def _handle_lock(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """Acquire a distributed or centralized lock on a resource."""
        if len(command.args) != 1:
            return encode_error("ERR wrong number of arguments for 'LOCK' command")

        if self._lock_manager is None:
            return encode_error("ERR lock service not available")

        resource_id = command.args[0].decode("utf-8", errors="replace")
        peername = writer.get_extra_info("peername")
        client_id = f"{peername[0]}:{peername[1]}" if peername else ""

        try:
            token = await self._lock_manager.acquire(resource_id, client_id=client_id)
            return encode_bulk_string(str(token).encode("utf-8"))
        except Exception as exc:
            return encode_error(f"ERR lock acquisition failed: {exc}")

    async def _handle_unlock(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """Release a held lock on a resource."""
        if len(command.args) not in (1, 2):
            return encode_error("ERR wrong number of arguments for 'UNLOCK' command")

        if self._lock_manager is None:
            return encode_error("ERR lock service not available")

        resource_id = command.args[0].decode("utf-8", errors="replace")
        token = command.args[1].decode("utf-8", errors="replace") if len(command.args) == 2 else None
        peername = writer.get_extra_info("peername")
        client_id = f"{peername[0]}:{peername[1]}" if peername else ""

        try:
            await self._lock_manager.release(resource_id, token=token, client_id=client_id)
            return encode_simple_string("OK")
        except Exception as exc:
            return encode_error(f"ERR unlock failed: {exc}")

    async def _handle_fenced_set(self, command: Command, writer: asyncio.StreamWriter) -> bytes:
        """Perform a protected write validated by a fencing token."""
        # FENCED_SET key value token [EX seconds]
        if len(command.args) not in (3, 5):
            return encode_error("ERR wrong number of arguments for 'FENCED_SET' command")

        key = command.args[0]
        value = command.args[1]
        token_str = command.args[2].decode("utf-8", errors="replace")

        ttl: Optional[int] = None
        if len(command.args) == 5:
            opt = command.args[3].upper()
            if opt != b"EX":
                return encode_error(f"ERR syntax error: unexpected option '{command.args[3].decode(errors='replace')}'")
            try:
                ttl = int(command.args[4])
                if ttl <= 0:
                    return encode_error("ERR invalid expire time in 'FENCED_SET' command")
            except ValueError:
                return encode_error("ERR value is not an integer or out of range")

        from miniredis.mutex.protocol import FencingToken, FencingWatermarkError

        try:
            token = FencingToken.from_str(token_str)
        except Exception:
            return encode_error("ERR invalid fencing token format")

        async with self._commit_lock:
            if not self._write_gate.can_serve_writes:
                return encode_error(f"READONLY {self._write_gate.reason}")

            # Validate against existing watermark before applying mutation
            current_wm = self._store.get_watermark(key)
            if current_wm is not None:
                try:
                    current_parsed = FencingToken.from_str(current_wm)
                    if token <= current_parsed:
                        return encode_error(
                            f"ERR stale fencing token {token} (current high-water mark is {current_parsed})"
                        )
                except Exception:
                    pass

            expiry_deadline_ms = 0
            if ttl is not None and self._clock is not None:
                expiry_deadline_ms = int((self._clock() + ttl) * 1000)

            if self._replicator is not None:
                quorum_ok = await self._replicator.propagate_write(
                    "SET", key, value, ttl_seconds=ttl or 0, expiry_deadline_ms=expiry_deadline_ms, fencing_token=str(token)
                )
                if not quorum_ok:
                    return encode_error(f"READONLY quorum write failed: {self._write_gate.reason}")

            try:
                record = self._log_write("SET", key, value, ttl or 0, expiry_deadline_ms, str(token))
            except LedgerWriteError as exc:
                return encode_error(f"ERR write not persisted: {exc}")
            try:
                self._store.fenced_set(
                    key,
                    value,
                    token,
                    ttl_seconds=ttl,
                    expires_at=(expiry_deadline_ms / 1000.0 if expiry_deadline_ms else None),
                )
            except FencingWatermarkError as exc:
                # Logged and replicated: replay and replicas reach the same refusal.
                return encode_error(f"ERR {exc}")

            # Record primary applied write in progress tracker
            self._record_progress(record, "SET", key, value, ttl or 0, expiry_deadline_ms)

        return await self._acknowledge(record, encode_simple_string("OK"))
