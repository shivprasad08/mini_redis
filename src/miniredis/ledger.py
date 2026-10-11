"""
Persistent hash-chained write log (M20).

Every committed mutation is appended to ``<data-dir>/ledger.jsonl`` *before*
it is applied to the in-memory store (write-ahead), and the store is rebuilt
from the log when the node restarts.

Record format (version 1)
─────────────────────────
One JSON object per line, terminated by ``\\n``. Two kinds:

``op`` — one replicated write::

    {"v":1,"kind":"op","epoch":E,"seq":S,"op":"SET"|"DEL"|"EXPIRE",
     "key":<b64>,"value":<b64>,"ttl":<int>,"deadline_ms":<int>,
     "token":<fencing token or "">,"prev":<hex>,"hash":<hex>}

``checkpoint`` — a leader snapshot installed by a recovering replica::

    {"v":1,"kind":"checkpoint","epoch":E,"seq":S,"digest":<history digest>,
     "adopt":<leader's chain head or "">,"items":[[<b64 key>,<b64 value>,<deadline_ms>],...],
     "watermarks":{<b64 key>:<token>},"prev":<hex>,"hash":<hex>}

``epoch``/``seq`` are the authoritative leadership epoch and per-epoch
replication sequence; ``deadline_ms`` is the absolute expiry deadline on the
cluster's corrected wall clock (0 = no TTL), so replay never extends a TTL.

Hash chain
──────────
``hash = sha256(canonical_json(record without "hash"))`` where canonical JSON
has sorted keys, no whitespace and ASCII escapes. ``prev`` is the previous
record's chain head; the first record links to ``GENESIS_HASH``. The chain
head after an ``op`` is its ``hash``. After a ``checkpoint`` it is ``adopt``
(the leader's head at the snapshot point) when present — so a replica that
caught up by snapshot keeps producing exactly the leader's hashes for later
writes — otherwise the checkpoint's own hash. No node-specific field is
hashed, so replicas applying the same operation stream build identical chains.

Ordering rules checked on replay and by VERIFY: within an epoch sequences are
contiguous (S, S+1, ...); a newer epoch restarts at sequence 1; a checkpoint
may not move to an older epoch.

Durability policies (``--fsync``)
──────────────────────────────────
``always``   write + flush + fsync inside the append, before the mutation is
             applied; the client/replication ack follows. Survives process and
             OS crashes for every acknowledged write.
``batch``    write + flush (to the OS) inside the append; a group-commit task
             fsyncs at most every ``fsync_interval_ms`` (default 2 ms) and
             acknowledgements wait for the fsync covering their record. Same
             guarantee as ``always`` for acknowledged writes; reads may observe
             a write before its fsync.
``interval`` write + flush inside the append; fsync every ``fsync_interval_ms``
             (default 1000 ms) in the background; acks do not wait. Survives a
             process crash (data is in the OS page cache); an OS crash or power
             loss can lose up to one interval of acknowledged writes.

Failure handling
────────────────
* A trailing record without its terminating newline is an interrupted append:
  it is moved to ``ledger.jsonl.torn-<ms>`` and the file truncated to the last
  complete record.
* Any *complete* record that fails parsing, hash, link or ordering checks
  raises ``LedgerCorruptionError``: the node refuses to start (fail closed) and
  the file is left untouched for investigation.
* A failed write or fsync marks the ledger failed; every later append raises,
  so the node stops acknowledging writes instead of losing them silently.
* An OS-level lock on ``<data-dir>/LOCK`` prevents two processes from sharing
  one ledger.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

FORMAT_VERSION = 1
GENESIS_HASH = hashlib.sha256(b"miniredis-ledger:v1:genesis").hexdigest()
FSYNC_POLICIES = ("always", "batch", "interval")
DEFAULT_INTERVAL_MS = {"always": 0, "batch": 2, "interval": 1000}
LEDGER_FILE = "ledger.jsonl"
OPS = ("SET", "DEL", "EXPIRE")


class LedgerError(Exception):
    """Base class for ledger failures."""


class LedgerCorruptionError(LedgerError):
    def __init__(self, line_no: int, reason: str) -> None:
        super().__init__(f"line {line_no}: {reason}")
        self.line_no = line_no
        self.reason = reason


class LedgerWriteError(LedgerError):
    """An append or fsync failed; the ledger refuses further writes."""


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def record_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), validate=True)


@dataclass(frozen=True)
class LedgerRecord:
    payload: dict[str, Any]
    hash: str
    end_offset: int = 0  # file offset just past this record (for durability waits)

    @property
    def kind(self) -> str:
        return self.payload["kind"]

    @property
    def epoch(self) -> int:
        return int(self.payload["epoch"])

    @property
    def seq(self) -> int:
        return int(self.payload["seq"])


@dataclass
class ChainState:
    head: str = GENESIS_HASH
    epoch: int = 0
    seq: int = 0
    records: int = 0


_OP_FIELDS = {"v", "kind", "epoch", "seq", "op", "key", "value", "ttl", "deadline_ms", "token", "prev"}
_CHECKPOINT_FIELDS = {"v", "kind", "epoch", "seq", "digest", "adopt", "items", "watermarks", "prev"}


def check_record(state: ChainState, line_no: int, obj: Any) -> LedgerRecord:
    """Validate one decoded record against the chain so far and advance *state*."""
    if not isinstance(obj, dict) or "hash" not in obj:
        raise LedgerCorruptionError(line_no, "record is not an object with a hash")
    claimed = obj["hash"]
    payload = {k: v for k, v in obj.items() if k != "hash"}
    if payload.get("v") != FORMAT_VERSION:
        raise LedgerCorruptionError(line_no, f"unsupported format version {payload.get('v')!r}")
    kind = payload.get("kind")
    expected_fields = _OP_FIELDS if kind == "op" else _CHECKPOINT_FIELDS if kind == "checkpoint" else None
    if expected_fields is None:
        raise LedgerCorruptionError(line_no, f"unknown record kind {kind!r}")
    if set(payload) != expected_fields:
        raise LedgerCorruptionError(line_no, f"fields {sorted(payload)} do not match a {kind} record")
    if record_hash(payload) != claimed:
        raise LedgerCorruptionError(line_no, "hash does not match record content (modified record)")
    if payload["prev"] != state.head:
        raise LedgerCorruptionError(
            line_no, "previous-hash link broken (record removed, inserted or reordered)"
        )
    epoch, seq = payload["epoch"], payload["seq"]
    if not isinstance(epoch, int) or not isinstance(seq, int) or epoch < 0 or seq < 0:
        raise LedgerCorruptionError(line_no, "epoch/seq must be non-negative integers")

    if kind == "op":
        if payload["op"] not in OPS:
            raise LedgerCorruptionError(line_no, f"unknown operation {payload['op']!r}")
        if state.records and epoch < state.epoch:
            raise LedgerCorruptionError(line_no, f"epoch {epoch} is older than {state.epoch}")
        if state.records and epoch == state.epoch:
            if seq != state.seq + 1:
                raise LedgerCorruptionError(
                    line_no, f"sequence {seq} does not follow {state.seq} in epoch {epoch}"
                )
        elif seq != 1:
            raise LedgerCorruptionError(line_no, f"epoch {epoch} must start at sequence 1, got {seq}")
        try:
            unb64(payload["key"])
            unb64(payload["value"])
        except (ValueError, TypeError) as exc:
            raise LedgerCorruptionError(line_no, f"invalid base64 payload: {exc}") from None
        state.head = claimed
    else:
        if state.records and epoch < state.epoch:
            raise LedgerCorruptionError(line_no, f"checkpoint moves back to epoch {epoch} < {state.epoch}")
        state.head = payload["adopt"] or claimed
    state.epoch, state.seq = epoch, seq
    state.records += 1
    return LedgerRecord(payload, claimed)


@dataclass
class VerifyReport:
    ok: bool
    records: int
    last_epoch: int
    last_sequence: int
    head_hash: str
    first_invalid_line: Optional[int] = None
    error: Optional[str] = None
    torn_tail_bytes: int = 0
    path: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "path": self.path,
            "records": self.records,
            "last_epoch": self.last_epoch,
            "last_sequence": self.last_sequence,
            "head_hash": self.head_hash,
            "first_invalid_line": self.first_invalid_line,
            "error": self.error,
            "torn_tail_bytes": self.torn_tail_bytes,
        }


def scan(data: bytes) -> tuple[list[LedgerRecord], ChainState, int]:
    """Validate *data* (whole file contents). Returns records, chain state, torn-tail size.

    Raises LedgerCorruptionError for any invalid complete record.
    """
    state = ChainState()
    records: list[LedgerRecord] = []
    complete_end = data.rfind(b"\n") + 1
    torn = len(data) - complete_end
    offset = 0
    for line_no, line in enumerate(data[:complete_end].split(b"\n")[:-1], start=1):
        offset += len(line) + 1
        try:
            obj = json.loads(line.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LedgerCorruptionError(line_no, f"unparsable record: {exc}") from None
        record = check_record(state, line_no, obj)
        records.append(LedgerRecord(record.payload, record.hash, offset))
    return records, state, torn


def verify_file(path: Path) -> VerifyReport:
    """Independently re-read and verify a ledger file."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        data = b""
    try:
        records, state, torn = scan(data)
    except LedgerCorruptionError as exc:
        return VerifyReport(False, 0, 0, 0, "", exc.line_no, exc.reason, path=str(path))
    error = f"incomplete trailing record ({torn} bytes)" if torn else None
    return VerifyReport(torn == 0, state.records, state.epoch, state.seq, state.head,
                        state.records + 1 if torn else None, error, torn, str(path))


class _DirLock:
    """Exclusive OS-level lock on <dir>/LOCK, released when the process exits."""

    def __init__(self, directory: Path) -> None:
        self._file = open(directory / "LOCK", "a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            raise LedgerError(f"data directory {directory} is in use by another process") from exc

    def release(self) -> None:
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        self._file.close()


class WriteLedger:
    """Append-only hash-chained log for one node."""

    def __init__(
        self,
        directory: str | os.PathLike[str],
        fsync_policy: str = "always",
        fsync_interval_ms: Optional[int] = None,
    ) -> None:
        if fsync_policy not in FSYNC_POLICIES:
            raise ValueError(f"fsync policy must be one of {FSYNC_POLICIES}")
        self.directory = Path(directory)
        self.path = self.directory / LEDGER_FILE
        self.fsync_policy = fsync_policy
        interval = DEFAULT_INTERVAL_MS[fsync_policy] if fsync_interval_ms is None else fsync_interval_ms
        self._interval_seconds = max(0, interval) / 1000.0
        self._state = ChainState()
        self._file: Optional[Any] = None
        self._lock: Optional[_DirLock] = None
        self._written_offset = 0
        self._durable_offset = 0
        self._failure: Optional[str] = None
        self._dirty = asyncio.Event()
        self._waiters: list[tuple[int, asyncio.Future[None]]] = []
        self._flusher: Optional[asyncio.Task[None]] = None
        self._closing = False

    # ── lifecycle ────────────────────────────────────────────────────

    def open(self) -> list[LedgerRecord]:
        """Lock the directory, validate the ledger and return its records for replay."""
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = _DirLock(self.directory)
        try:
            data = self.path.read_bytes() if self.path.exists() else b""
            records, state, torn = scan(data)  # raises LedgerCorruptionError: fail closed
            if torn:
                complete = len(data) - torn
                torn_path = self.path.with_name(f"{LEDGER_FILE}.torn-{int(time.time() * 1000)}")
                torn_path.write_bytes(data[complete:])
                with open(self.path, "r+b") as handle:
                    handle.truncate(complete)
                    handle.flush()
                    os.fsync(handle.fileno())
                logger.warning(
                    "Ledger %s: discarded interrupted trailing append (%d bytes, saved to %s)",
                    self.path, torn, torn_path.name,
                )
            self._state = state
            self._file = open(self.path, "ab")
            self._written_offset = self._durable_offset = self._file.tell()
        except BaseException:
            self._lock.release()
            self._lock = None
            raise
        logger.info(
            "Ledger %s: %d records, epoch=%s seq=%s head=%s (fsync=%s)",
            self.path, state.records, state.epoch, state.seq, state.head[:12], self.fsync_policy,
        )
        return records

    async def start(self) -> None:
        if self.fsync_policy != "always" and self._flusher is None:
            self._flusher = asyncio.create_task(self._flush_loop())

    async def close(self) -> None:
        self._closing = True
        if self._flusher is not None:
            self._dirty.set()
            self._flusher.cancel()
            await asyncio.gather(self._flusher, return_exceptions=True)
            self._flusher = None
        if self._file is not None:
            try:
                if self._failure is None:
                    self._file.flush()
                    os.fsync(self._file.fileno())
                    self._mark_durable(self._written_offset)
            finally:
                self._file.close()
                self._file = None
        for _, future in self._waiters:
            if not future.done():
                future.set_exception(LedgerWriteError("ledger closed"))
        self._waiters.clear()
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    # ── state ────────────────────────────────────────────────────────

    @property
    def head_hash(self) -> str:
        return self._state.head

    @property
    def last_epoch(self) -> int:
        return self._state.epoch

    @property
    def last_sequence(self) -> int:
        return self._state.seq

    @property
    def record_count(self) -> int:
        return self._state.records

    @property
    def failed(self) -> Optional[str]:
        return self._failure

    def head(self) -> dict[str, Any]:
        return {
            "records": self._state.records,
            "epoch": self._state.epoch,
            "seq": self._state.seq,
            "hash": self._state.head,
        }

    # ── appends ──────────────────────────────────────────────────────

    def next_local_sequence(self, epoch: int) -> int:
        """Sequence for a write that has no replicator to allocate one."""
        return self._state.seq + 1 if epoch == self._state.epoch and self._state.records else 1

    def append_op(
        self,
        *,
        epoch: int,
        seq: int,
        op: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        deadline_ms: int = 0,
        token: str = "",
    ) -> LedgerRecord:
        payload = {
            "v": FORMAT_VERSION,
            "kind": "op",
            "epoch": int(epoch),
            "seq": int(seq),
            "op": op.upper(),
            "key": b64(key),
            "value": b64(value),
            "ttl": int(ttl_seconds),
            "deadline_ms": int(deadline_ms),
            "token": token or "",
            "prev": self._state.head,
        }
        return self._append(payload)

    def append_checkpoint(
        self,
        *,
        epoch: int,
        seq: int,
        digest: str,
        adopt_head: str,
        items: list[tuple[bytes, bytes, Optional[float]]],
        watermarks: dict[bytes, str],
    ) -> LedgerRecord:
        payload = {
            "v": FORMAT_VERSION,
            "kind": "checkpoint",
            "epoch": int(epoch),
            "seq": int(seq),
            "digest": digest,
            "adopt": adopt_head,
            "items": [
                [b64(k), b64(v), int(exp * 1000) if exp is not None else 0] for k, v, exp in items
            ],
            "watermarks": {b64(k): str(t) for k, t in watermarks.items()},
            "prev": self._state.head,
        }
        return self._append(payload)

    def _append(self, payload: dict[str, Any]) -> LedgerRecord:
        """Validate, write and (policy permitting) fsync one record. Synchronous on purpose:
        callers append and apply the mutation with no await in between."""
        if self._failure is not None:
            raise LedgerWriteError(f"ledger unavailable after earlier failure: {self._failure}")
        if self._file is None:
            raise LedgerWriteError("ledger is not open")
        digest = record_hash(payload)
        trial = ChainState(self._state.head, self._state.epoch, self._state.seq, self._state.records)
        try:
            check_record(trial, self._state.records + 1, {**payload, "hash": digest})
        except LedgerCorruptionError as exc:
            # Refuse to persist a record the verifier would reject (e.g. a sequence gap).
            raise LedgerWriteError(f"refusing to append invalid record: {exc.reason}") from None
        line = canonical_json({**payload, "hash": digest}) + b"\n"
        try:
            self._file.write(line)
            self._file.flush()
            if self.fsync_policy == "always":
                os.fsync(self._file.fileno())
        except OSError as exc:
            self._failure = f"{type(exc).__name__}: {exc}"
            logger.error("Ledger %s write failed; refusing further writes: %s", self.path, exc)
            raise LedgerWriteError(self._failure) from exc
        self._state = trial
        self._written_offset += len(line)
        if self.fsync_policy == "always":
            self._durable_offset = self._written_offset
        else:
            self._dirty.set()
        return LedgerRecord(payload, digest, self._written_offset)

    async def wait_durable(self, record: LedgerRecord) -> None:
        """Return once *record* satisfies the policy's acknowledgement rule."""
        if self.fsync_policy != "batch" or record.end_offset <= self._durable_offset:
            if self._failure is not None and record.end_offset > self._durable_offset:
                raise LedgerWriteError(self._failure)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append((record.end_offset, future))
        self._dirty.set()
        await future

    def _mark_durable(self, offset: int) -> None:
        self._durable_offset = max(self._durable_offset, offset)
        remaining = []
        for end, future in self._waiters:
            if end <= self._durable_offset:
                if not future.done():
                    future.set_result(None)
            else:
                remaining.append((end, future))
        self._waiters = remaining

    async def _flush_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closing:
            await self._dirty.wait()
            self._dirty.clear()
            if self._interval_seconds:
                await asyncio.sleep(self._interval_seconds)  # group-commit window
            if self._file is None or self._failure is not None:
                continue
            target = self._written_offset
            if target <= self._durable_offset:
                continue
            try:
                await loop.run_in_executor(None, os.fsync, self._file.fileno())
            except OSError as exc:
                self._failure = f"fsync failed: {exc}"
                logger.error("Ledger %s fsync failed; refusing further writes: %s", self.path, exc)
                for _, future in self._waiters:
                    if not future.done():
                        future.set_exception(LedgerWriteError(self._failure))
                self._waiters.clear()
                continue
            self._mark_durable(target)

    # ── verification ─────────────────────────────────────────────────

    def verify(self) -> VerifyReport:
        """Re-read the file from disk and check it independently of in-memory state."""
        if self._file is not None:
            self._file.flush()
        report = verify_file(self.path)
        if report.ok and (report.records, report.head_hash) != (self._state.records, self._state.head):
            report.ok = False
            report.error = (
                f"file has {report.records} records ending at {report.head_hash[:12]}, but this node "
                f"committed {self._state.records} records ending at {self._state.head[:12]} "
                "(file truncated or rewritten while running)"
            )
        return report


def replay(records: list[LedgerRecord], store: Any, progress: Any = None) -> tuple[int, int]:
    """Rebuild *store* (and *progress*) from validated records, in log order.

    Nothing is appended while replaying, so restarting never duplicates writes.
    Deadlines are absolute, so a key whose TTL passed while the node was down
    is already expired after replay. Returns the last (epoch, sequence).
    """
    epoch = seq = 0
    for record in records:
        p = record.payload
        epoch, seq = int(p["epoch"]), int(p["seq"])
        if p["kind"] == "checkpoint":
            store.replace_all(
                [(unb64(k), unb64(v), d / 1000.0 if d else None) for k, v, d in p["items"]],
                {unb64(k): t for k, t in p["watermarks"].items()},
            )
            if progress is not None:
                progress.restore_from_snapshot(epoch=epoch, sequence=seq, history_digest=p["digest"], role="replica")
            continue

        key, value = unb64(p["key"]), unb64(p["value"])
        deadline_ms, ttl = int(p["deadline_ms"]), int(p["ttl"])
        expires_at = deadline_ms / 1000.0 if deadline_ms > 0 else None
        if p["op"] == "SET":
            if p["token"]:
                try:
                    store.fenced_set(key, value, p["token"], expires_at=expires_at)
                except Exception:
                    pass  # the original write was refused the same way
            else:
                store.set(key, value, ttl_seconds=None if expires_at else (ttl or None), expires_at=expires_at)
        elif p["op"] == "DEL":
            store.delete(key)
        elif p["op"] == "EXPIRE":
            if expires_at is not None:
                store.expire_at(key, expires_at)
            else:
                store.expire(key, ttl)
        if progress is not None:
            progress.record_applied(
                epoch=epoch,
                sequence=seq,
                op_type=p["op"],
                key=key.decode("utf-8", errors="replace"),
                value=value,
                ttl_seconds=ttl,
                expiry_deadline_ms=deadline_ms,
            )
    return epoch, seq
