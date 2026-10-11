"""
Replication progress tracker for mini-redis (M15).

Each node holds a single ReplicationProgress instance that records:
- The node's current role (primary / replica).
- The current election epoch.
- The highest *contiguous* applied sequence for the current epoch.
- A deterministic chained SHA-256 history fingerprint over all applied
  write operations (including their canonical fields). Each step hashes
  the previous hex digest together with the next canonical operation, so
  the fingerprint is fully described by its hex string and can be restored
  from a leader snapshot when a replica rejoins.
- Whether a sequence gap or rejected operation has been observed.

A gap is set permanently once observed; it is **never** cleared by mere
network recovery. Only an explicit reset (signalling node replacement)
clears a gap.

Design notes
────────────
* All public mutating methods are synchronous (no ``await``).  The
  ``Store`` and ``ReplicaServicer`` paths are likewise synchronous at
  the point of application, so no async coordination is needed here.
* A ``_lock`` (threading.Lock) is used so the tracker can be safely
  called from multiple asyncio tasks without data races.  asyncio does
  not pre-empt between non-await boundaries, but the lock is cheap
  insurance against future refactors and cross-thread testing.
* ``snapshot()`` returns a *copy* of all mutable fields so RPC handlers
  cannot observe partially-updated state.
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from typing import Literal, Optional


Role = Literal["primary", "replica", "unknown"]

# Fingerprint of an empty history (no writes applied yet).
EMPTY_HISTORY_DIGEST = hashlib.sha256(b"").hexdigest()


def _chain_digest(previous_digest: str, canonical: bytes) -> str:
    """Extend a history fingerprint by one canonical operation."""
    return hashlib.sha256(previous_digest.encode("ascii") + canonical).hexdigest()


@dataclass(frozen=True)
class ProgressSnapshot:
    """Immutable point-in-time view of a node's replication progress."""

    node_id: str
    role: Role
    epoch: int
    last_applied_sequence: int
    history_digest: str       # hex-encoded SHA-256 of the applied history
    gap_detected: bool
    gap_expected_sequence: int   # 0 when no gap
    gap_observed_sequence: int   # 0 when no gap


def _canonical_op(
    epoch: int,
    sequence: int,
    op_type: str,
    key: str,
    value: bytes,
    ttl_seconds: int,
    expiry_deadline_ms: int,
) -> bytes:
    """Produce a deterministic canonical byte representation for a write.

    The same operation on both primary and replica must produce the
    *same* canonical bytes regardless of when the hash is computed.

    Rules:
    - Use JSON with sorted keys for the text fields.
    - Encode value as a latin-1 escaped string so it can be JSON-serialised
      deterministically without base64 overhead.
    - Include expiry_deadline_ms (M14 common absolute deadline) when non-zero.
    - Do NOT include Lamport timestamps — they differ per node.
    - Do NOT include wall-clock receive times.
    """
    record = {
        "epoch": epoch,
        "seq": sequence,
        "op": op_type.upper(),
        "key": key,
        "ttl": ttl_seconds,
        "deadline_ms": expiry_deadline_ms,
    }
    # Value is arbitrary bytes; encode deterministically as latin-1 escape.
    record["val"] = value.decode("latin-1") if value else ""
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=True)
    return canonical.encode("utf-8")


class ReplicationProgress:
    """Thread-safe per-node replication progress tracker.

    All components that belong to one node (ReplicaServicer, Replicator,
    Dispatcher, BullyElection) share the **same** instance.  Server.py
    creates the instance and injects it into each component.
    """

    def __init__(self, node_id: str, role: Role = "replica", epoch: int = 0) -> None:
        self._node_id = node_id
        self._role: Role = role
        self._epoch = epoch
        self._last_applied_sequence = 0
        self._history_digest = EMPTY_HISTORY_DIGEST
        self._gap_detected = False
        self._gap_expected_sequence = 0
        self._gap_observed_sequence = 0
        self._lock = threading.Lock()

    # ── role / epoch management ──────────────────────────────────────

    def set_role(self, role: Role) -> None:
        """Update the tracked role (primary / replica)."""
        with self._lock:
            self._role = role

    def advance_epoch(self, new_epoch: int) -> None:
        """Transition to *new_epoch*.

        The per-epoch sequence counter resets to 0.  The history
        fingerprint is *not* cleared — the accumulated writes before
        promotion remain part of this node's history, and the new
        epoch's writes are appended on top.  This lets the fingerprint
        correctly detect divergence even across leadership changes.

        Obsolete-epoch calls are silently ignored.
        """
        with self._lock:
            if new_epoch <= self._epoch:
                return
            self._epoch = new_epoch
            self._last_applied_sequence = 0
            # Do not clear the history digest — retain cross-epoch continuity.
            # Do not clear gap — a previously detected gap is never auto-cleared.

    def update_role_and_epoch(self, role: Role, epoch: int) -> None:
        """Convenience wrapper used by election code."""
        with self._lock:
            self._role = role
            if epoch > self._epoch:
                self._epoch = epoch
                self._last_applied_sequence = 0

    # ── write recording ──────────────────────────────────────────────

    def record_applied(
        self,
        epoch: int,
        sequence: int,
        op_type: str,
        key: str,
        value: bytes,
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> None:
        """Record a successfully applied write operation.

        Rules enforced here (mirrors ReplicaServicer acceptance logic):
        1. Obsolete-epoch writes are ignored.
        2. Duplicate sequences are ignored (idempotent).
        3. Non-contiguous sequences (gaps) are recorded as gaps and NOT
           applied to the sequence counter.
        4. Only the next contiguous sequence advances the counter and
           updates the fingerprint.
        """
        with self._lock:
            if epoch < self._epoch:
                # Obsolete epoch — silently ignore.
                return

            if epoch > self._epoch:
                # New epoch accepted (should match advance_epoch call, but handle it here too).
                self._epoch = epoch
                self._last_applied_sequence = 0

            if sequence <= self._last_applied_sequence:
                # Duplicate — already applied.
                return

            expected = self._last_applied_sequence + 1
            if sequence != expected:
                # Gap detected.
                if not self._gap_detected:
                    self._gap_detected = True
                    self._gap_expected_sequence = expected
                    self._gap_observed_sequence = sequence
                return

            # Contiguous write — advance and fingerprint.
            self._last_applied_sequence = sequence
            canonical = _canonical_op(
                epoch, sequence, op_type, key, value, ttl_seconds, expiry_deadline_ms
            )
            self._history_digest = _chain_digest(self._history_digest, canonical)

    def record_gap(self, epoch: int, expected_sequence: int, observed_sequence: int) -> None:
        """Explicitly record a sequence gap (e.g. when ReplicaServicer rejects one).

        Only recorded if no prior gap exists and the epoch is current.
        """
        with self._lock:
            if epoch < self._epoch:
                return
            if self._gap_detected:
                return
            self._gap_detected = True
            self._gap_expected_sequence = expected_sequence
            self._gap_observed_sequence = observed_sequence

    def reset_gap(self) -> None:
        """Explicitly clear a previously detected gap.

        This should only be called when the implementation can establish
        that the missing writes have been resolved (e.g. node replaced
        with a verified consistent snapshot).  M15 itself never calls
        this automatically.
        """
        with self._lock:
            self._gap_detected = False
            self._gap_expected_sequence = 0
            self._gap_observed_sequence = 0

    def restore_from_snapshot(
        self,
        epoch: int,
        sequence: int,
        history_digest: str,
        role: Role = "replica",
    ) -> None:
        """Restore progress tracker state from a verified leader snapshot.

        Because the fingerprint is a hash chain, adopting the leader's hex
        digest is enough for subsequent writes to extend it identically.
        """
        with self._lock:
            self._role = role
            self._epoch = epoch
            self._last_applied_sequence = sequence
            self._history_digest = history_digest
            self._gap_detected = False
            self._gap_expected_sequence = 0
            self._gap_observed_sequence = 0


    # ── snapshot ────────────────────────────────────────────────────

    def snapshot(self) -> ProgressSnapshot:
        """Return an immutable point-in-time copy of the progress state."""
        with self._lock:
            return ProgressSnapshot(
                node_id=self._node_id,
                role=self._role,
                epoch=self._epoch,
                last_applied_sequence=self._last_applied_sequence,
                history_digest=self._history_digest,
                gap_detected=self._gap_detected,
                gap_expected_sequence=self._gap_expected_sequence,
                gap_observed_sequence=self._gap_observed_sequence,
            )

    # ── convenience properties (read-only, for introspection) ────────

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def role(self) -> Role:
        with self._lock:
            return self._role
