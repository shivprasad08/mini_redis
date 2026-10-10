"""
In-memory key-value store for mini-redis.

This module owns *all* mutable state for the node's key-value data.
It exposes a pure-Python API that returns native types (``bytes``,
``int``, ``bool``, ``None``) — it has **no knowledge** of the wire
protocol.  The :mod:`miniredis.dispatcher` is responsible for mapping
these return values into RESP-encoded responses.

Concurrency assumption
──────────────────────
``Store`` is designed to run inside a single ``asyncio`` event loop.
All public methods are synchronous and execute atomically from the
event loop's perspective (no ``await`` points), so no locking is
required.  Do **not** call ``Store`` methods from multiple OS threads.

Expiry strategy — lazy eviction
───────────────────────────────
Keys with a TTL are tracked in a parallel ``_expiries`` dict that maps
each key to its absolute expiry time (``time.monotonic()``-based).
Expired keys are **not** swept in the background; instead, every
read-path operation (``get``, ``exists``) calls ``_evict_if_expired``
so that stale keys are removed on access.  This keeps the
implementation simple and avoids background tasks, at the cost of
stale keys occupying memory until they are next touched.
"""

from __future__ import annotations

import time
from typing import Callable, Optional


# Type alias for a clock function — enables deterministic testing
# without monkey-patching ``time.monotonic`` globally.
ClockFn = Callable[[], float]


class Store:
    """In-memory key-value store with optional per-key TTL.

    Parameters
    ----------
    clock:
        A zero-argument callable returning the current time as a
        ``float`` (seconds).  Defaults to :func:`time.monotonic`.
        Inject a custom clock in tests for deterministic expiry
        behaviour.
    """

    def __init__(self, clock: Optional[ClockFn] = None) -> None:
        self._data: dict[bytes, bytes] = {}
        self._expiries: dict[bytes, float] = {}
        self._clock: ClockFn = clock or time.monotonic

    # ── public API ──────────────────────────────────────────────────

    def get(self, key: bytes) -> Optional[bytes]:
        """Return the value for *key*, or ``None`` if missing/expired."""
        self._evict_if_expired(key)
        return self._data.get(key)

    def set(
        self,
        key: bytes,
        value: bytes,
        ttl_seconds: Optional[int] = None,
        expires_at: Optional[float] = None,
    ) -> None:
        """Set *key* to *value*, optionally with a TTL in seconds.

        If *ttl_seconds* is ``None`` any existing TTL on the key is
        removed (the key becomes persistent).
        """
        self._data[key] = value
        if expires_at is not None:
            self._expiries[key] = expires_at
        elif ttl_seconds is not None:
            self._expiries[key] = self._clock() + ttl_seconds
        else:
            # Overwriting a key clears its old TTL.
            self._expiries.pop(key, None)

    def delete(self, key: bytes) -> int:
        """Delete *key*.  Returns 1 if the key existed, 0 otherwise."""
        self._evict_if_expired(key)
        if key in self._data:
            del self._data[key]
            self._expiries.pop(key, None)
            return 1
        return 0

    def expire(self, key: bytes, ttl_seconds: int) -> int:
        """Set a TTL on an existing *key*.

        Returns 1 if the key exists and the timeout was set,
        0 if the key does not exist.
        """
        self._evict_if_expired(key)
        if key not in self._data:
            return 0
        self._expiries[key] = self._clock() + ttl_seconds
        return 1

    def expire_at(self, key: bytes, expires_at: float) -> int:
        """Set an absolute expiry deadline using the store's clock domain."""
        self._evict_if_expired(key)
        if key not in self._data:
            return 0
        self._expiries[key] = expires_at
        return 1

    def exists(self, key: bytes) -> bool:
        """Return ``True`` if *key* exists and has not expired."""
        self._evict_if_expired(key)
        return key in self._data

    # ── internal helpers ────────────────────────────────────────────

    def _is_expired(self, key: bytes) -> bool:
        """Check whether *key* has a TTL that has elapsed."""
        expiry = self._expiries.get(key)
        if expiry is None:
            return False
        return self._clock() >= expiry

    def _evict_if_expired(self, key: bytes) -> None:
        """Remove *key* from both data and expiry dicts if it has expired."""
        if self._is_expired(key):
            self._data.pop(key, None)
            self._expiries.pop(key, None)
