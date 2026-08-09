"""
Tests for the in-memory Store.

Uses an injectable clock function for deterministic TTL testing —
no monkey-patching of ``time.monotonic`` required.
"""

from __future__ import annotations

import pytest

from miniredis.storage import Store


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


class MockClock:
    """A manually-advanceable clock for deterministic TTL tests."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


# ──────────────────────────────────────────────────────────────────────
# Basic GET / SET / DELETE
# ──────────────────────────────────────────────────────────────────────


class TestBasicOperations:
    def test_get_missing_key(self) -> None:
        store = Store()
        assert store.get(b"nonexistent") is None

    def test_set_then_get(self) -> None:
        store = Store()
        store.set(b"key", b"value")
        assert store.get(b"key") == b"value"

    def test_set_overwrites(self) -> None:
        store = Store()
        store.set(b"key", b"v1")
        store.set(b"key", b"v2")
        assert store.get(b"key") == b"v2"

    def test_delete_existing(self) -> None:
        store = Store()
        store.set(b"key", b"value")
        assert store.delete(b"key") == 1
        assert store.get(b"key") is None

    def test_delete_nonexistent(self) -> None:
        store = Store()
        assert store.delete(b"nope") == 0

    def test_exists_true(self) -> None:
        store = Store()
        store.set(b"key", b"value")
        assert store.exists(b"key") is True

    def test_exists_false(self) -> None:
        store = Store()
        assert store.exists(b"nope") is False


# ──────────────────────────────────────────────────────────────────────
# TTL / expiry
# ──────────────────────────────────────────────────────────────────────


class TestExpiry:
    def test_set_with_ttl_before_expiry(self) -> None:
        clock = MockClock(start=100.0)
        store = Store(clock=clock)
        store.set(b"key", b"val", ttl_seconds=10)

        clock.advance(5)  # 105 — within TTL
        assert store.get(b"key") == b"val"

    def test_set_with_ttl_after_expiry(self) -> None:
        clock = MockClock(start=100.0)
        store = Store(clock=clock)
        store.set(b"key", b"val", ttl_seconds=10)

        clock.advance(10)  # 110 — exactly at expiry boundary
        assert store.get(b"key") is None

    def test_set_with_ttl_well_past_expiry(self) -> None:
        clock = MockClock(start=0.0)
        store = Store(clock=clock)
        store.set(b"key", b"val", ttl_seconds=5)

        clock.advance(100)
        assert store.get(b"key") is None

    def test_expire_on_existing_key(self) -> None:
        clock = MockClock(start=0.0)
        store = Store(clock=clock)
        store.set(b"key", b"val")

        assert store.expire(b"key", 3) == 1

        clock.advance(2)
        assert store.get(b"key") == b"val"  # still alive

        clock.advance(1)  # now at 3
        assert store.get(b"key") is None  # expired

    def test_expire_on_nonexistent_key(self) -> None:
        store = Store()
        assert store.expire(b"nope", 10) == 0

    def test_exists_respects_expiry(self) -> None:
        clock = MockClock(start=0.0)
        store = Store(clock=clock)
        store.set(b"key", b"val", ttl_seconds=5)

        assert store.exists(b"key") is True
        clock.advance(5)
        assert store.exists(b"key") is False

    def test_delete_respects_expiry(self) -> None:
        """Deleting an expired key should return 0 (already gone)."""
        clock = MockClock(start=0.0)
        store = Store(clock=clock)
        store.set(b"key", b"val", ttl_seconds=5)

        clock.advance(10)
        assert store.delete(b"key") == 0

    def test_set_without_ttl_clears_existing_ttl(self) -> None:
        clock = MockClock(start=0.0)
        store = Store(clock=clock)
        store.set(b"key", b"v1", ttl_seconds=5)

        # Overwrite without TTL — the key should become persistent.
        store.set(b"key", b"v2")
        clock.advance(100)
        assert store.get(b"key") == b"v2"
