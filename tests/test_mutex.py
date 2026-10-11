"""
Unit tests for M16 Distributed Mutual Exclusion (Ricart-Agrawala & Centralized Baseline).
"""

import asyncio
import pytest

from miniredis.logical_clock import LamportClock
from miniredis.mutex.protocol import FencingToken, FencingWatermarkError, LockState
from miniredis.mutex.manager import RicartAgrawalaManager, CentralizedLockManager
from miniredis.cluster import WriteAdmissionGate
from miniredis.storage import Store


class TestFencingToken:
    def test_token_ordering_by_epoch(self):
        t1 = FencingToken(epoch=1, lamport_ts=10, counter=1, node_id="node-a")
        t2 = FencingToken(epoch=2, lamport_ts=5, counter=1, node_id="node-b")
        assert t1 < t2
        assert t2 > t1

    def test_token_ordering_by_lamport(self):
        t1 = FencingToken(epoch=1, lamport_ts=5, counter=2, node_id="node-a")
        t2 = FencingToken(epoch=1, lamport_ts=10, counter=1, node_id="node-b")
        assert t1 < t2

    def test_token_ordering_by_counter(self):
        t1 = FencingToken(epoch=1, lamport_ts=5, counter=1, node_id="node-a")
        t2 = FencingToken(epoch=1, lamport_ts=5, counter=2, node_id="node-a")
        assert t1 < t2

    def test_token_ordering_by_node_id(self):
        t1 = FencingToken(epoch=1, lamport_ts=5, counter=1, node_id="node-a")
        t2 = FencingToken(epoch=1, lamport_ts=5, counter=1, node_id="node-b")
        assert t1 < t2

    def test_token_serialization_and_parsing(self):
        t1 = FencingToken(epoch=3, lamport_ts=15, counter=42, node_id="node-xyz")
        s = t1.to_str()
        parsed = FencingToken.from_str(s)
        assert parsed == t1
        assert parsed.epoch == 3
        assert parsed.lamport_ts == 15
        assert parsed.counter == 42
        assert parsed.node_id == "node-xyz"

    def test_parse_integer_token(self):
        parsed = FencingToken.from_str(100)
        assert parsed.counter == 100


class TestStoreFencingWatermark:
    def test_fenced_set_records_watermark(self):
        store = Store()
        t1 = FencingToken(epoch=1, lamport_ts=10, counter=1, node_id="node-a")
        store.fenced_set(b"k1", b"v1", t1)
        assert store.get(b"k1") == b"v1"
        assert store.get_watermark(b"k1") == t1

    def test_fenced_set_rejects_older_or_equal_token(self):
        store = Store()
        t1 = FencingToken(epoch=1, lamport_ts=10, counter=1, node_id="node-a")
        t2 = FencingToken(epoch=1, lamport_ts=20, counter=2, node_id="node-a")
        store.fenced_set(b"k1", b"v1", t2)

        # Older token rejected
        with pytest.raises(FencingWatermarkError):
            store.fenced_set(b"k1", b"stale_val", t1)

        # Equal token rejected
        with pytest.raises(FencingWatermarkError):
            store.fenced_set(b"k1", b"stale_val_2", t2)

        # Key value was not overwritten by rejected write
        assert store.get(b"k1") == b"v1"

    def test_fenced_set_accepts_monotonically_higher_token(self):
        store = Store()
        t1 = FencingToken(epoch=1, lamport_ts=10, counter=1, node_id="node-a")
        t2 = FencingToken(epoch=1, lamport_ts=20, counter=2, node_id="node-a")
        store.fenced_set(b"k1", b"v1", t1)
        store.fenced_set(b"k1", b"v2", t2)
        assert store.get(b"k1") == b"v2"
        assert store.get_watermark(b"k1") == t2


class TestRicartAgrawalaProtocolLogic:
    @pytest.mark.asyncio
    async def test_single_node_acquire_release(self):
        mgr = RicartAgrawalaManager(
            node_id="node-a",
            peer_addresses={},  # Single node cluster
            lamport_clock=LamportClock(),
        )
        token = await mgr.acquire("res1", client_id="client-1")
        assert token.node_id == "node-a"
        status = mgr.get_status("res1")
        assert status["state"] == LockState.HELD.value
        assert status["current_holder"] == "node-a"

        # Release
        ok = await mgr.release("res1", token=str(token), client_id="client-1")
        assert ok is True
        status = mgr.get_status("res1")
        assert status["state"] == LockState.RELEASED.value

    @pytest.mark.asyncio
    async def test_unauthorized_release_rejected(self):
        mgr = RicartAgrawalaManager(
            node_id="node-a",
            peer_addresses={},
            lamport_clock=LamportClock(),
        )
        token = await mgr.acquire("res1", client_id="client-1")

        # Wrong client
        with pytest.raises(PermissionError):
            await mgr.release("res1", token=str(token), client_id="client-2")

        # Wrong token
        wrong_token = FencingToken(epoch=99, lamport_ts=99, counter=99, node_id="other")
        with pytest.raises(PermissionError):
            await mgr.release("res1", token=str(wrong_token), client_id="client-1")

        # Valid release succeeds
        await mgr.release("res1", token=str(token), client_id="client-1")

    @pytest.mark.asyncio
    async def test_duplicate_acquire_on_same_resource_raises(self):
        mgr = RicartAgrawalaManager(
            node_id="node-a",
            peer_addresses={},
            lamport_clock=LamportClock(),
        )
        await mgr.acquire("res1")
        with pytest.raises(RuntimeError):
            await mgr.acquire("res1")

    @pytest.mark.asyncio
    async def test_separate_resources_independent(self):
        mgr = RicartAgrawalaManager(
            node_id="node-a",
            peer_addresses={},
            lamport_clock=LamportClock(),
        )
        t1 = await mgr.acquire("res1")
        t2 = await mgr.acquire("res2")
        assert mgr.get_status("res1")["state"] == LockState.HELD.value
        assert mgr.get_status("res2")["state"] == LockState.HELD.value
        await mgr.release("res1", token=str(t1))
        await mgr.release("res2", token=str(t2))

    @pytest.mark.asyncio
    async def test_ricart_agrawala_deferral_and_grant_ordering(self):
        # Test the core RA logic directly:
        # Node A is in WANTED state with Lamport timestamp 5.
        # Incoming request from Node B with timestamp 10 should be DEFERRED.
        clock_a = LamportClock(initial=4)
        mgr_a = RicartAgrawalaManager(
            node_id="node-a",
            peer_addresses={"node-b": "127.0.0.1:9999"},
            lamport_clock=clock_a,
        )
        res = mgr_a._get_resource("res1")
        res.state = LockState.WANTED
        res.our_ts = 5

        granted, deferred, err = await mgr_a.handle_request_lock(
            requesting_node_id="node-b",
            resource_id="res1",
            request_id="req-b-1",
            election_epoch=0,
            lamport_timestamp=10,
        )
        assert deferred is True
        assert granted is False
        assert len(res.deferred) == 1
        assert res.deferred[0].requesting_node_id == "node-b"

        # Now test lower timestamp from Node C with timestamp 2.
        # Should be GRANTED immediately because (2, "node-c") < (5, "node-a").
        granted_c, deferred_c, _ = await mgr_a.handle_request_lock(
            requesting_node_id="node-c",
            resource_id="res1",
            request_id="req-c-1",
            election_epoch=0,
            lamport_timestamp=2,
        )
        assert granted_c is True
        assert deferred_c is False

    @pytest.mark.asyncio
    async def test_ricart_agrawala_tie_breaking_by_node_id(self):
        # Node B and Node C both have timestamp 5.
        # If local is Node B: (5, "node-b") < (5, "node-c") -> Node B has priority -> defers C!
        mgr_b = RicartAgrawalaManager(
            node_id="node-b",
            peer_addresses={"node-c": "127.0.0.1:9999"},
            lamport_clock=LamportClock(initial=4),
        )
        res = mgr_b._get_resource("res1")
        res.state = LockState.WANTED
        res.our_ts = 5

        granted, deferred, _ = await mgr_b.handle_request_lock(
            requesting_node_id="node-c",
            resource_id="res1",
            request_id="req-c-1",
            election_epoch=0,
            lamport_timestamp=5,
        )
        assert deferred is True
        assert granted is False

        # If local is Node C: (5, "node-c") > (5, "node-b") -> Node B has higher priority -> grants B immediately!
        mgr_c = RicartAgrawalaManager(
            node_id="node-c",
            peer_addresses={"node-b": "127.0.0.1:9999"},
            lamport_clock=LamportClock(initial=4),
        )
        res_c = mgr_c._get_resource("res1")
        res_c.state = LockState.WANTED
        res_c.our_ts = 5

        granted_b, deferred_b, _ = await mgr_c.handle_request_lock(
            requesting_node_id="node-b",
            resource_id="res1",
            request_id="req-b-1",
            election_epoch=0,
            lamport_timestamp=5,
        )
        assert granted_b is True
        assert deferred_b is False


class TestCentralizedLockBaseline:
    @pytest.mark.asyncio
    async def test_primary_grant_and_release(self):
        gate = WriteAdmissionGate(can_serve_writes=True)
        mgr = CentralizedLockManager(
            node_id="primary-1",
            role="primary",
            write_gate=gate,
            lamport_clock=LamportClock(),
        )
        t1 = await mgr.acquire("res1", client_id="client-a")
        assert t1.node_id == "primary-1"
        assert mgr.get_status("res1")["state"] == LockState.HELD.value

        # Second acquire times out while held
        with pytest.raises(TimeoutError):
            await mgr.acquire("res1", client_id="client-b", timeout_seconds=0.1)

        # Release
        await mgr.release("res1", token=str(t1), client_id="client-a")
        assert mgr.get_status("res1")["state"] == LockState.RELEASED.value

        # Now second client can acquire
        t2 = await mgr.acquire("res1", client_id="client-b")
        assert t2 > t1
        await mgr.release("res1", token=str(t2), client_id="client-b")

    @pytest.mark.asyncio
    async def test_centralized_lock_blocked_without_write_authority(self):
        gate = WriteAdmissionGate(can_serve_writes=False, reason="minority partition")
        mgr = CentralizedLockManager(
            node_id="primary-1",
            role="primary",
            write_gate=gate,
            lamport_clock=LamportClock(),
        )
        with pytest.raises(PermissionError):
            await mgr.acquire("res1")
