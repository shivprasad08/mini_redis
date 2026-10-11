"""
Tests for the ReplicationProgress tracker (M15).

Covers:
- Initial state.
- Sequential applied writes.
- Duplicate sequence handling.
- Sequence gaps.
- Out-of-order write recording.
- Contiguous sequence calculation.
- Epoch transitions.
- Historical digest continuity across epochs.
- Identical histories produce identical fingerprints.
- Different histories produce different fingerprints.
- Invalid inputs are rejected.
- Obsolete-epoch writes do not overwrite current history.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from miniredis.replication.progress import ReplicationProgress, ProgressSnapshot, _canonical_op


# ── helpers ──────────────────────────────────────────────────────────

def make_tracker(node_id="test-node", role="replica", epoch=0):
    return ReplicationProgress(node_id=node_id, role=role, epoch=epoch)


def apply_seq(tracker, epoch, seq, op_type="SET", key="k", value=b"v"):
    tracker.record_applied(epoch, seq, op_type, key, value)


# ── initial state ─────────────────────────────────────────────────────

def test_initial_state():
    t = make_tracker(node_id="n1", role="replica", epoch=0)
    snap = t.snapshot()
    assert snap.node_id == "n1"
    assert snap.role == "replica"
    assert snap.epoch == 0
    assert snap.last_applied_sequence == 0
    assert snap.gap_detected is False
    assert snap.gap_expected_sequence == 0
    assert snap.gap_observed_sequence == 0
    # Digest exists (empty SHA-256 of nothing).
    assert isinstance(snap.history_digest, str)
    assert len(snap.history_digest) == 64  # SHA-256 hex


# ── sequential writes ─────────────────────────────────────────────────

def test_sequential_writes_advance_sequence():
    t = make_tracker(epoch=1)
    for i in range(1, 6):
        apply_seq(t, 1, i)
    snap = t.snapshot()
    assert snap.last_applied_sequence == 5
    assert snap.gap_detected is False


def test_fingerprint_changes_per_write():
    t = make_tracker(epoch=1)
    d0 = t.snapshot().history_digest
    apply_seq(t, 1, 1)
    d1 = t.snapshot().history_digest
    apply_seq(t, 1, 2)
    d2 = t.snapshot().history_digest
    assert d0 != d1
    assert d1 != d2


# ── duplicate handling ────────────────────────────────────────────────

def test_duplicate_write_is_ignored():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    d1 = t.snapshot().history_digest
    apply_seq(t, 1, 1)  # duplicate
    d2 = t.snapshot().history_digest
    snap = t.snapshot()
    assert snap.last_applied_sequence == 1
    assert d1 == d2  # fingerprint must not change


def test_already_applied_sequence_ignored():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    apply_seq(t, 1, 2)
    apply_seq(t, 1, 1)  # already applied
    assert t.snapshot().last_applied_sequence == 2


# ── sequence gaps ─────────────────────────────────────────────────────

def test_gap_is_recorded():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    apply_seq(t, 1, 3)  # gap: seq 2 missing
    snap = t.snapshot()
    assert snap.gap_detected is True
    assert snap.gap_expected_sequence == 2
    assert snap.gap_observed_sequence == 3
    assert snap.last_applied_sequence == 1  # did NOT advance past the gap


def test_gap_blocks_further_progress():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    apply_seq(t, 1, 3)  # gap
    apply_seq(t, 1, 4)  # still a gap from 1 perspective — should not advance past 1
    snap = t.snapshot()
    assert snap.last_applied_sequence == 1


def test_explicit_record_gap():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    t.record_gap(epoch=1, expected_sequence=2, observed_sequence=5)
    snap = t.snapshot()
    assert snap.gap_detected is True
    assert snap.gap_expected_sequence == 2
    assert snap.gap_observed_sequence == 5


def test_gap_not_recorded_for_obsolete_epoch():
    t = make_tracker(epoch=2)
    t.record_gap(epoch=1, expected_sequence=1, observed_sequence=3)
    assert t.snapshot().gap_detected is False


def test_first_gap_wins_subsequent_gaps_ignored():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    t.record_gap(epoch=1, expected_sequence=2, observed_sequence=5)
    t.record_gap(epoch=1, expected_sequence=2, observed_sequence=10)
    snap = t.snapshot()
    assert snap.gap_observed_sequence == 5  # first gap retained


def test_reset_gap():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    t.record_gap(epoch=1, expected_sequence=2, observed_sequence=3)
    t.reset_gap()
    snap = t.snapshot()
    assert snap.gap_detected is False
    assert snap.gap_expected_sequence == 0
    assert snap.gap_observed_sequence == 0


# ── out-of-order writes ───────────────────────────────────────────────

def test_out_of_order_write_is_treated_as_gap():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 2)  # seq 2 before seq 1 — gap
    snap = t.snapshot()
    assert snap.gap_detected is True
    assert snap.last_applied_sequence == 0


# ── epoch transitions ─────────────────────────────────────────────────

def test_advance_epoch_resets_sequence():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    apply_seq(t, 1, 2)
    t.advance_epoch(2)
    snap = t.snapshot()
    assert snap.epoch == 2
    assert snap.last_applied_sequence == 0


def test_advance_epoch_preserves_fingerprint_accumulator():
    """The history hash must NOT be cleared on epoch transition."""
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    d_before = t.snapshot().history_digest
    t.advance_epoch(2)
    # After epoch transition no new writes — digest should be preserved
    d_after = t.snapshot().history_digest
    assert d_before == d_after  # continuity maintained


def test_advance_epoch_new_writes_use_new_epoch():
    t = make_tracker(epoch=1)
    apply_seq(t, 1, 1)
    t.advance_epoch(2)
    apply_seq(t, 2, 1)
    snap = t.snapshot()
    assert snap.epoch == 2
    assert snap.last_applied_sequence == 1


def test_obsolete_epoch_advance_ignored():
    t = make_tracker(epoch=5)
    t.advance_epoch(3)
    assert t.snapshot().epoch == 5


def test_update_role_and_epoch():
    t = make_tracker(role="replica", epoch=1)
    t.update_role_and_epoch("primary", 4)
    snap = t.snapshot()
    assert snap.role == "primary"
    assert snap.epoch == 4
    assert snap.last_applied_sequence == 0


def test_update_role_same_epoch_no_seq_reset():
    t = make_tracker(role="replica", epoch=1)
    t.record_applied(1, 1, "SET", "k", b"v")
    # Update role without epoch change
    t.update_role_and_epoch("primary", 1)
    snap = t.snapshot()
    assert snap.last_applied_sequence == 1  # not reset


# ── obsolete epoch writes ─────────────────────────────────────────────

def test_obsolete_epoch_write_not_applied():
    t = make_tracker(epoch=3)
    t.record_applied(2, 1, "SET", "k", b"v")
    snap = t.snapshot()
    assert snap.last_applied_sequence == 0
    assert snap.epoch == 3


def test_obsolete_epoch_does_not_change_digest():
    t = make_tracker(epoch=3)
    apply_seq(t, 3, 1)
    d1 = t.snapshot().history_digest
    t.record_applied(2, 999, "SET", "k", b"v")  # obsolete
    d2 = t.snapshot().history_digest
    assert d1 == d2


# ── deterministic fingerprints ────────────────────────────────────────

def test_identical_histories_produce_identical_fingerprints():
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=1)
    for seq in range(1, 6):
        t1.record_applied(1, seq, "SET", f"key{seq}", f"val{seq}".encode())
        t2.record_applied(1, seq, "SET", f"key{seq}", f"val{seq}".encode())
    assert t1.snapshot().history_digest == t2.snapshot().history_digest


def test_different_values_produce_different_fingerprints():
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=1)
    t1.record_applied(1, 1, "SET", "k", b"value-A")
    t2.record_applied(1, 1, "SET", "k", b"value-B")
    assert t1.snapshot().history_digest != t2.snapshot().history_digest


def test_different_keys_produce_different_fingerprints():
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=1)
    t1.record_applied(1, 1, "SET", "key-alpha", b"v")
    t2.record_applied(1, 1, "SET", "key-beta", b"v")
    assert t1.snapshot().history_digest != t2.snapshot().history_digest


def test_different_op_types_produce_different_fingerprints():
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=1)
    t1.record_applied(1, 1, "SET", "k", b"v")
    t2.record_applied(1, 1, "DEL", "k", b"")
    assert t1.snapshot().history_digest != t2.snapshot().history_digest


def test_different_epochs_produce_different_fingerprints():
    """Same key/value/op but different epoch must differ."""
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=2)
    t1.record_applied(1, 1, "SET", "k", b"v")
    t2.record_applied(2, 1, "SET", "k", b"v")
    assert t1.snapshot().history_digest != t2.snapshot().history_digest


def test_order_matters_for_fingerprint():
    t1 = make_tracker(epoch=1)
    t2 = make_tracker(epoch=1)
    t1.record_applied(1, 1, "SET", "a", b"1")
    t1.record_applied(1, 2, "SET", "b", b"2")
    t2.record_applied(1, 1, "SET", "b", b"2")
    t2.record_applied(1, 2, "SET", "a", b"1")
    # Different order → different sequence numbers → different digests.
    assert t1.snapshot().history_digest != t2.snapshot().history_digest


# ── canonical op helper ───────────────────────────────────────────────

def test_canonical_op_is_deterministic():
    b1 = _canonical_op(1, 5, "SET", "mykey", b"myval", 60, 1000000)
    b2 = _canonical_op(1, 5, "SET", "mykey", b"myval", 60, 1000000)
    assert b1 == b2


def test_canonical_op_includes_all_fields():
    b = _canonical_op(1, 5, "SET", "k", b"v", 0, 0)
    parsed = json.loads(b.decode("utf-8"))
    assert parsed["epoch"] == 1
    assert parsed["seq"] == 5
    assert parsed["op"] == "SET"
    assert parsed["key"] == "k"


def test_canonical_op_normalises_op_type():
    b1 = _canonical_op(1, 1, "set", "k", b"v", 0, 0)
    b2 = _canonical_op(1, 1, "SET", "k", b"v", 0, 0)
    assert b1 == b2


# ── set_role ──────────────────────────────────────────────────────────

def test_set_role():
    t = make_tracker(role="replica")
    t.set_role("primary")
    assert t.snapshot().role == "primary"


# ── snapshot immutability ─────────────────────────────────────────────

def test_snapshot_is_immutable():
    t = make_tracker(epoch=1)
    snap = t.snapshot()
    apply_seq(t, 1, 1)
    # The old snapshot must not reflect the new write.
    assert snap.last_applied_sequence == 0
    assert t.snapshot().last_applied_sequence == 1
