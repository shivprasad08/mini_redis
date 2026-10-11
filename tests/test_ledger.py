"""
M20 unit tests: hash-chained write log, verification, replay and fsync policies.

All ledgers live in pytest's tmp_path; no project or deployment data is touched.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import miniredis.ledger as ledger_module
from miniredis.ledger import (
    GENESIS_HASH,
    LedgerCorruptionError,
    LedgerError,
    LedgerWriteError,
    WriteLedger,
    record_hash,
    replay,
    verify_file,
)
from miniredis.replication.progress import ReplicationProgress
from miniredis.storage import Store


def _write_three(ledger: WriteLedger) -> None:
    ledger.append_op(epoch=7, seq=1, op="SET", key=b"a", value=b"1")
    ledger.append_op(epoch=7, seq=2, op="SET", key=b"b", value=b"2")
    ledger.append_op(epoch=7, seq=3, op="DEL", key=b"a")


def _lines(path: Path) -> list[bytes]:
    return path.read_bytes().split(b"\n")[:-1]


async def _open(tmp_path: Path, policy: str = "always", interval_ms=None) -> WriteLedger:
    ledger = WriteLedger(tmp_path, policy, interval_ms)
    ledger.open()
    await ledger.start()
    return ledger


async def test_first_record_links_to_genesis_and_records_chain(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    first = ledger.append_op(epoch=7, seq=1, op="SET", key=b"a", value=b"1", deadline_ms=123)
    second = ledger.append_op(epoch=7, seq=2, op="SET", key=b"b", value=b"2")
    await ledger.close()

    assert first.payload["prev"] == GENESIS_HASH
    assert second.payload["prev"] == first.hash
    assert first.hash == record_hash(first.payload)
    stored = [json.loads(line) for line in _lines(tmp_path / "ledger.jsonl")]
    assert [r["hash"] for r in stored] == [first.hash, second.hash]
    assert stored[0]["v"] == 1 and stored[0]["deadline_ms"] == 123


async def test_verify_and_reopen_continue_the_chain(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    _write_three(ledger)
    head = ledger.head_hash
    report = ledger.verify()
    assert report.ok and report.records == 3 and report.last_sequence == 3 and report.head_hash == head
    await ledger.close()

    reopened = WriteLedger(tmp_path)
    records = reopened.open()
    assert len(records) == 3 and reopened.head_hash == head
    nxt = reopened.append_op(epoch=7, seq=4, op="SET", key=b"c", value=b"3")
    assert nxt.payload["prev"] == head
    assert reopened.verify().ok
    await reopened.close()


async def test_modified_record_is_detected_and_refused(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    _write_three(ledger)
    await ledger.close()
    path = tmp_path / "ledger.jsonl"
    lines = _lines(path)
    tampered = json.loads(lines[1])
    tampered["value"] = "OTk5"  # b"999"
    path.write_bytes(lines[0] + b"\n" + json.dumps(tampered).encode() + b"\n" + lines[2] + b"\n")
    before = path.read_bytes()

    report = verify_file(path)
    assert not report.ok and report.first_invalid_line == 2 and "modified" in report.error
    with pytest.raises(LedgerCorruptionError) as excinfo:
        WriteLedger(tmp_path).open()
    assert excinfo.value.line_no == 2
    assert path.read_bytes() == before, "a corrupted ledger must be preserved, not rewritten"


async def test_reordered_or_removed_records_are_detected(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    _write_three(ledger)
    await ledger.close()
    path = tmp_path / "ledger.jsonl"
    lines = _lines(path)

    path.write_bytes(lines[1] + b"\n" + lines[0] + b"\n" + lines[2] + b"\n")
    assert "link broken" in verify_file(path).error
    path.write_bytes(lines[0] + b"\n" + lines[2] + b"\n")
    assert "link broken" in verify_file(path).error


async def test_tail_truncation_while_running_fails_verify(tmp_path: Path) -> None:
    """Dropping the newest record keeps the file self-consistent; VERIFY must still notice."""
    ledger = await _open(tmp_path)
    _write_three(ledger)
    path = tmp_path / "ledger.jsonl"
    lines = _lines(path)
    path.write_bytes(lines[0] + b"\n" + lines[1] + b"\n")
    report = ledger.verify()
    assert not report.ok and "truncated or rewritten" in report.error
    await ledger.close()


async def test_interrupted_trailing_append_is_recovered(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    _write_three(ledger)
    head = ledger.head_hash
    await ledger.close()
    path = tmp_path / "ledger.jsonl"
    path.write_bytes(path.read_bytes() + b'{"v":1,"kind":"op","epoch":7,"se')

    assert not verify_file(path).ok  # torn tail is reported, not silently valid
    recovered = WriteLedger(tmp_path)
    assert len(recovered.open()) == 3
    assert recovered.head_hash == head
    assert recovered.verify().ok
    torn = list(tmp_path.glob("ledger.jsonl.torn-*"))
    assert len(torn) == 1 and torn[0].read_bytes().startswith(b'{"v":1')
    await recovered.close()


async def test_corrupted_complete_record_is_never_truncated_away(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    _write_three(ledger)
    await ledger.close()
    path = tmp_path / "ledger.jsonl"
    lines = _lines(path)
    path.write_bytes(lines[0] + b"\n" + b"not json at all\n" + lines[2] + b"\n")
    before = path.read_bytes()
    with pytest.raises(LedgerCorruptionError):
        WriteLedger(tmp_path).open()
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("ledger.jsonl.torn-*"))


async def test_append_refuses_gaps_duplicates_and_older_epochs(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    ledger.append_op(epoch=7, seq=1, op="SET", key=b"a", value=b"1")
    for epoch, seq in ((7, 3), (7, 1), (6, 1), (9, 2)):
        with pytest.raises(LedgerWriteError):
            ledger.append_op(epoch=epoch, seq=seq, op="SET", key=b"x", value=b"y")
    ledger.append_op(epoch=9, seq=1, op="SET", key=b"b", value=b"2")  # new epoch restarts at 1
    assert ledger.verify().ok and ledger.record_count == 2
    await ledger.close()


async def test_second_process_cannot_open_the_same_directory(tmp_path: Path) -> None:
    ledger = await _open(tmp_path)
    with pytest.raises(LedgerError, match="in use"):
        WriteLedger(tmp_path).open()
    await ledger.close()
    again = WriteLedger(tmp_path)
    again.open()
    await again.close()


async def test_replay_rebuilds_state_deletions_ttls_and_digest(tmp_path: Path) -> None:
    now = [1_000.0]
    ledger = await _open(tmp_path)
    direct = ReplicationProgress("n", role="replica")
    ops = [
        (1, "SET", b"keep", b"v1", 0, 0),
        (2, "SET", b"gone", b"v2", 0, 0),
        (3, "SET", b"short", b"v3", 5, 1_005_000),     # expires at t=1005
        (4, "SET", b"long", b"v4", 100, 1_100_000),    # expires at t=1100
        (5, "DEL", b"gone", b"", 0, 0),
        (6, "EXPIRE", b"keep", b"", 50, 1_050_000),    # keep now expires at t=1050
    ]
    for seq, op, key, value, ttl, deadline in ops:
        ledger.append_op(epoch=3, seq=seq, op=op, key=key, value=value, ttl_seconds=ttl, deadline_ms=deadline)
        direct.record_applied(3, seq, op, key.decode(), value, ttl, deadline)
    await ledger.close()

    now[0] = 1_010.0  # restart after "short" expired
    reopened = WriteLedger(tmp_path)
    store = Store(clock=lambda: now[0])
    progress = ReplicationProgress("n", role="replica")
    assert replay(reopened.open(), store, progress) == (3, 6)
    await reopened.close()

    assert store.get(b"keep") == b"v1"
    assert store.get(b"gone") is None
    assert store.get(b"short") is None, "replay must not extend an expired TTL"
    assert store.get(b"long") == b"v4"
    assert store._expiries[b"keep"] == 1_050.0 and store._expiries[b"long"] == 1_100.0  # noqa: SLF001
    snap = progress.snapshot()
    assert (snap.epoch, snap.last_applied_sequence) == (3, 6)
    assert snap.history_digest == direct.snapshot().history_digest


async def test_checkpoint_adopts_the_leader_head(tmp_path: Path) -> None:
    leader = await _open(tmp_path / "leader")
    leader.append_op(epoch=4, seq=1, op="SET", key=b"a", value=b"1")
    leader.append_op(epoch=4, seq=2, op="SET", key=b"b", value=b"2")

    replica = await _open(tmp_path / "replica")
    replica.append_checkpoint(
        epoch=4, seq=2, digest="d", adopt_head=leader.head_hash,
        items=[(b"a", b"1", None), (b"b", b"2", None)], watermarks={},
    )
    assert replica.head_hash == leader.head_hash
    same_from_leader = leader.append_op(epoch=4, seq=3, op="SET", key=b"c", value=b"3")
    same_on_replica = replica.append_op(epoch=4, seq=3, op="SET", key=b"c", value=b"3")
    assert same_on_replica.hash == same_from_leader.hash
    assert replica.verify().ok
    await replica.close()
    await leader.close()

    store = Store()
    replay(WriteLedger(tmp_path / "replica").open(), store)
    assert (store.get(b"a"), store.get(b"b"), store.get(b"c")) == (b"1", b"2", b"3")


# ── fsync policies ────────────────────────────────────────────────────


@pytest.fixture
def fsync_calls(monkeypatch):
    calls: list[int] = []
    real = ledger_module.os.fsync

    def counting(fd: int) -> None:
        calls.append(fd)
        real(fd)

    monkeypatch.setattr(ledger_module.os, "fsync", counting)
    return calls


async def test_always_policy_fsyncs_before_append_returns(tmp_path: Path, fsync_calls) -> None:
    ledger = await _open(tmp_path, "always")
    before = len(fsync_calls)
    record = ledger.append_op(epoch=1, seq=1, op="SET", key=b"k", value=b"v")
    assert len(fsync_calls) == before + 1, "the record is fsynced inside the append"
    await asyncio.wait_for(ledger.wait_durable(record), 0.01)  # nothing left to wait for
    await ledger.close()


async def test_batch_policy_acks_only_after_a_group_fsync(tmp_path: Path, fsync_calls) -> None:
    ledger = await _open(tmp_path, "batch", 50)
    before = len(fsync_calls)
    records = [ledger.append_op(epoch=1, seq=i, op="SET", key=b"k", value=b"v") for i in range(1, 21)]
    assert len(fsync_calls) == before, "batch appends do not fsync inline"
    waiters = [asyncio.ensure_future(ledger.wait_durable(r)) for r in records]
    await asyncio.sleep(0.01)
    assert not any(w.done() for w in waiters), "acks must wait for the group fsync"
    await asyncio.wait_for(asyncio.gather(*waiters), 2)
    group_fsyncs = len(fsync_calls) - before
    assert 1 <= group_fsyncs < len(records), "one fsync covers the whole group"
    await ledger.close()


async def test_interval_policy_acks_immediately_and_fsyncs_later(tmp_path: Path, fsync_calls) -> None:
    ledger = await _open(tmp_path, "interval", 100)
    before = len(fsync_calls)
    record = ledger.append_op(epoch=1, seq=1, op="SET", key=b"k", value=b"v")
    await asyncio.wait_for(ledger.wait_durable(record), 0.01)
    assert len(fsync_calls) == before, "interval mode acknowledges before fsync"
    assert (tmp_path / "ledger.jsonl").read_bytes().endswith(b"\n"), "but the record is already in the OS"
    deadline = asyncio.get_running_loop().time() + 2
    while len(fsync_calls) == before and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.02)
    assert len(fsync_calls) > before, "background fsync within the interval"
    await ledger.close()


async def test_fsync_failure_fails_closed(tmp_path: Path, monkeypatch) -> None:
    ledger = await _open(tmp_path, "always")

    def broken(fd: int) -> None:
        raise OSError(5, "I/O error")

    monkeypatch.setattr(ledger_module.os, "fsync", broken)
    with pytest.raises(LedgerWriteError):
        ledger.append_op(epoch=1, seq=1, op="SET", key=b"k", value=b"v")
    monkeypatch.undo()
    with pytest.raises(LedgerWriteError, match="earlier failure"):
        ledger.append_op(epoch=1, seq=1, op="SET", key=b"k", value=b"v")
    assert ledger.failed
    await ledger.close()
