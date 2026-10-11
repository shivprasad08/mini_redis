"""
Chandy–Lamport global snapshot (M17).

Processes are the cluster nodes; channels are the directed FIFO links between
every ordered pair of nodes (``miniredis.links``). The only inter-node
messages that change the captured state — key/value contents, epoch and
replication progress — are ``ReplicateWrite`` messages, which travel on those
links; markers travel on the same links, so FIFO holds per channel.

Algorithm (per node, per snapshot id):

* **Initiator** records its local state, then sends a marker on every
  outgoing channel, and starts recording every incoming channel.
* **First marker** received on channel C: record local state, the state of C
  is empty, send markers on every outgoing channel, record every other
  incoming channel.
* **Later marker** on channel C: stop recording C; its state is the messages
  received on C between the local recording and that marker.
* A node's snapshot is complete when markers arrived on all incoming channels.

Making "record state + send markers" atomic
───────────────────────────────────────────
The local state is captured and the markers are *enqueued* on the links in one
synchronous step while holding the node's commit lock — the same lock the
primary holds while it allocates a sequence, sends a write and applies it.
So no write can be sent before the marker yet be missing from the recorded
state, or vice versa. A marker handler does not return (and therefore, with
stop-and-wait links, its sender sends nothing further) until the node has
recorded, so a channel closed by a marker that raced the recording is truly
empty.

The coordinator (the node that ran ``SNAPSHOT``) collects every node's report
with ``CollectSnapshot`` and declares the snapshot ``COMPLETED`` only if every
participant completed, every one of the n·(n-1) channels received its marker,
and all nodes recorded the same leadership epoch with no epoch change while
the snapshot ran (election traffic is not part of the recorded channels, so a
leadership change makes the cut unverifiable and the snapshot ``FAILED``).
Snapshot installs on recovering replicas are deferred while a snapshot is
recording for the same reason.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import grpc

from miniredis.logical_clock import LamportClock
from miniredis.peer_channel import open_peer_channel
from miniredis.replication.proto import replication_pb2, replication_pb2_grpc

logger = logging.getLogger(__name__)

SNAPSHOT_ID_RE = re.compile(r"^snap-[A-Za-z0-9_.:\-]{1,64}-[0-9a-f]{12}$")

STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
NODE_COMPLETE = "COMPLETE"
NODE_INCOMPLETE = "INCOMPLETE"
NODE_UNKNOWN = "UNKNOWN"
NODE_EPOCH_CHANGED = "EPOCH_CHANGED"
NODE_UNREACHABLE = "UNREACHABLE"


def valid_snapshot_id(snapshot_id: str) -> bool:
    return bool(SNAPSHOT_ID_RE.match(snapshot_id or ""))


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Channel:
    """Incoming channel sender -> this node."""

    recording: bool = False
    marker_received: bool = False
    messages: list[dict[str, Any]] = field(default_factory=list)
    duplicate_markers: int = 0


@dataclass
class _LocalSnapshot:
    snapshot_id: str
    initiator: str
    created_monotonic: float
    channels: dict[str, _Channel]
    recorded: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    state: Optional[dict[str, Any]] = None
    recorded_epoch: Optional[int] = None
    recorded_at_ms: int = 0
    errors: list[str] = field(default_factory=list)

    def check_done(self) -> None:
        if self.state is not None and all(ch.marker_received for ch in self.channels.values()):
            self.done.set()


class SnapshotManager:
    """Per-node Chandy–Lamport state machine and coordinator."""

    def __init__(
        self,
        node_id: str,
        peers: dict[str, Any],  # peer_id -> PeerEndpoint (grpc_address)
        links: Any,  # LinkManager
        capture_state: Callable[[], dict[str, Any]],
        epoch_provider: Callable[[], int],
        commit_lock: Optional[asyncio.Lock] = None,
        lamport_clock: Optional[LamportClock] = None,
        default_timeout_ms: int = 5000,
        max_active: int = 4,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
    ) -> None:
        self.node_id = node_id
        self._peers = {pid: peer for pid, peer in peers.items() if pid != node_id}
        self._links = links
        self._capture_state = capture_state
        self._epoch_provider = epoch_provider
        self._commit_lock = commit_lock
        self._lamport = lamport_clock or LamportClock()
        self.default_timeout_ms = default_timeout_ms
        self._max_active = max_active
        self._rpc_filter = rpc_filter
        self._snapshots: dict[str, _LocalSnapshot] = {}
        self._tombstones: OrderedDict[str, float] = OrderedDict()
        self._results: OrderedDict[str, dict[str, Any]] = OrderedDict()

    # ── queries ──────────────────────────────────────────────────────

    def recording_active(self) -> bool:
        """True while any snapshot has recorded state but not yet closed all channels."""
        return any(s.state is not None and not s.done.is_set() for s in self._snapshots.values())

    def get_result(self, snapshot_id: str) -> Optional[dict[str, Any]]:
        return self._results.get(snapshot_id)

    def local_snapshot(self, snapshot_id: str) -> Optional[_LocalSnapshot]:
        return self._snapshots.get(snapshot_id)

    # ── message hooks ────────────────────────────────────────────────

    def on_channel_message(self, sender_id: str, message: dict[str, Any]) -> None:
        """Record an application message delivered on channel sender -> this node."""
        for snap in self._snapshots.values():
            channel = snap.channels.get(sender_id)
            if channel is not None and channel.recording:
                channel.messages.append(message)

    async def handle_marker(self, snapshot_id: str, initiator_id: str, sender_id: str) -> str:
        if not valid_snapshot_id(snapshot_id):
            return "invalid_id"
        if sender_id not in self._peers:
            return "unknown_sender"
        if snapshot_id in self._tombstones:
            return "stale"
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            self._gc()
            if self._active_count() >= self._max_active:
                logger.warning("Rejecting marker for %s: too many concurrent snapshots", snapshot_id)
                return "busy"
            snap = self._new_snapshot(snapshot_id, initiator_id)
            # The first marker's channel is closed with an empty state.
            snap.channels[sender_id].marker_received = True
            await self._record(snap)
            return "recorded"

        await snap.recorded.wait()
        channel = snap.channels[sender_id]
        if channel.marker_received:
            channel.duplicate_markers += 1
            return "duplicate"
        channel.marker_received = True
        channel.recording = False
        snap.check_done()
        return "closed"

    # ── recording ────────────────────────────────────────────────────

    def _new_snapshot(self, snapshot_id: str, initiator: str) -> _LocalSnapshot:
        snap = _LocalSnapshot(
            snapshot_id=snapshot_id,
            initiator=initiator,
            created_monotonic=time.monotonic(),
            channels={peer_id: _Channel() for peer_id in self._peers},
        )
        self._snapshots[snapshot_id] = snap
        return snap

    async def _record(self, snap: _LocalSnapshot) -> None:
        """Record local state and enqueue markers atomically w.r.t. commits."""
        if self._commit_lock is not None:
            async with self._commit_lock:
                self._record_now(snap)
        else:
            self._record_now(snap)

    def _record_now(self, snap: _LocalSnapshot) -> None:
        # Synchronous: no message can be applied between capture, opening the
        # channel recorders and enqueueing the markers.
        snap.state = self._capture_state()
        snap.recorded_epoch = self._epoch_provider()
        snap.recorded_at_ms = _now_ms()
        for channel in snap.channels.values():
            if not channel.marker_received:
                channel.recording = True
        for peer_id in self._peers:
            link = self._links.get(peer_id) if self._links is not None else None
            if link is None:
                snap.errors.append(f"no link to {peer_id}")
                continue
            future = link.submit(
                "SnapshotMarker",
                replication_pb2.SnapshotMarkerRequest(
                    snapshot_id=snap.snapshot_id, initiator_id=snap.initiator
                ),
                timeout_seconds=max(1.0, self.default_timeout_ms / 1000.0),
            )
            future.add_done_callback(lambda f, p=peer_id, s=snap: self._marker_sent(s, p, f))
        snap.recorded.set()
        snap.check_done()
        logger.info(
            "Snapshot %s: %s recorded local state (epoch=%s), markers queued to %s",
            snap.snapshot_id, self.node_id, snap.recorded_epoch, sorted(self._peers),
        )

    def _marker_sent(self, snap: _LocalSnapshot, peer_id: str, future: "asyncio.Future[Any]") -> None:
        if future.cancelled():
            snap.errors.append(f"marker to {peer_id} cancelled")
            return
        exc = future.exception()
        if exc is not None:
            snap.errors.append(f"marker to {peer_id} undeliverable: {_describe(exc)}")
            return
        status = future.result().status
        if status not in ("recorded", "closed", "duplicate"):
            snap.errors.append(f"marker to {peer_id} rejected: {status}")

    # ── per-node report ──────────────────────────────────────────────

    async def collect_local(self, snapshot_id: str, wait_ms: int) -> dict[str, Any]:
        """Wait (bounded) for this node's part of *snapshot_id* and report it.

        The collection request can overtake the marker (it travels on a separate
        channel), so an unknown id is waited for rather than reported at once.
        """
        deadline = time.monotonic() + max(0, wait_ms) / 1000.0
        snap = self._snapshots.get(snapshot_id)
        while snap is None and time.monotonic() < deadline and snapshot_id not in self._tombstones:
            await asyncio.sleep(0.01)
            snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"node_id": self.node_id, "status": NODE_UNKNOWN,
                    "errors": ["no marker received for this snapshot"]}
        try:
            await asyncio.wait_for(snap.done.wait(), timeout=max(0.0, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            pass
        return self._report(snap)

    def _report(self, snap: _LocalSnapshot) -> dict[str, Any]:
        if not snap.done.is_set():
            status = NODE_INCOMPLETE
        elif snap.recorded_epoch != self._epoch_provider():
            status = NODE_EPOCH_CHANGED
        else:
            status = NODE_COMPLETE
        return {
            "node_id": self.node_id,
            "status": status,
            "recorded_epoch": snap.recorded_epoch,
            "recorded_at_ms": snap.recorded_at_ms,
            "state": snap.state,
            "channels": {
                sender: {
                    "marker_received": ch.marker_received,
                    "messages": list(ch.messages),
                    "duplicate_markers": ch.duplicate_markers,
                }
                for sender, ch in snap.channels.items()
            },
            "errors": list(snap.errors),
        }

    def release(self, snapshot_id: str) -> bool:
        """Drop coordination state; late markers for this id are ignored afterwards."""
        self._tombstones[snapshot_id] = time.monotonic()
        while len(self._tombstones) > 1024:
            self._tombstones.popitem(last=False)
        return self._snapshots.pop(snapshot_id, None) is not None

    def _active_count(self) -> int:
        return sum(1 for s in self._snapshots.values() if not s.done.is_set())

    def _gc(self) -> None:
        horizon = time.monotonic() - max(60.0, 4 * self.default_timeout_ms / 1000.0)
        for snapshot_id in [sid for sid, s in self._snapshots.items() if s.created_monotonic < horizon]:
            self.release(snapshot_id)

    # ── coordinator ──────────────────────────────────────────────────

    async def initiate(self, timeout_ms: Optional[int] = None) -> dict[str, Any]:
        timeout_ms = timeout_ms or self.default_timeout_ms
        self._gc()
        if self._active_count() >= self._max_active:
            return self._store_result({
                "snapshot_id": None, "status": STATUS_FAILED,
                "reason": "too many concurrent snapshots on this node",
            })
        snapshot_id = f"snap-{self.node_id}-{uuid.uuid4().hex[:12]}"
        started_ms = _now_ms()
        deadline = time.monotonic() + timeout_ms / 1000.0
        snap = self._new_snapshot(snapshot_id, self.node_id)
        await self._record(snap)

        reports: dict[str, dict[str, Any]] = {}
        try:
            # Fail fast if a marker cannot be delivered; otherwise wait for every node.
            async def own_report() -> dict[str, Any]:
                while not snap.done.is_set():
                    if any("undeliverable" in e or "rejected" in e for e in snap.errors):
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        await asyncio.wait_for(snap.done.wait(), timeout=min(0.05, remaining))
                    except asyncio.TimeoutError:
                        pass
                return self._report(snap)

            remote = {
                peer_id: asyncio.create_task(self._collect_remote(peer_id, snapshot_id, deadline))
                for peer_id in self._peers
            }
            local = await own_report()
            reports[self.node_id] = local
            if local["status"] != NODE_COMPLETE:
                # Our own channels cannot all close (e.g. an undeliverable marker):
                # nobody else can complete either, so stop waiting.
                for task in remote.values():
                    task.cancel()
            for peer_id, task in remote.items():
                try:
                    reports[peer_id] = await task
                except asyncio.CancelledError:
                    reports[peer_id] = {"node_id": peer_id, "status": NODE_INCOMPLETE,
                                        "errors": ["collection aborted: snapshot already failed"]}
        finally:
            await self._release_everywhere(snapshot_id)

        result = self._assemble(snapshot_id, started_ms, reports)
        level = logging.INFO if result["status"] == STATUS_COMPLETED else logging.WARNING
        logger.log(level, "Snapshot %s %s %s", snapshot_id, result["status"], result.get("reason") or "")
        return self._store_result(result)

    async def _collect_remote(self, peer_id: str, snapshot_id: str, deadline: float) -> dict[str, Any]:
        peer = self._peers[peer_id]
        wait_ms = max(0, int((deadline - time.monotonic()) * 1000))
        if self._rpc_filter is not None and not self._rpc_filter("CollectSnapshot", peer.grpc_address):
            return {"node_id": peer_id, "status": NODE_UNREACHABLE, "errors": ["partitioned"]}
        channel = open_peer_channel(peer.grpc_address)
        try:
            stub = replication_pb2_grpc.ReplicationServiceStub(channel)
            ts = await self._lamport.tick()
            response = await asyncio.wait_for(
                stub.CollectSnapshot(replication_pb2.CollectSnapshotRequest(
                    snapshot_id=snapshot_id, wait_ms=wait_ms,
                    requester_id=self.node_id, lamport_timestamp=ts,
                )),
                timeout=wait_ms / 1000.0 + 0.5,  # never far beyond the snapshot deadline
            )
            await self._lamport.receive(response.lamport_timestamp)
            report = json.loads(response.report_json)
            report["node_id"] = peer_id
            return report
        except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError, ValueError) as exc:
            return {"node_id": peer_id, "status": NODE_UNREACHABLE, "errors": [_describe(exc)]}
        finally:
            await channel.close()

    async def _release_everywhere(self, snapshot_id: str) -> None:
        self.release(snapshot_id)

        async def release_remote(peer_id: str) -> None:
            peer = self._peers[peer_id]
            if self._rpc_filter is not None and not self._rpc_filter("ReleaseSnapshot", peer.grpc_address):
                return
            channel = open_peer_channel(peer.grpc_address)
            try:
                stub = replication_pb2_grpc.ReplicationServiceStub(channel)
                await asyncio.wait_for(
                    stub.ReleaseSnapshot(replication_pb2.ReleaseSnapshotRequest(
                        snapshot_id=snapshot_id, requester_id=self.node_id)),
                    timeout=1.0,
                )
            except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError):
                pass  # peers also garbage-collect stale snapshot state on their own
            finally:
                await channel.close()

        await asyncio.gather(*(release_remote(p) for p in self._peers))

    def _assemble(self, snapshot_id: str, started_ms: int, reports: dict[str, dict[str, Any]]) -> dict[str, Any]:
        participants = sorted([self.node_id, *self._peers])
        problems: list[str] = []
        nodes: dict[str, Any] = {}
        channels: list[dict[str, Any]] = []
        epochs = set()
        for node_id in participants:
            report = reports.get(node_id, {"status": NODE_UNREACHABLE, "errors": ["no report"]})
            status = report.get("status")
            if status != NODE_COMPLETE:
                problems.append(f"{node_id}: {status}" + (f" ({'; '.join(report.get('errors') or [])})"
                                                          if report.get("errors") else ""))
            state = report.get("state") or {}
            nodes[node_id] = {"status": status, "recorded_at_ms": report.get("recorded_at_ms"), **state}
            if report.get("recorded_epoch") is not None:
                epochs.add(report["recorded_epoch"])
            for sender in participants:
                if sender == node_id:
                    continue
                ch = (report.get("channels") or {}).get(sender)
                if ch is None or not ch.get("marker_received"):
                    problems.append(f"channel {sender}->{node_id}: marker not received")
                channels.append({
                    "from": sender,
                    "to": node_id,
                    "marker_received": bool(ch and ch.get("marker_received")),
                    "in_flight": len(ch["messages"]) if ch else 0,
                    "messages": ch["messages"] if ch else [],
                    "duplicate_markers": ch.get("duplicate_markers", 0) if ch else 0,
                })
        if len(epochs) > 1:
            problems.append(f"nodes recorded different leadership epochs {sorted(epochs)}")
        completed_ms = _now_ms()
        return {
            "snapshot_id": snapshot_id,
            "status": STATUS_FAILED if problems else STATUS_COMPLETED,
            "reason": "; ".join(problems) if problems else None,
            "initiator": self.node_id,
            "algorithm": "chandy-lamport",
            "started_at_ms": started_ms,
            "completed_at_ms": completed_ms,
            "duration_ms": completed_ms - started_ms,
            "epoch": next(iter(epochs)) if len(epochs) == 1 else None,
            "participants": participants,
            "nodes": nodes,
            "channels": channels,
            "summary": {
                "channels": len(channels),
                "channels_closed": sum(1 for c in channels if c["marker_received"]),
                "in_flight_messages": sum(c["in_flight"] for c in channels),
            },
        }

    def _store_result(self, result: dict[str, Any]) -> dict[str, Any]:
        if result.get("snapshot_id"):
            self._results[result["snapshot_id"]] = result
            while len(self._results) > 16:
                self._results.popitem(last=False)
        return result


def _describe(exc: BaseException) -> str:
    if isinstance(exc, grpc.aio.AioRpcError):
        return f"{exc.code().name}: {exc.details()}"
    return repr(exc)
