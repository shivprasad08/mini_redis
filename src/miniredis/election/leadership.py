"""
Leadership confirmation, safe startup and replica rejoin for mini-redis.

Why this exists
───────────────
Every node keeps its keyspace, epoch and replication progress in memory
only. When a process restarts it has *no* memory of the cluster, so the
``--role`` it was launched with cannot be trusted: after a failover, the
node configured as the initial primary may come back while another node
leads in a newer epoch. Starting it as primary would create two primaries
(split brain) and its empty history would compete with the real data.

This module makes ``--role`` a *bootstrap hint* and adds the missing
"leadership confirmation" step:

* ``survey_cluster`` asks every peer for its replication progress
  (the existing ``GetProgress`` RPC: role, epoch, sequence, digest and the
  leader it follows).
* ``LeadershipReconciler.establish_initial_role`` runs at startup:
    - If any reachable peer reports an established epoch (>= 1), the node
      follows the confirmed leader (a peer that itself reports
      ``role=primary`` in the highest epoch) and pulls a leader snapshot.
      If no confirmed leader is reachable, it stays a write-blocked replica.
    - Only if *no* reachable peer reports leadership **and** a majority is
      reachable may the node configured as primary bootstrap the cluster.
      An isolated node never promotes itself.
* ``LeadershipReconciler.run`` keeps checking afterwards: a primary that
  sees a newer epoch steps down; a replica follows the confirmed leader and
  re-synchronizes after a sequence gap, divergent history or stalled lag;
  an established cluster with no confirmed leader triggers an election
  through the configured algorithm (bully or ring), which enforces majority.

Source of truth
───────────────
The election object (``BullyElection`` / ``RingElection``) remains the
only owner of role, epoch and leader identity. This module only reads peer
progress and drives the election's own transitions (``accept_coordinator``,
``start_election``, ``bootstrap_as_primary``, ``step_down``).

Epochs
──────
Epoch ``0`` means "this process has not learned any leadership yet".
Bootstraps and elections both produce ``max(previous + 1, time.time_ns())``
so epochs are unique and monotonic. Epochs are deliberately *not*
persisted: the data they describe is in memory, and a persisted epoch on
an empty node would make it look up to date. A restarted node must learn
the current epoch (and the state that goes with it) from the cluster.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

import grpc

from miniredis.cluster import majority_count
from miniredis.peer_channel import open_peer_channel
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.replication.proto import replication_pb2, replication_pb2_grpc

logger = logging.getLogger(__name__)

RpcFilter = Callable[[str, str], bool]


class StateSyncer(Protocol):
    """Pulls a snapshot from *grpc_address* and adopts it locally."""

    def __call__(self, grpc_address: str, *, require_primary: bool = True) -> Awaitable[bool]: ...


def next_epoch(current_epoch: int) -> int:
    """Return a fresh leadership epoch, unique and newer than *current_epoch*."""
    return max(current_epoch + 1, time.time_ns())


# ──────────────────────────────────────────────────────────────────────
# Cluster survey
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ClusterView:
    """Progress reported by the peers that answered one survey round."""

    responses: Mapping[str, replication_pb2.ProgressResponse] = field(default_factory=dict)
    unreachable: tuple[str, ...] = ()

    @property
    def max_epoch(self) -> int:
        return max((int(r.epoch) for r in self.responses.values()), default=0)

    def confirmed_leader(self, min_epoch: int = 0) -> Optional[str]:
        """Peer that *itself* reports role=primary in the newest epoch seen.

        A claim relayed by a replica is not enough: the old leader may be
        dead or restarted. Requiring ``epoch == max_epoch`` means a deposed
        primary that has not stepped down yet is never chosen over the
        leader of a newer epoch.
        """
        newest = self.max_epoch
        if newest < max(min_epoch, 1):
            return None
        primaries = [
            peer_id
            for peer_id, response in self.responses.items()
            if response.role == "primary" and int(response.epoch) == newest
        ]
        return sorted(primaries)[-1] if primaries else None

    def claimed_leaders(self) -> set[str]:
        """Leaders that reachable peers say they follow in the newest epoch."""
        newest = self.max_epoch
        return {
            response.primary_node_id
            for response in self.responses.values()
            if response.primary_node_id and int(response.epoch) == newest
        }


async def probe_peer_progress(
    peer_id: str,
    grpc_address: str,
    requester_id: str,
    lamport_clock: LamportClock,
    timeout_seconds: float,
    rpc_filter: Optional[RpcFilter] = None,
) -> Optional[replication_pb2.ProgressResponse]:
    """Query one peer's ``GetProgress``; ``None`` means unreachable."""
    if rpc_filter is not None and not rpc_filter("GetProgress", grpc_address):
        return None

    channel = open_peer_channel(grpc_address)
    try:
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)
        request_ts = await lamport_clock.tick()
        log_lamport_event(
            timestamp=request_ts,
            node_id=requester_id,
            event_type="progress_request_send",
            message_id=peer_id,
            description="leadership survey",
        )
        response = await asyncio.wait_for(
            stub.GetProgress(
                replication_pb2.ProgressRequest(
                    requester_node_id=requester_id,
                    lamport_timestamp=request_ts,
                )
            ),
            timeout=timeout_seconds,
        )
        await lamport_clock.receive(response.lamport_timestamp)
        return response
    except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError, OSError) as exc:
        detail = exc.details() if isinstance(exc, grpc.aio.AioRpcError) else repr(exc)
        logger.debug("GetProgress to %s (%s) failed: %s", peer_id, grpc_address, detail)
        return None
    finally:
        await channel.close()


async def survey_cluster(
    peers: Mapping[str, Any],
    self_node_id: str,
    lamport_clock: LamportClock,
    timeout_seconds: float,
    rpc_filter: Optional[RpcFilter] = None,
) -> ClusterView:
    """Probe every peer concurrently and collect what they report."""
    peer_items = [(peer_id, peer) for peer_id, peer in peers.items() if peer_id != self_node_id]
    results = await asyncio.gather(
        *(
            probe_peer_progress(
                peer_id,
                peer.grpc_address,
                self_node_id,
                lamport_clock,
                timeout_seconds,
                rpc_filter,
            )
            for peer_id, peer in peer_items
        )
    )
    responses: dict[str, replication_pb2.ProgressResponse] = {}
    unreachable: list[str] = []
    for (peer_id, _peer), response in zip(peer_items, results):
        if response is None:
            unreachable.append(peer_id)
        else:
            responses[peer_id] = response
    return ClusterView(responses=responses, unreachable=tuple(unreachable))


# ──────────────────────────────────────────────────────────────────────
# Shared election behaviour
# ──────────────────────────────────────────────────────────────────────


class LeadershipMixin:
    """Leadership-safety transitions shared by Bully and Ring elections.

    Relies on the attributes both election classes already define
    (``_node_id``, ``_peers``, ``_role``, ``_election_epoch``, ``_dispatcher``,
    ``_write_gate``, ``_progress``, ``_lock_manager``, ``_lamport_clock``,
    ``_rpc_filter``, ``_rpc_timeout_seconds``, ``_current_primary_*``).
    """

    _state_syncer: Optional[StateSyncer] = None
    _leadership_listener: Optional[Callable[[], None]] = None
    _links: Optional[Any] = None  # LinkManager: FIFO links used by replicators (M17)
    _step_down_task: Optional["asyncio.Task[None]"] = None

    def attach_state_syncer(self, state_syncer: Optional[StateSyncer]) -> None:
        """Attach the coroutine used to pull a snapshot from another node."""
        self._state_syncer = state_syncer

    def attach_links(self, links: Optional[Any]) -> None:
        """Attach the node's FIFO links; replicators created on promotion use them."""
        self._links = links

    def attach_leadership_listener(self, listener: Optional[Callable[[], None]]) -> None:
        """Attach a callback fired when a leadership conflict is observed."""
        self._leadership_listener = listener

    @property
    def time_synchronizer(self) -> Optional[Any]:
        return self._time_synchronizer  # type: ignore[attr-defined]

    def observe_higher_epoch(self, epoch: int) -> None:
        """React to proof that leadership epoch *epoch* exists.

        Called synchronously from replication paths (a replica's rejection
        Ack, or replicated writes arriving at this node). A primary fences
        itself *immediately* — before any await — so it cannot admit another
        client write, then steps down asynchronously.
        """
        if epoch <= self._election_epoch:  # type: ignore[attr-defined]
            return
        if self._role == "primary":  # type: ignore[attr-defined]
            reason = (
                f"stale leader: epoch {epoch} supersedes local epoch {self._election_epoch}"  # type: ignore[attr-defined]
            )
            logger.warning("Node %s fencing itself: %s", self._node_id, reason)  # type: ignore[attr-defined]
            self._dispatcher.set_writes_enabled(False)  # type: ignore[attr-defined]
            self._write_gate.block_writes(reason)  # type: ignore[attr-defined]
            if self._step_down_task is None or self._step_down_task.done():
                self._step_down_task = asyncio.get_running_loop().create_task(self.step_down(reason))
        if self._leadership_listener is not None:
            self._leadership_listener()

    async def step_down(self, reason: str) -> None:
        """Stop acting as primary without claiming any new leader."""
        if self._role != "primary":  # type: ignore[attr-defined]
            return
        logger.warning("Node %s stepping down as primary: %s", self._node_id, reason)  # type: ignore[attr-defined]
        self._role = "replica"
        self._current_primary_id = ""
        self._current_primary_address = ""
        self._dispatcher.set_writes_enabled(False)  # type: ignore[attr-defined]
        self._write_gate.block_writes(reason)  # type: ignore[attr-defined]
        replicator = self._dispatcher.replicator  # type: ignore[attr-defined]
        self._dispatcher.set_replicator(None)  # type: ignore[attr-defined]
        if replicator is not None:
            await replicator.aclose()
        if self._progress is not None:  # type: ignore[attr-defined]
            self._progress.set_role("replica")  # type: ignore[attr-defined]
        if self._lock_manager is not None and hasattr(self._lock_manager, "set_role"):  # type: ignore[attr-defined]
            self._lock_manager.set_role("replica")  # type: ignore[attr-defined]

    def _can_bootstrap(self) -> bool:
        return (
            self._election_epoch == 0  # type: ignore[attr-defined]
            and not self._current_primary_id  # type: ignore[attr-defined]
            and self._role != "primary"  # type: ignore[attr-defined]
        )

    async def survey_peers(self) -> ClusterView:
        return await survey_cluster(
            self._peers,  # type: ignore[attr-defined]
            self._node_id,  # type: ignore[attr-defined]
            self._lamport_clock,  # type: ignore[attr-defined]
            self._rpc_timeout_seconds,  # type: ignore[attr-defined]
            self._rpc_filter,  # type: ignore[attr-defined]
        )

    async def confirm_leadership(self) -> bool:
        """Check that no reachable peer has moved to a newer epoch.

        Used before a primary resumes writes after a partition heals; if a
        newer epoch exists the node steps down instead.
        """
        view = await self.survey_peers()
        if view.max_epoch > self._election_epoch:  # type: ignore[attr-defined]
            await self.step_down(
                f"peer reports newer epoch {view.max_epoch} after majority restored"
            )
            if self._leadership_listener is not None:
                self._leadership_listener()
            return False
        return True

    async def _catch_up_before_leading(self) -> bool:
        """Adopt the most advanced reachable peer's state before leading.

        Bully/ring priority is by node id, not by data. A node that just
        restarted (empty, epoch 0) may still win; this makes sure its empty
        history never replaces the cluster's newer data.
        """
        if self._state_syncer is None or self._progress is None:  # type: ignore[attr-defined]
            return True
        view = await self.survey_peers()
        local = self._progress.snapshot()  # type: ignore[attr-defined]
        local_position = (local.epoch, local.last_applied_sequence)
        best_id: Optional[str] = None
        best_position = local_position
        for peer_id, response in sorted(view.responses.items()):
            position = (int(response.epoch), int(response.last_applied_sequence))
            if position > best_position:
                best_id, best_position = peer_id, position
        if best_id is None:
            return True

        peer = self._peers[best_id]  # type: ignore[attr-defined]
        logger.warning(
            "Node %s is behind %s (local %s, peer %s); catching up before leading",
            self._node_id,  # type: ignore[attr-defined]
            best_id,
            local_position,
            best_position,
        )
        return await self._state_syncer(peer.grpc_address, require_primary=False)


# ──────────────────────────────────────────────────────────────────────
# Reconciler
# ──────────────────────────────────────────────────────────────────────


class LeadershipReconciler:
    """Establishes and continuously re-confirms a node's place in the cluster."""

    def __init__(
        self,
        election: Any,
        progress: Any,
        peers: Mapping[str, Any],
        node_id: str,
        lamport_clock: LamportClock,
        state_syncer: StateSyncer,
        total_cluster_size: int,
        interval_seconds: float = 1.0,
        probe_timeout_seconds: float = 0.8,
        leaderless_threshold: int = 5,
        stalled_threshold: int = 3,
        rpc_filter: Optional[RpcFilter] = None,
        on_follow: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self._election = election
        self._progress = progress
        self._peers = {peer_id: peer for peer_id, peer in peers.items() if peer_id != node_id}
        self._node_id = node_id
        self._lamport_clock = lamport_clock
        self._state_syncer = state_syncer
        self._total_cluster_size = total_cluster_size
        self._interval_seconds = interval_seconds
        self._probe_timeout_seconds = probe_timeout_seconds
        self._leaderless_threshold = leaderless_threshold
        self._stalled_threshold = stalled_threshold
        self._rpc_filter = rpc_filter
        self._on_follow = on_follow

        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._election_task: Optional[asyncio.Task[None]] = None
        self._leaderless_checks = 0
        self._last_position: Optional[tuple[int, int]] = None
        self._stalled_checks = 0

    # ── lifecycle ────────────────────────────────────────────────────

    def wake(self) -> None:
        """Run the next reconciliation round now instead of after the interval."""
        self._wake_event.set()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        for task in (self._task, self._election_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._task = None
        self._election_task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Leadership reconciliation failed on %s", self._node_id)
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self._interval_seconds)
            except asyncio.TimeoutError:
                pass
            self._wake_event.clear()

    # ── helpers ──────────────────────────────────────────────────────

    async def survey(self) -> ClusterView:
        return await survey_cluster(
            self._peers,
            self._node_id,
            self._lamport_clock,
            self._probe_timeout_seconds,
            self._rpc_filter,
        )

    def _has_majority(self, view: ClusterView) -> bool:
        return 1 + len(view.responses) >= majority_count(self._total_cluster_size)

    def _reset_counters(self) -> None:
        self._leaderless_checks = 0
        self._last_position = None
        self._stalled_checks = 0

    async def _follow(self, leader_id: str, epoch: int) -> bool:
        """Follow *leader_id* through the election and pull its snapshot."""
        peer = self._peers.get(leader_id)
        if peer is None:
            logger.warning("Node %s cannot follow unknown leader %s", self._node_id, leader_id)
            return False

        logger.warning(
            "Node %s following confirmed leader %s (epoch %s)", self._node_id, leader_id, epoch
        )
        await self._election.accept_coordinator(
            new_primary_id=leader_id,
            new_primary_address=peer.tcp_address,
            election_epoch=epoch,
        )
        if self._election.role == "primary" or self._election.current_primary_id != leader_id:
            # The election rejected the announcement (we already know a newer epoch).
            return False
        if self._on_follow is not None:
            await self._on_follow(leader_id, peer)
        self._reset_counters()
        synced = await self._state_syncer(peer.grpc_address)
        if not synced:
            logger.warning(
                "Node %s follows %s but could not sync yet; will retry", self._node_id, leader_id
            )
        return synced

    # ── startup ──────────────────────────────────────────────────────

    async def establish_initial_role(
        self,
        configured_role: str,
        poll_interval_seconds: float = 0.5,
    ) -> str:
        """Decide whether this process bootstraps, follows, or waits.

        Returns ``"primary"`` only after a successful bootstrap of a brand
        new cluster; otherwise ``"replica"`` (writes stay blocked unless and
        until a valid election promotes this node).
        """
        attempt = 0
        while not self._stop_event.is_set():
            attempt += 1

            # A coordinator announcement may have arrived while we were probing.
            if self._election.role == "primary":
                return "primary"
            leader_id = self._election.current_primary_id
            if leader_id and leader_id != self._node_id:
                peer = self._peers.get(leader_id)
                if peer is not None:
                    if self._on_follow is not None:
                        await self._on_follow(leader_id, peer)
                    await self._state_syncer(peer.grpc_address)
                return "replica"

            view = await self.survey()
            if view.max_epoch >= 1:
                leader = view.confirmed_leader(min_epoch=self._election.election_epoch)
                if leader is not None and leader != self._node_id:
                    await self._follow(leader, int(view.responses[leader].epoch))
                else:
                    logger.warning(
                        "Node %s joined an established cluster (epoch %s) with no confirmed "
                        "primary reachable; staying a write-blocked replica",
                        self._node_id,
                        view.max_epoch,
                    )
                return "replica"

            if configured_role != "primary":
                # Fresh cluster: follow the configured primary (existing behaviour).
                return "replica"

            if not self._has_majority(view):
                if attempt == 1 or attempt % 10 == 0:
                    logger.warning(
                        "Node %s waiting for a majority before bootstrapping "
                        "(reachable peers: %s, unreachable: %s)",
                        self._node_id,
                        sorted(view.responses),
                        list(view.unreachable),
                    )
                await asyncio.sleep(poll_interval_seconds)
                continue

            if await self._election.bootstrap_as_primary():
                logger.info(
                    "Node %s bootstrapped a new cluster as primary (epoch %s)",
                    self._node_id,
                    self._election.election_epoch,
                )
                return "primary"
            await asyncio.sleep(poll_interval_seconds)

        return self._election.role

    # ── steady state ─────────────────────────────────────────────────

    async def reconcile_once(self) -> None:
        """One leadership-confirmation round (see module docstring)."""
        election = self._election
        if election.is_electing:
            return

        view = await self.survey()
        my_epoch = election.election_epoch

        if election.role == "primary":
            self._reset_counters()
            if view.max_epoch > my_epoch:
                await election.step_down(
                    f"peer reports newer epoch {view.max_epoch} (local epoch {my_epoch})"
                )
                leader = view.confirmed_leader(min_epoch=view.max_epoch)
                if leader is not None and leader != self._node_id:
                    await self._follow(leader, int(view.responses[leader].epoch))
            return

        leader = view.confirmed_leader(min_epoch=my_epoch)
        if leader is None or leader == self._node_id:
            self._maybe_start_election(view)
            return
        self._leaderless_checks = 0

        leader_response = view.responses[leader]
        if leader != election.current_primary_id or int(leader_response.epoch) > my_epoch:
            await self._follow(leader, int(leader_response.epoch))
            return

        await self._repair_replica_state(leader, leader_response)

    def _maybe_start_election(self, view: ClusterView) -> None:
        established = view.max_epoch >= 1 or self._election.election_epoch >= 1
        if not established or not self._has_majority(view):
            self._leaderless_checks = 0
            return

        claimed = view.claimed_leaders()
        if self._election.current_primary_id:
            claimed.add(self._election.current_primary_id)
        # A leader that others still follow but we cannot reach is the
        # failure detectors' call, not ours.
        if any(c != self._node_id and c not in view.responses for c in claimed):
            self._leaderless_checks = 0
            return

        self._leaderless_checks += 1
        if self._leaderless_checks < self._leaderless_threshold:
            return
        self._leaderless_checks = 0
        if self._election_task is not None and not self._election_task.done():
            return
        logger.warning(
            "Node %s sees an established cluster (epoch %s) with no confirmed primary; "
            "starting an election",
            self._node_id,
            max(view.max_epoch, self._election.election_epoch),
        )
        self._election_task = asyncio.create_task(self._election.start_election())

    async def _repair_replica_state(
        self,
        leader_id: str,
        leader: replication_pb2.ProgressResponse,
    ) -> None:
        snap = self._progress.snapshot()
        leader_epoch = int(leader.epoch)
        leader_seq = int(leader.last_applied_sequence)

        # Only checks where the replica is behind count towards "stalled";
        # time spent in step with the leader must not pre-arm a re-sync.
        position = (snap.epoch, snap.last_applied_sequence)
        if position == (leader_epoch, leader_seq):
            self._last_position = None
            self._stalled_checks = 0
        elif position == self._last_position:
            self._stalled_checks += 1
        else:
            self._last_position = position
            self._stalled_checks = 0

        reason: Optional[str] = None
        if snap.gap_detected:
            reason = (
                f"sequence gap (expected {snap.gap_expected_sequence}, "
                f"observed {snap.gap_observed_sequence})"
            )
        elif snap.epoch < leader_epoch:
            reason = f"local epoch {snap.epoch} older than leader epoch {leader_epoch}"
        elif snap.epoch == leader_epoch:
            if snap.last_applied_sequence == leader_seq and snap.history_digest != leader.history_digest:
                reason = "history digest differs from leader at the same sequence"
            elif snap.last_applied_sequence != leader_seq and self._stalled_checks >= self._stalled_threshold:
                reason = (
                    f"no replication progress at sequence {snap.last_applied_sequence} "
                    f"(leader at {leader_seq})"
                )

        if reason is None:
            return

        logger.warning("Node %s re-synchronizing from %s: %s", self._node_id, leader_id, reason)
        self._last_position = None
        self._stalled_checks = 0
        await self._state_syncer(self._peers[leader_id].grpc_address)
