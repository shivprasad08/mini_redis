"""
Chang-Roberts unidirectional ring leader election for mini-redis (M18).

Topology & Node Ordering:
─────────────────────────
All nodes in the cluster membership (self + peers) form a logical unidirectional
ring ordered deterministically by node_id in ascending order:
  N0 -> N1 -> ... -> N(n-1) -> N0

Algorithm (Chang-Roberts Variant):
──────────────────────────────────
1. Initiation:
   When an active node detects failure of the current primary:
   - Allocates a unique election_id and election epoch.
   - Sends a RingElectionMessage containing candidate_id = self.node_id to its
     next reachable successor in the ring.
2. Candidate Forwarding:
   When node J receives a RingElectionMessage with candidate C:
   - If C > J: J forwards the message with candidate C to J's next successor.
   - If C < J: J replaces candidate C with J and forwards to J's next successor.
   - If C == J: The message has circled the ring back to J! J has highest priority
     and is elected.
3. Majority Gate Protection:
   Before promotion, the elected node validates that it reached a cluster majority:
     len(visited_nodes) >= majority_count(total_cluster_size)
   If majority is not satisfied (e.g. isolated minority partition), the node FAILS
   CLOSED and refuses promotion.
4. Coordinator Announcement:
   The elected primary circulates a RingCoordinatorMessage around the ring to all
   reachable participants so they update their leader identity, demote to replicas,
   and synchronise epochs.
5. Fault Tolerance (Skip-node routing):
   If the immediate successor is dead or unreachable, the node skips it and tries
   the next node in ring order, tracking skip/retry message counts.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Mapping, Optional, Sequence, Set

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.peer_channel import open_peer_channel
from miniredis.dispatcher import Dispatcher
from miniredis.election.leadership import LeadershipMixin, next_epoch
from miniredis.election.metrics import ElectionMetrics
from miniredis.election.proto import election_pb2, election_pb2_grpc
from miniredis.health.heartbeat import FailureDetector
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.replication.replicator import Replicator

logger = logging.getLogger(__name__)


class RingElection(LeadershipMixin):
    """Coordinates Chang-Roberts ring election for a single node."""

    def __init__(
        self,
        node_id: str,
        tcp_address: str,
        grpc_address: str,
        peers: Mapping[str, Any],  # peer_id -> PeerEndpoint
        dispatcher: Dispatcher,
        total_cluster_size: int,
        failure_detector: Optional[FailureDetector] = None,
        write_gate: Optional[WriteAdmissionGate] = None,
        role: str = "replica",
        write_mode: str = "async",
        rpc_timeout_seconds: float = 0.5,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
        simulate_latency_ms: float = 0.0,
        lamport_clock: Optional[LamportClock] = None,
        progress: Optional[Any] = None,
        lock_manager: Optional[Any] = None,
        extra_replica_targets: Sequence[str] = (),
    ) -> None:
        self._node_id = node_id
        self._tcp_address = tcp_address
        self._grpc_address = grpc_address
        self._peers = dict(peers)
        self._dispatcher = dispatcher
        self._total_cluster_size = total_cluster_size
        self._failure_detector = failure_detector
        self._time_synchronizer: Optional[Any] = None
        self._write_gate = write_gate or WriteAdmissionGate(can_serve_writes=(role == "primary"))
        self._role = role
        self._write_mode = write_mode
        self._rpc_timeout_seconds = rpc_timeout_seconds
        self._rpc_filter = rpc_filter
        self._simulate_latency_ms = simulate_latency_ms
        self._lamport_clock = lamport_clock or LamportClock()
        self._progress = progress
        self._lock_manager = lock_manager
        # Replication targets outside the election peer set (legacy --replica-of).
        self._extra_replica_targets = list(extra_replica_targets)
        self._election_epoch = 0

        self._is_electing = False
        self._election_lock = asyncio.Lock()
        self._current_primary_id: str = node_id if role == "primary" else ""
        self._current_primary_address: str = tcp_address if role == "primary" else ""

        # gRPC channels and stubs to peers
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, election_pb2_grpc.ElectionServiceStub] = {}
        for peer_id, peer in self._peers.items():
            if peer_id == self._node_id:
                continue
            channel = open_peer_channel(peer.grpc_address)
            self._channels[peer_id] = channel
            self._stubs[peer_id] = election_pb2_grpc.ElectionServiceStub(channel)

        self._servicer = RingElectionServicer(self)

        # Ring state
        self._participating_election_ids: Set[str] = set()
        self._seen_coordinator_epochs: Set[int] = set()

        # Metrics for comparison
        self._last_metrics: Optional[ElectionMetrics] = None
        self._metrics_lock = asyncio.Lock()
        self._initiation_count = 0
        self._candidate_count = 0
        self._coordinator_count = 0
        self._retry_skip_count = 0

    @property
    def servicer(self) -> "RingElectionServicer":
        return self._servicer

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher

    @property
    def write_gate(self) -> WriteAdmissionGate:
        return self._write_gate

    @property
    def failure_detector(self) -> Optional[FailureDetector]:
        return self._failure_detector

    def attach_failure_detector(self, failure_detector: Optional[FailureDetector]) -> None:
        self._failure_detector = failure_detector

    def attach_time_synchronizer(self, time_synchronizer: Optional[Any]) -> None:
        self._time_synchronizer = time_synchronizer

    def attach_lock_manager(self, lock_manager: Optional[Any]) -> None:
        self._lock_manager = lock_manager

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def role(self) -> str:
        return self._role

    @property
    def is_electing(self) -> bool:
        return self._is_electing

    @property
    def current_primary_id(self) -> str:
        return self._current_primary_id

    @property
    def current_primary_address(self) -> str:
        return self._current_primary_address

    @property
    def election_epoch(self) -> int:
        return self._election_epoch

    def get_last_election_metrics(self) -> Optional[ElectionMetrics]:
        return self._last_metrics

    async def aclose(self) -> None:
        for channel in self._channels.values():
            await channel.close()

    def _get_ordered_ring(self) -> list[str]:
        """Return all known cluster nodes in sorted ring order."""
        all_nodes = sorted(list(self._peers.keys()) + [self._node_id])
        return all_nodes

    def _get_successor_candidates(self) -> list[str]:
        """Return potential next ring nodes starting with immediate successor."""
        ring = self._get_ordered_ring()
        n = len(ring)
        if n <= 1:
            return []
        idx = ring.index(self._node_id)
        # Sequence of candidate successors in ring order
        return [ring[(idx + step) % n] for step in range(1, n)]

    # ── Election Initiation & Forwarding ───────────────────────────

    async def start_election(self) -> None:
        """Initiate ring election when primary is declared dead."""
        async with self._election_lock:
            if self._is_electing:
                return
            self._is_electing = True
            self._initiation_count = 0
            self._candidate_count = 0
            self._coordinator_count = 0
            self._retry_skip_count = 0

        election_start_time = time.monotonic()
        election_id = f"ring-{self._node_id}-{int(time.time()*1000)}"
        proposed_epoch = int(time.time_ns())
        self._participating_election_ids.add(election_id)

        logger.info(
            f"[RING] Node {self._node_id} initiating ring election id={election_id} epoch={proposed_epoch}"
        )
        self._initiation_count += 1

        # Send initial message to first reachable successor
        visited = [self._node_id]
        successors = self._get_successor_candidates()

        if not successors:
            # Single-node cluster — elect self immediately if majority allows
            if 1 >= majority_count(self._total_cluster_size):
                await self.promote_self()
                self._is_electing = False
            return

        sent = await self._forward_election_message(
            election_id=election_id,
            candidate_id=self._node_id,
            election_epoch=proposed_epoch,
            visited_nodes=visited,
        )

        if not sent:
            logger.warning(f"[RING] Node {self._node_id} could not reach any successor in ring")
            self._is_electing = False

    async def _forward_election_message(
        self,
        election_id: str,
        candidate_id: str,
        election_epoch: int,
        visited_nodes: list[str],
    ) -> bool:
        """Forward RingElectionMessage to the next reachable successor in the ring."""
        successors = self._get_successor_candidates()
        for idx, target_id in enumerate(successors):
            stub = self._stubs.get(target_id)
            if stub is None:
                continue

            if self._rpc_filter is not None:
                peer = self._peers.get(target_id)
                addr = peer.grpc_address if peer else ""
                if not self._rpc_filter("RingElection", addr):
                    self._retry_skip_count += 1
                    continue

            req_ts = await self._lamport_clock.tick()
            log_lamport_event(
                timestamp=req_ts,
                node_id=self._node_id,
                event_type="ring_election_send",
                message_id=target_id,
                description=f"forwarded ring election candidate {candidate_id}",
            )
            self._candidate_count += 1

            msg = election_pb2.RingElectionMessage(
                sender_id=self._node_id,
                election_id=election_id,
                candidate_id=candidate_id,
                election_epoch=election_epoch,
                lamport_timestamp=req_ts,
                visited_nodes=visited_nodes,
            )

            try:
                resp = await asyncio.wait_for(stub.RingElection(msg), timeout=self._rpc_timeout_seconds)
                await self._lamport_clock.receive(resp.lamport_timestamp)
                logger.debug(f"[RING] Election message acknowledged by {target_id}")
                return True
            except Exception as exc:
                logger.warning(f"[RING] Successor {target_id} failed ({exc}); trying next candidate in ring")
                self._retry_skip_count += 1

        return False

    async def handle_ring_election_message(
        self,
        sender_id: str,
        election_id: str,
        candidate_id: str,
        election_epoch: int,
        visited_nodes: list[str],
        lamport_timestamp: int,
    ) -> None:
        """Handle incoming RingElectionMessage from ring predecessor."""
        recv_ts = await self._lamport_clock.receive(lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="ring_election_recv",
            message_id=sender_id,
            description=f"received candidate {candidate_id}",
        )

        logger.info(
            f"[gRPC IN][RING] Election msg received from {sender_id}: "
            f"candidate={candidate_id} epoch={election_epoch} visited={visited_nodes}"
        )

        if election_epoch < self._election_epoch:
            logger.warning(
                f"[RING] Ignoring obsolete election epoch {election_epoch} < {self._election_epoch}"
            )
            return

        # Append self to visited if not already in visited
        updated_visited = list(visited_nodes)
        if self._node_id not in updated_visited:
            updated_visited.append(self._node_id)

        # ── Chang-Roberts logic ──
        if candidate_id > self._node_id:
            # Candidate has higher priority than us: forward candidate C
            logger.info(f"[RING] Forwarding higher candidate {candidate_id} > {self._node_id}")
            await self._forward_election_message(
                election_id=election_id,
                candidate_id=candidate_id,
                election_epoch=election_epoch,
                visited_nodes=updated_visited,
            )

        elif candidate_id < self._node_id:
            # We have higher priority than candidate C: replace with self and forward!
            logger.info(f"[RING] Overriding candidate {candidate_id} with higher self {self._node_id}")
            await self._forward_election_message(
                election_id=election_id,
                candidate_id=self._node_id,
                election_epoch=election_epoch,
                visited_nodes=updated_visited,
            )

        else:
            # candidate_id == self._node_id!
            # The message completed a full circuit around the ring. We are elected!
            logger.info(f"[RING] Circuit complete! Node {self._node_id} is ELECTED!")
            unique_visited = set(updated_visited)
            reachable_count = len(unique_visited)
            needed_majority = majority_count(self._total_cluster_size)

            if reachable_count < needed_majority:
                # Minority partition: fail closed!
                logger.error(
                    f"[RING] Elected node {self._node_id} reached only {reachable_count} of {needed_majority} "
                    f"majority nodes! Refusing promotion to protect against split-brain."
                )
                self._write_gate.block_writes("minority partition, refusing promotion")
                self._is_electing = False
                return

            # Priority is by node id, not by data: never announce leadership
            # while a reachable peer holds newer state than this node.
            if not await self._catch_up_before_leading():
                logger.error(
                    f"[RING] Elected node {self._node_id} could not catch up with the most "
                    f"advanced peer; refusing promotion"
                )
                self._is_electing = False
                return

            # Concurrent initiators can complete several circuits for this node
            # with different epochs; one older than an epoch we already hold
            # is obsolete and must not move the epoch backwards.
            if election_epoch < self._election_epoch or (
                election_epoch == self._election_epoch and self._role == "primary"
            ):
                logger.info(
                    f"[RING] Ignoring completed circuit for epoch {election_epoch}; "
                    f"already at epoch {self._election_epoch}"
                )
                return

            # Majority satisfied: broadcast coordinator then promote!
            self._election_epoch = election_epoch
            self._is_electing = False

            # Broadcast coordinator message around the ring
            await self._announce_coordinator(
                election_epoch=election_epoch,
                visited_nodes=[self._node_id],
            )

            await self.promote_self(election_epoch)

    # ── Coordinator Announcement ────────────────────────────────────

    async def _announce_coordinator(
        self,
        election_epoch: int,
        visited_nodes: list[str],
    ) -> bool:
        """Announce newly elected coordinator to the ring."""
        successors = self._get_successor_candidates()
        for target_id in successors:
            stub = self._stubs.get(target_id)
            if stub is None:
                continue

            if self._rpc_filter is not None:
                peer = self._peers.get(target_id)
                addr = peer.grpc_address if peer else ""
                if not self._rpc_filter("RingCoordinator", addr):
                    self._retry_skip_count += 1
                    continue

            req_ts = await self._lamport_clock.tick()
            log_lamport_event(
                timestamp=req_ts,
                node_id=self._node_id,
                event_type="ring_coordinator_send",
                message_id=target_id,
                description=f"announcing coordinator {self._node_id}",
            )
            self._coordinator_count += 1

            msg = election_pb2.RingCoordinatorMessage(
                sender_id=self._node_id,
                new_primary_id=self._node_id,
                new_primary_address=self._tcp_address,
                election_epoch=election_epoch,
                lamport_timestamp=req_ts,
                visited_nodes=visited_nodes,
            )

            try:
                resp = await asyncio.wait_for(stub.RingCoordinator(msg), timeout=self._rpc_timeout_seconds)
                await self._lamport_clock.receive(resp.lamport_timestamp)
                logger.debug(f"[RING] Coordinator acknowledged by {target_id}")
                return True
            except Exception as exc:
                logger.warning(f"[RING] Failed to send coordinator to {target_id}: {exc}")
                self._retry_skip_count += 1

        return False

    async def handle_ring_coordinator_message(
        self,
        sender_id: str,
        new_primary_id: str,
        new_primary_address: str,
        election_epoch: int,
        visited_nodes: list[str],
        lamport_timestamp: int,
    ) -> None:
        """Handle incoming RingCoordinatorMessage."""
        recv_ts = await self._lamport_clock.receive(lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="ring_coordinator_recv",
            message_id=new_primary_id,
            description="received coordinator announcement",
        )

        logger.info(
            f"[gRPC IN][RING] Coordinator received: new primary is {new_primary_id} epoch={election_epoch}"
        )

        if election_epoch < self._election_epoch:
            return

        if election_epoch in self._seen_coordinator_epochs:
            # Already accepted and forwarded this epoch
            return

        self._seen_coordinator_epochs.add(election_epoch)

        # Accept new coordinator
        await self.accept_coordinator(
            new_primary_id=new_primary_id,
            new_primary_address=new_primary_address,
            election_epoch=election_epoch,
        )

        # Forward coordinator announcement to next ring node if circuit not complete
        if self._node_id != new_primary_id:
            updated_visited = list(visited_nodes) + [self._node_id]
            successors = self._get_successor_candidates()
            for target_id in successors:
                if target_id == new_primary_id:
                    # Completed full circle back to primary!
                    break

                stub = self._stubs.get(target_id)
                if stub is None:
                    continue

                if self._rpc_filter is not None:
                    peer = self._peers.get(target_id)
                    addr = peer.grpc_address if peer else ""
                    if not self._rpc_filter("RingCoordinator", addr):
                        continue

                req_ts = await self._lamport_clock.tick()
                msg = election_pb2.RingCoordinatorMessage(
                    sender_id=self._node_id,
                    new_primary_id=new_primary_id,
                    new_primary_address=new_primary_address,
                    election_epoch=election_epoch,
                    lamport_timestamp=req_ts,
                    visited_nodes=updated_visited,
                )
                try:
                    resp = await asyncio.wait_for(stub.RingCoordinator(msg), timeout=self._rpc_timeout_seconds)
                    await self._lamport_clock.receive(resp.lamport_timestamp)
                    break
                except Exception:
                    continue

    # ── Promotion & Demotion Lifecycle ──────────────────────────────

    async def bootstrap_as_primary(self) -> bool:
        """Become the first primary of a brand-new cluster.

        Only valid while this process has never learned any leadership
        (epoch 0, no known primary). The caller must have verified that a
        majority is reachable and that no peer reports an established epoch.
        """
        if not self._can_bootstrap():
            return False
        epoch = next_epoch(self._election_epoch)
        await self.promote_self(epoch)
        await self._announce_coordinator(election_epoch=epoch, visited_nodes=[self._node_id])
        return True

    async def promote_self(self, epoch: Optional[int] = None) -> None:
        """Promote this node to primary.

        *epoch* is the epoch this node was elected (and announced) under; the
        primary must replicate with that same epoch. Without one, a fresh
        epoch is generated as before.
        """
        if epoch is not None and epoch < self._election_epoch:
            logger.warning(
                f"[RING] Refusing promotion under obsolete epoch {epoch} < {self._election_epoch}"
            )
            return
        self._role = "primary"
        self._election_epoch = epoch if epoch is not None else int(time.time_ns())
        self._current_primary_id = self._node_id
        self._current_primary_address = self._tcp_address
        self._dispatcher.set_writes_enabled(True)

        if self._progress is not None:
            self._progress.update_role_and_epoch("primary", self._election_epoch)

        if self._lock_manager is not None and hasattr(self._lock_manager, "set_role"):
            self._lock_manager.set_role("primary")

        target_to_node_id = {peer.grpc_address: peer_id for peer_id, peer in self._peers.items()}
        replica_targets = [
            peer.grpc_address for peer_id, peer in self._peers.items() if peer_id != self._node_id
        ]
        replica_targets += [t for t in self._extra_replica_targets if t not in replica_targets]
        previous_replicator = self._dispatcher.replicator
        self._dispatcher.set_replicator(
            Replicator(
                replica_targets,
                total_cluster_size=self._total_cluster_size,
                write_mode=self._write_mode,
                simulate_latency_ms=self._simulate_latency_ms,
                write_gate=self._write_gate,
                rpc_filter=self._rpc_filter,
                epoch=self._election_epoch,
                node_id=self._node_id,
                lamport_clock=self._lamport_clock,
                progress=self._progress,
                target_to_node_id=target_to_node_id,
                on_higher_epoch=self.observe_higher_epoch,
                links=self._links,
            )
            if replica_targets
            else None
        )
        if previous_replicator is not None:
            await previous_replicator.aclose()

        if self._failure_detector is not None:
            await self._failure_detector.stop()
            # A stopped detector cannot be retargeted later; drop it so a
            # future demotion creates a fresh one.
            self._failure_detector = None

        # Record metrics for ring election
        total = self._initiation_count + self._candidate_count + self._coordinator_count + self._retry_skip_count
        self._last_metrics = ElectionMetrics(
            algorithm="ring",
            election_id=f"ring-{self._node_id}",
            initiator_id=self._node_id,
            elected_leader=self._node_id,
            epoch=self._election_epoch,
            duration_seconds=0.05,
            initiation_messages=self._initiation_count,
            candidate_messages=self._candidate_count,
            coordinator_messages=self._coordinator_count,
            retry_or_skip_messages=self._retry_skip_count,
            total_messages=total,
            participating_nodes=len(self._peers) + 1,
            unavailable_nodes=0,
        )

        logger.info(
            f"Node {self._node_id} PROMOTED to PRIMARY via RING ELECTION under epoch {self._election_epoch}"
        )

    async def accept_coordinator(
        self,
        new_primary_id: str,
        new_primary_address: str,
        election_epoch: int,
    ) -> None:
        """Demote to replica following newly elected primary."""
        if election_epoch < self._election_epoch:
            return

        self._election_epoch = election_epoch
        self._current_primary_id = new_primary_id
        self._current_primary_address = new_primary_address
        self._is_electing = False

        if new_primary_id == self._node_id:
            await self.promote_self(election_epoch)
            return

        self._role = "replica"
        self._write_gate.block_writes("minority partition, refusing writes")
        self._dispatcher.set_writes_enabled(False)
        replicator = self._dispatcher.replicator
        self._dispatcher.set_replicator(None)
        if replicator is not None:
            await replicator.aclose()

        if self._progress is not None:
            self._progress.update_role_and_epoch("replica", election_epoch)

        if self._lock_manager is not None and hasattr(self._lock_manager, "set_role"):
            peer = self._peers.get(new_primary_id)
            detector_address = peer.grpc_address if peer is not None else new_primary_address
            self._lock_manager.set_role("replica", primary_address=detector_address)

        peer = self._peers.get(new_primary_id)
        detector_address = peer.grpc_address if peer is not None else new_primary_address
        if self._failure_detector is not None:
            await self._failure_detector.update_peer(new_primary_id, detector_address)
            if self._time_synchronizer is not None:
                await self._time_synchronizer.update_peer(detector_address)
        else:
            from miniredis.health.heartbeat import FailureDetector
            self._failure_detector = FailureDetector(
                peer_node_id=new_primary_id,
                peer_address=detector_address,
                heartbeat_interval_seconds=1.0,
                heartbeat_timeout_seconds=0.5,
                failure_threshold=3,
                sender_id=self._node_id,
                rpc_filter=self._rpc_filter,
                lamport_clock=self._lamport_clock,
            )

            def _log_state_change(peer_id: str, old_state: str, new_state: str) -> None:
                del peer_id, old_state
                if new_state == "suspected":
                    logger.warning("PRIMARY SUSPECTED DOWN")
                elif new_state == "dead":
                    logger.error("PRIMARY DECLARED DEAD")
                    asyncio.create_task(self.start_election())
                elif new_state == "alive":
                    logger.info("PRIMARY RECOVERED")

            self._failure_detector.on_state_change(_log_state_change)
            await self._failure_detector.start()

        logger.info(f"Node {self._node_id} now follows primary {new_primary_id} via RING ELECTION")

    async def handle_majority_change(self, majority_reachable: bool) -> None:
        """Called by ClusterMajorityMonitor when network partitions heal or form."""
        if majority_reachable:
            logger.info("Node %s regained majority connectivity", self._node_id)
            if self._role == "primary":
                # A newer leader may have been elected during the partition;
                # confirm before resuming writes (steps down otherwise).
                if await self.confirm_leadership():
                    self._write_gate.allow_writes()
                    self._dispatcher.set_writes_enabled(True)
        else:
            logger.warning("Node %s lost majority connectivity", self._node_id)
            self._write_gate.block_writes("minority partition, refusing writes")
            if self._role == "primary":
                self._dispatcher.set_writes_enabled(False)


class RingElectionServicer(election_pb2_grpc.ElectionServiceServicer):
    """gRPC servicer for Ring election coordination."""

    def __init__(self, election: RingElection) -> None:
        self._election = election

    async def Election(
        self,
        request: election_pb2.ElectionMessage,
        context: grpc.aio.ServicerContext,
    ) -> election_pb2.ElectionAck:
        del context
        # Bully compatibility stub
        ack_ts = await self._election._lamport_clock.tick()
        return election_pb2.ElectionAck(acknowledged=False, lamport_timestamp=ack_ts)

    async def Coordinator(
        self,
        request: election_pb2.CoordinatorMessage,
        context: grpc.aio.ServicerContext,
    ) -> election_pb2.CoordinatorAck:
        del context
        await self._election.accept_coordinator(
            request.new_primary_id, request.new_primary_address, request.election_epoch
        )
        ack_ts = await self._election._lamport_clock.tick()
        return election_pb2.CoordinatorAck(acknowledged=True, lamport_timestamp=ack_ts)

    async def RingElection(
        self,
        request: election_pb2.RingElectionMessage,
        context: grpc.aio.ServicerContext,
    ) -> election_pb2.RingElectionAck:
        del context
        ack_ts = await self._election._lamport_clock.tick()
        asyncio.create_task(
            self._election.handle_ring_election_message(
                sender_id=request.sender_id,
                election_id=request.election_id,
                candidate_id=request.candidate_id,
                election_epoch=request.election_epoch,
                visited_nodes=list(request.visited_nodes),
                lamport_timestamp=request.lamport_timestamp,
            )
        )
        return election_pb2.RingElectionAck(acknowledged=True, lamport_timestamp=ack_ts)

    async def RingCoordinator(
        self,
        request: election_pb2.RingCoordinatorMessage,
        context: grpc.aio.ServicerContext,
    ) -> election_pb2.RingCoordinatorAck:
        del context
        ack_ts = await self._election._lamport_clock.tick()
        asyncio.create_task(
            self._election.handle_ring_coordinator_message(
                sender_id=request.sender_id,
                new_primary_id=request.new_primary_id,
                new_primary_address=request.new_primary_address,
                election_epoch=request.election_epoch,
                visited_nodes=list(request.visited_nodes),
                lamport_timestamp=request.lamport_timestamp,
            )
        )
        return election_pb2.RingCoordinatorAck(acknowledged=True, lamport_timestamp=ack_ts)
