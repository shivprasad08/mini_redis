"""
Bully-based leader election for mini-redis.

The project uses the Bully algorithm rather than Raft here because the
course deadline favors a smaller, easier-to-explain failure-recovery
mechanism, while the syllabus explicitly allows election algorithms as
an alternative to full log-replicating consensus. The priority rule is
deterministic node-id ordering, which keeps the implementation simple
and stable across restarts.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.peer_channel import open_peer_channel
from miniredis.dispatcher import Dispatcher
from miniredis.health.heartbeat import FailureDetector
from miniredis.health.proto import health_pb2
from miniredis.health.proto import health_pb2_grpc
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.replication.replicator import Replicator
from miniredis.election.proto import election_pb2
from miniredis.election.proto import election_pb2_grpc
from miniredis.election.metrics import ElectionMetrics
from miniredis.election.leadership import LeadershipMixin, next_epoch

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PeerEndpoint:
	"""Known peer endpoints for Bully election and promotion."""

	node_id: str
	grpc_address: str
	tcp_address: str


class BullyElection(LeadershipMixin):
	"""Coordinates Bully leader election for a single node."""

	def __init__(
		self,
		node_id: str,
		tcp_address: str,
		grpc_address: str,
		peers: Mapping[str, PeerEndpoint],
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
		self._progress = progress  # ReplicationProgress | None (M15)
		self._lock_manager = lock_manager  # LockManager | None (M16)
		# Replication targets outside the election peer set (legacy --replica-of).
		self._extra_replica_targets = list(extra_replica_targets)
		self._election_epoch = 0

		# Metrics tracking for M18 comparison
		self._last_metrics: Optional[ElectionMetrics] = None
		self._election_start_time: float = 0.0
		self._election_msg_count: int = 0
		self._coordinator_msg_count: int = 0
		self._retry_skip_count: int = 0
		self._unavailable_nodes: int = 0

		self._is_electing = False
		self._election_lock = asyncio.Lock()
		self._channels: dict[str, grpc.aio.Channel] = {}
		self._stubs: dict[str, election_pb2_grpc.ElectionServiceStub] = {}
		self._health_stubs: dict[str, health_pb2_grpc.HealthServiceStub] = {}
		self._current_primary_id: str = node_id if role == "primary" else ""
		self._current_primary_address: str = tcp_address if role == "primary" else ""
		self._servicer = ElectionServicer(self)

		for peer_id, peer in self._peers.items():
			if peer_id == self._node_id:
				continue
			channel = open_peer_channel(peer.grpc_address)
			self._channels[peer_id] = channel
			self._stubs[peer_id] = election_pb2_grpc.ElectionServiceStub(channel)
			self._health_stubs[peer_id] = health_pb2_grpc.HealthServiceStub(channel)

	@property
	def servicer(self) -> "ElectionServicer":
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
		"""Attach or replace the failure detector used to watch the primary."""
		self._failure_detector = failure_detector

	def attach_time_synchronizer(self, time_synchronizer: Optional[Any]) -> None:
		"""Attach or replace the replica time synchronizer."""
		self._time_synchronizer = time_synchronizer

	def attach_lock_manager(self, lock_manager: Optional[Any]) -> None:
		"""Attach or replace the lock manager for M16."""
		self._lock_manager = lock_manager

	@property
	def node_id(self) -> str:
		return self._node_id

	@property
	def priority(self) -> str:
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
		"""Return message and timing metrics from the last completed election."""
		return self._last_metrics

	def _peer_is_higher_priority(self, peer_id: str) -> bool:
		return peer_id > self._node_id

	async def handle_majority_change(self, majority_reachable: bool) -> None:
		"""React to majority-loss or majority-restoration events."""
		if majority_reachable:
			if self._role == "primary":
				self._write_gate.allow_writes()
			return

		logger.warning("Node %s lost cluster majority", self._node_id)
		self._write_gate.block_writes("minority partition, refusing writes")
		if self._role == "primary":
			self._role = "replica"
			self._dispatcher.set_writes_enabled(False)
			replicator = self._dispatcher.replicator
			self._dispatcher.set_replicator(None)
			if replicator is not None:
				await replicator.aclose()
			# GetProgress must not keep advertising this node as primary.
			if self._progress is not None:
				self._progress.set_role("replica")

	async def _has_cluster_majority(self) -> bool:
		reachable_peers = 0
		tasks = [asyncio.create_task(self._probe_peer_health(peer_id)) for peer_id in self._health_stubs]
		try:
			for task in asyncio.as_completed(tasks):
				if await task:
					reachable_peers += 1
		finally:
			for task in tasks:
				if not task.done():
					task.cancel()
			if tasks:
				await asyncio.gather(*tasks, return_exceptions=True)

		return (reachable_peers + 1) >= majority_count(self._total_cluster_size)

	async def _probe_peer_health(self, peer_id: str) -> bool:
		peer = self._peers[peer_id]
		if self._rpc_filter is not None and not self._rpc_filter("Heartbeat", peer.grpc_address):
			return False

		request_ts = await self._lamport_clock.tick()
		request = health_pb2.Ping(
			sender_id=self._node_id,
			timestamp_ms=0,
			lamport_timestamp=request_ts,
		)
		log_lamport_event(
			timestamp=request_ts,
			node_id=self._node_id,
			event_type="heartbeat_send",
			message_id=peer_id,
			description="election majority probe",
		)
		try:
			stub = self._health_stubs[peer_id]
			response = await asyncio.wait_for(stub.Heartbeat(request), timeout=self._rpc_timeout_seconds)
			response_ts = await self._lamport_clock.receive(response.lamport_timestamp)
			log_lamport_event(
				timestamp=response_ts,
				node_id=self._node_id,
				event_type="heartbeat_recv",
				message_id=response.responder_id,
				description="election majority response",
			)
			return True
		except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError):
			return False

	async def notify_higher_priority_peer(self, sender_id: str) -> None:
		"""Start an election when a lower-priority peer calls for one."""
		del sender_id
		async with self._election_lock:
			if self._is_electing:
				return
			self._is_electing = True
			self._election_start_time = time.monotonic()
			self._election_msg_count = 0
			self._coordinator_msg_count = 0
			self._retry_skip_count = 0
			self._unavailable_nodes = 0

		asyncio.create_task(self._run_election_round())

	async def start_election(self) -> None:
		"""Begin a Bully election if one is not already running."""
		async with self._election_lock:
			if self._is_electing:
				return
			self._is_electing = True
			self._election_start_time = time.monotonic()
			self._election_msg_count = 0
			self._coordinator_msg_count = 0
			self._retry_skip_count = 0
			self._unavailable_nodes = 0

		logger.info(f"[BULLY] Node {self._node_id} starting an election")
		await self._run_election_round()

	async def _run_election_round(self) -> None:
		# The Bully priority check is not enough: a highest-priority node in a
		# minority partition must remain a replica and refuse writes.
		if not await self._has_cluster_majority():
			logger.warning("Node %s cannot reach majority, refusing to become primary", self._node_id)
			await self.handle_majority_change(False)
			async with self._election_lock:
				self._is_electing = False
			return

		higher_peers = [peer_id for peer_id in self._stubs if self._peer_is_higher_priority(peer_id)]
		if not higher_peers:
			logger.info(f"[BULLY] Node {self._node_id} has no higher-priority peers — declaring victory")
			await self._declare_victory()
			return

		logger.info(
			f"[gRPC OUT][BULLY] Node {self._node_id} sending Election RPC to higher-priority peers: {higher_peers}"
		)
		tasks = [asyncio.create_task(self._probe_higher_peer(peer_id)) for peer_id in higher_peers]

		higher_peer_responded = False
		try:
			for task in asyncio.as_completed(tasks):
				if await task:
					higher_peer_responded = True
					break
		finally:
			for task in tasks:
				if not task.done():
					task.cancel()
			if tasks:
				await asyncio.gather(*tasks, return_exceptions=True)

		if higher_peer_responded:
			logger.info("Node %s standing down for higher-priority coordinator", self._node_id)
			async with self._election_lock:
				self._is_electing = False
			return

		await self._declare_victory()

	async def _probe_higher_peer(self, peer_id: str) -> bool:
		stub = self._stubs[peer_id]
		peer = self._peers[peer_id]
		if self._rpc_filter is not None and not self._rpc_filter("Election", peer.grpc_address):
			self._retry_skip_count += 1
			return False

		self._election_msg_count += 1
		request_ts = await self._lamport_clock.tick()
		log_lamport_event(
			timestamp=request_ts,
			node_id=self._node_id,
			event_type="election_send",
			message_id=peer_id,
			description="sent election request",
		)
		request = election_pb2.ElectionMessage(
			sender_id=self._node_id,
			sender_priority=0,
			lamport_timestamp=request_ts,
		)
		try:
			response = await asyncio.wait_for(
				stub.Election(request), timeout=self._rpc_timeout_seconds
			)
			response_ts = await self._lamport_clock.receive(response.lamport_timestamp)
			log_lamport_event(
				timestamp=response_ts,
				node_id=self._node_id,
				event_type="election_ack_recv",
				message_id=peer_id,
				description="received election ack",
			)
			return bool(response.acknowledged)
		except (asyncio.TimeoutError, grpc.aio.AioRpcError):
			logger.debug("No election response from higher-priority peer %s", peer_id)
			self._unavailable_nodes += 1
			return False

	async def _declare_victory(self) -> None:
		logger.info("[BULLY] Node %s WON the election", self._node_id)
		# Priority is by node id, not by data: never announce leadership
		# while a reachable peer holds newer state than this node.
		if not await self._catch_up_before_leading():
			logger.error(
				"[BULLY] Node %s could not catch up with the most advanced peer; refusing to lead",
				self._node_id,
			)
			async with self._election_lock:
				self._is_electing = False
			return

		self._election_epoch = next_epoch(self._election_epoch)
		await self._broadcast_coordinator()

		duration = max(0.001, time.monotonic() - self._election_start_time)
		total = self._election_msg_count + self._coordinator_msg_count + self._retry_skip_count
		self._last_metrics = ElectionMetrics(
			algorithm="bully",
			election_id=f"bully-{self._node_id}",
			initiator_id=self._node_id,
			elected_leader=self._node_id,
			epoch=self._election_epoch,
			duration_seconds=duration,
			initiation_messages=self._election_msg_count,
			candidate_messages=self._election_msg_count,
			coordinator_messages=self._coordinator_msg_count,
			retry_or_skip_messages=self._retry_skip_count,
			total_messages=total,
			participating_nodes=max(1, len(self._peers) + 1 - self._unavailable_nodes),
			unavailable_nodes=self._unavailable_nodes,
		)

		await self.promote_self()

		async with self._election_lock:
			self._is_electing = False

	async def _broadcast_coordinator(self) -> None:
		"""Announce this node as coordinator for the current epoch to every peer."""
		coordinator_tasks = []
		for peer_id, stub in self._stubs.items():
			if peer_id == self._node_id:
				continue
			peer = self._peers[peer_id]
			logger.info(f"[gRPC OUT][BULLY] Node {self._node_id} sending Coordinator RPC to {peer_id}")
			coordinator_tasks.append(
				asyncio.create_task(
					self._send_coordinator(stub, peer.grpc_address, peer.tcp_address)
				)
			)

		if coordinator_tasks:
			await asyncio.gather(*coordinator_tasks, return_exceptions=True)

	async def bootstrap_as_primary(self) -> bool:
		"""Become the first primary of a brand-new cluster.

		Only valid while this process has never learned any leadership
		(epoch 0, no known primary). The caller must already have checked
		that no reachable peer reports an established epoch; promote_self
		re-checks majority, so an isolated node still cannot lead.
		"""
		if not self._can_bootstrap():
			return False
		epoch = next_epoch(self._election_epoch)
		self._election_epoch = epoch
		await self.promote_self()
		if self._role != "primary":
			if self._election_epoch == epoch:
				self._election_epoch = 0
			return False
		await self._broadcast_coordinator()
		return True

	async def _send_coordinator(
		self,
		stub: election_pb2_grpc.ElectionServiceStub,
		grpc_address: str,
		tcp_address: str,
	) -> None:
		if self._rpc_filter is not None and not self._rpc_filter("Coordinator", grpc_address):
			self._retry_skip_count += 1
			return

		self._coordinator_msg_count += 1
		request_ts = await self._lamport_clock.tick()
		log_lamport_event(
			timestamp=request_ts,
			node_id=self._node_id,
			event_type="coordinator_send",
			message_id=grpc_address,
			description="sent coordinator",
		)
		request = election_pb2.CoordinatorMessage(
			new_primary_id=self._node_id,
			new_primary_address=self._tcp_address,
			election_epoch=self._election_epoch,
			lamport_timestamp=request_ts,
		)
		try:
			response = await asyncio.wait_for(
				stub.Coordinator(request), timeout=self._rpc_timeout_seconds
			)
			response_ts = await self._lamport_clock.receive(response.lamport_timestamp)
			log_lamport_event(
				timestamp=response_ts,
				node_id=self._node_id,
				event_type="coordinator_ack_recv",
				message_id=grpc_address,
				description="received coordinator ack",
			)
		except (asyncio.TimeoutError, grpc.aio.AioRpcError):
			logger.debug("Coordinator announcement to %s timed out", tcp_address)
			self._unavailable_nodes += 1
   
	async def aclose(self) -> None:
		"""Close every peer channel this election instance opened."""
		channels = list(self._channels.values())
		self._channels.clear()
		self._stubs.clear()
		self._health_stubs.clear()
		if channels:
			await asyncio.gather(*(channel.close() for channel in channels), return_exceptions=True)
	
	async def promote_self(self) -> None:
		"""Promote this node to primary and enable client writes."""
		if self._role == "primary":
			return

		if not await self._has_cluster_majority():
			logger.warning("Node %s cannot reach majority, promotion refused", self._node_id)
			await self.handle_majority_change(False)
			return

		self._role = "primary"
		self._current_primary_id = self._node_id
		self._current_primary_address = self._tcp_address
		self._dispatcher.set_writes_enabled(True)

		# Update progress tracker: new role and epoch (M15).
		if self._progress is not None:
			self._progress.update_role_and_epoch("primary", self._election_epoch)

		# Update lock manager: promoted to primary (M16).
		if self._lock_manager is not None and hasattr(self._lock_manager, "set_role"):
			self._lock_manager.set_role("primary")

		target_to_node_id = {peer.grpc_address: peer_id for peer_id, peer in self._peers.items()}
		replica_targets = [
			peer.grpc_address for peer_id, peer in self._peers.items() if peer_id != self._node_id
		]
		replica_targets += [t for t in self._extra_replica_targets if t not in replica_targets]
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
			)
			if replica_targets
			else None
		)

		if self._failure_detector is not None:
			await self._failure_detector.stop()
			self._failure_detector = None
		if self._time_synchronizer is not None:
			await self._time_synchronizer.stop()
			self._time_synchronizer = None

		self._write_gate.allow_writes()

		logger.info("Node %s promoted itself to primary", self._node_id)

	async def accept_coordinator(
		self,
		new_primary_id: str,
		new_primary_address: str,
		election_epoch: int = 0,
	) -> None:
		"""Update local primary state from a coordinator announcement."""
		if election_epoch < self._election_epoch:
			logger.warning(
				"Ignoring obsolete coordinator epoch %s; current epoch is %s",
				election_epoch,
				self._election_epoch,
			)
			return

		self._election_epoch = election_epoch
		self._current_primary_id = new_primary_id
		self._current_primary_address = new_primary_address
		self._is_electing = False

		if new_primary_id == self._node_id:
			await self.promote_self()
			return

		self._role = "replica"
		self._write_gate.block_writes("minority partition, refusing writes")
		self._dispatcher.set_writes_enabled(False)
		replicator = self._dispatcher.replicator
		self._dispatcher.set_replicator(None)
		if replicator is not None:
			await replicator.aclose()

		# Update progress tracker: demoted to replica (M15).
		if self._progress is not None:
			self._progress.update_role_and_epoch("replica", election_epoch)

		# Update lock manager: demoted to replica (M16).
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

		logger.info("Node %s now follows primary %s", self._node_id, new_primary_id)


class ElectionServicer(election_pb2_grpc.ElectionServiceServicer):
	"""gRPC servicer for Bully election coordination."""

	def __init__(self, election: BullyElection) -> None:
		self._election = election

	async def Election(
		self,
		request: election_pb2.ElectionMessage,
		context: grpc.aio.ServicerContext,
	) -> election_pb2.ElectionAck:
		del context
		recv_ts = await self._election._lamport_clock.receive(request.lamport_timestamp)
		log_lamport_event(
			timestamp=recv_ts,
			node_id=self._election.node_id,
			event_type="election_recv",
			message_id=request.sender_id,
			description="received election request",
		)

		logger.info(f"[gRPC IN][BULLY] Election RPC received from {request.sender_id}")

		if self._election.node_id > request.sender_id:
			logger.info(
				f"[BULLY] Node {self._election.node_id} outranks {request.sender_id} — "
				f"acknowledging and starting my own election"
			)
			await self._election.notify_higher_priority_peer(request.sender_id)
			ack_ts = await self._election._lamport_clock.tick()
			return election_pb2.ElectionAck(acknowledged=True, lamport_timestamp=ack_ts)

		ack_ts = await self._election._lamport_clock.tick()
		return election_pb2.ElectionAck(acknowledged=False, lamport_timestamp=ack_ts)

	async def Coordinator(
		self,
		request: election_pb2.CoordinatorMessage,
		context: grpc.aio.ServicerContext,
	) -> election_pb2.CoordinatorAck:
		del context
		recv_ts = await self._election._lamport_clock.receive(request.lamport_timestamp)
		log_lamport_event(
			timestamp=recv_ts,
			node_id=self._election.node_id,
			event_type="coordinator_recv",
			message_id=request.new_primary_id,
			description="received coordinator",
		)
		logger.info(
			f"[gRPC IN][BULLY] Coordinator RPC received — new primary is {request.new_primary_id} "
			f"at {request.new_primary_address}"
		)
		await self._election.accept_coordinator(
			request.new_primary_id,
			request.new_primary_address,
			request.election_epoch,
		)
		ack_ts = await self._election._lamport_clock.tick()
		return election_pb2.CoordinatorAck(acknowledged=True, lamport_timestamp=ack_ts)
