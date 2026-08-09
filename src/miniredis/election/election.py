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
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.dispatcher import Dispatcher
from miniredis.health.heartbeat import FailureDetector
from miniredis.health.proto import health_pb2
from miniredis.health.proto import health_pb2_grpc
from miniredis.replication.replicator import Replicator
from miniredis.election.proto import election_pb2
from miniredis.election.proto import election_pb2_grpc

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PeerEndpoint:
	"""Known peer endpoints for Bully election and promotion."""

	node_id: str
	grpc_address: str
	tcp_address: str


class BullyElection:
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
	) -> None:
		self._node_id = node_id
		self._tcp_address = tcp_address
		self._grpc_address = grpc_address
		self._peers = dict(peers)
		self._dispatcher = dispatcher
		self._total_cluster_size = total_cluster_size
		self._failure_detector = failure_detector
		self._write_gate = write_gate or WriteAdmissionGate(can_serve_writes=(role == "primary"))
		self._role = role
		self._write_mode = write_mode
		self._rpc_timeout_seconds = rpc_timeout_seconds
		self._rpc_filter = rpc_filter

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
			channel = grpc.aio.insecure_channel(peer.grpc_address)
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
			self._dispatcher.set_replicator(None)

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

		request = health_pb2.Ping(sender_id=self._node_id, timestamp_ms=0)
		try:
			stub = self._health_stubs[peer_id]
			await asyncio.wait_for(stub.Heartbeat(request), timeout=self._rpc_timeout_seconds)
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

		asyncio.create_task(self._run_election_round())

	async def start_election(self) -> None:
		"""Begin a Bully election if one is not already running."""
		async with self._election_lock:
			if self._is_electing:
				return
			self._is_electing = True

		await self._run_election_round()

	async def _run_election_round(self) -> None:
		higher_peers = [peer_id for peer_id in self._stubs if self._peer_is_higher_priority(peer_id)]
		if not higher_peers:
			await self._declare_victory()
			return

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

		if not await self._has_cluster_majority():
			logger.warning("Node %s cannot reach majority, refusing to become primary", self._node_id)
			await self.handle_majority_change(False)
			async with self._election_lock:
				self._is_electing = False
			return

		await self._declare_victory()

	async def _probe_higher_peer(self, peer_id: str) -> bool:
		stub = self._stubs[peer_id]
		peer = self._peers[peer_id]
		if self._rpc_filter is not None and not self._rpc_filter("Election", peer.grpc_address):
			return False

		request = election_pb2.ElectionMessage(
			sender_id=self._node_id,
			sender_priority=0,
		)
		try:
			response = await asyncio.wait_for(
				stub.Election(request), timeout=self._rpc_timeout_seconds
			)
			return bool(response.acknowledged)
		except (asyncio.TimeoutError, grpc.aio.AioRpcError):
			logger.debug("No election response from higher-priority peer %s", peer_id)
			return False

	async def _declare_victory(self) -> None:
		logger.info("Node %s won the election", self._node_id)

		coordinator_tasks = []
		for peer_id, stub in self._stubs.items():
			if peer_id == self._node_id:
				continue
			peer = self._peers[peer_id]
			coordinator_tasks.append(
				asyncio.create_task(
					self._send_coordinator(stub, peer.grpc_address, peer.tcp_address)
				)
			)

		if coordinator_tasks:
			await asyncio.gather(*coordinator_tasks, return_exceptions=True)

		await self.promote_self()

		async with self._election_lock:
			self._is_electing = False

	async def _send_coordinator(
		self,
		stub: election_pb2_grpc.ElectionServiceStub,
		grpc_address: str,
		tcp_address: str,
	) -> None:
		if self._rpc_filter is not None and not self._rpc_filter("Coordinator", grpc_address):
			return

		request = election_pb2.CoordinatorMessage(
			new_primary_id=self._node_id,
			new_primary_address=self._tcp_address,
		)
		try:
			await asyncio.wait_for(
				stub.Coordinator(request), timeout=self._rpc_timeout_seconds
			)
		except (asyncio.TimeoutError, grpc.aio.AioRpcError):
			logger.debug("Coordinator announcement to %s timed out", tcp_address)
   
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

		self._role = "primary"
		self._current_primary_id = self._node_id
		self._current_primary_address = self._tcp_address
		self._dispatcher.set_writes_enabled(True)

		replica_targets = [
			peer.grpc_address for peer_id, peer in self._peers.items() if peer_id != self._node_id
		]
		self._dispatcher.set_replicator(
			Replicator(
				replica_targets,
				total_cluster_size=self._total_cluster_size,
				write_mode=self._write_mode,
				write_gate=self._write_gate,
				rpc_filter=self._rpc_filter,
			)
			if replica_targets
			else None
		)

		if self._failure_detector is not None:
			await self._failure_detector.stop()
			self._failure_detector = None

		self._write_gate.allow_writes()

		logger.info("Node %s promoted itself to primary", self._node_id)

	async def accept_coordinator(self, new_primary_id: str, new_primary_address: str) -> None:
		"""Update local primary state from a coordinator announcement."""
		self._current_primary_id = new_primary_id
		self._current_primary_address = new_primary_address
		self._is_electing = False

		if new_primary_id == self._node_id:
			await self.promote_self()
			return

		self._role = "replica"
		self._write_gate.block_writes("minority partition, refusing writes")
		self._dispatcher.set_writes_enabled(False)
		self._dispatcher.set_replicator(None)

		if self._failure_detector is not None:
			peer = self._peers.get(new_primary_id)
			detector_address = peer.grpc_address if peer is not None else new_primary_address
			await self._failure_detector.update_peer(new_primary_id, detector_address)

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

		if self._election.node_id > request.sender_id:
			await self._election.notify_higher_priority_peer(request.sender_id)
			return election_pb2.ElectionAck(acknowledged=True)

		return election_pb2.ElectionAck(acknowledged=False)

	async def Coordinator(
		self,
		request: election_pb2.CoordinatorMessage,
		context: grpc.aio.ServicerContext,
	) -> election_pb2.CoordinatorAck:
		del context
		await self._election.accept_coordinator(
			request.new_primary_id,
			request.new_primary_address,
		)
		return election_pb2.CoordinatorAck(acknowledged=True)

