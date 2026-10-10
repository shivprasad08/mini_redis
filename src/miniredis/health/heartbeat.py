"""
Heartbeat-based liveness monitoring for mini-redis nodes.

The same gRPC server that hosts replication also hosts health checks so
nodes do not need an extra transport or port for internal liveness
traffic. HealthServicer answers heartbeat RPCs on every node, while
FailureDetector runs on nodes that need to monitor a peer.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Callable, Literal, Optional

import grpc

from miniredis.health.proto import health_pb2
from miniredis.health.proto import health_pb2_grpc
from miniredis.logical_clock import LamportClock, log_lamport_event

logger = logging.getLogger(__name__)

NodeState = Literal["alive", "suspected", "dead"]
StateChangeCallback = Callable[[str, NodeState, NodeState], object]


def _now_ms() -> int:
	return int(time.time() * 1000)


class HealthServicer(health_pb2_grpc.HealthServiceServicer):
	"""Responds to heartbeats with this node's live identity."""

	def __init__(
		self,
		node_id: str,
		role: Literal["primary", "replica"],
		lamport_clock: Optional[LamportClock] = None,
	) -> None:
		self._node_id = node_id
		self._role = role
		self._lamport_clock = lamport_clock or LamportClock()

	async def Heartbeat(
		self,
		request: health_pb2.Ping,
		context: grpc.aio.ServicerContext,
	) -> health_pb2.Pong:
		del context
		ts = await self._lamport_clock.receive(request.lamport_timestamp)
		log_lamport_event(
			timestamp=ts,
			node_id=self._node_id,
			event_type="heartbeat_recv",
			message_id=request.sender_id or "unknown",
			description="received heartbeat",
		)
		response_ts = await self._lamport_clock.tick()
		log_lamport_event(
			timestamp=response_ts,
			node_id=self._node_id,
			event_type="heartbeat_send",
			message_id=request.sender_id or "unknown",
			description="sent heartbeat response",
		)
		return health_pb2.Pong(
			responder_id=self._node_id,
			role=self._role,
			timestamp_ms=_now_ms(),
			lamport_timestamp=response_ts,
		)


class FailureDetector:
	"""Monitors a peer node by id and gRPC address.

	The detector is intentionally generic: Module 9 uses it for replica
	watches on the primary, and Module 10 can reuse it for peer-to-peer
	failure tracking during election.
	"""

	def __init__(
		self,
		peer_node_id: str,
		peer_address: str,
		heartbeat_interval_seconds: float = 1.0,
		heartbeat_timeout_seconds: float = 0.5,
		failure_threshold: int = 3,
		sender_id: str = "",
		rpc_filter: Optional[Callable[[str, str], bool]] = None,
		lamport_clock: Optional[LamportClock] = None,
	) -> None:
		if failure_threshold <= 0:
			raise ValueError("failure_threshold must be positive")

		self._peer_node_id = peer_node_id
		self._peer_address = peer_address
		self._heartbeat_interval_seconds = heartbeat_interval_seconds
		self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
		self._failure_threshold = failure_threshold
		self._sender_id = sender_id
		self._rpc_filter = rpc_filter
		self._lamport_clock = lamport_clock or LamportClock()

		self._channel = grpc.aio.insecure_channel(peer_address)
		self._stub = health_pb2_grpc.HealthServiceStub(self._channel)
		self._state: NodeState = "alive"
		self._missed_heartbeats = 0
		self._callbacks: list[StateChangeCallback] = []
		self._task: Optional[asyncio.Task[None]] = None
		self._stop_event = asyncio.Event()

	@property
	def peer_node_id(self) -> str:
		return self._peer_node_id

	@property
	def peer_address(self) -> str:
		return self._peer_address

	@property
	def state(self) -> NodeState:
		return self._state

	def on_state_change(self, callback: StateChangeCallback) -> None:
		"""Register a callback fired only when the peer state changes."""
		self._callbacks.append(callback)

	async def update_peer(self, peer_node_id: str, peer_address: str) -> None:
		"""Retarget the detector at a different peer.

		This is used when a coordinator announces a new primary. The
		detector keeps running, but future heartbeats are sent to the
		replacement peer instead of the dead one.
		"""
		if peer_node_id == self._peer_node_id and peer_address == self._peer_address:
			return

		old_channel = self._channel

		self._peer_node_id = peer_node_id
		self._peer_address = peer_address
		self._missed_heartbeats = 0
		self._channel = grpc.aio.insecure_channel(peer_address)
		self._stub = health_pb2_grpc.HealthServiceStub(self._channel)
		await self._set_state("alive")

		await old_channel.close()

	async def start(self) -> None:
		"""Start the background heartbeat loop."""
		if self._task is not None:
			return

		self._stop_event.clear()
		self._task = asyncio.create_task(self._run())
		await asyncio.sleep(0)

	async def stop(self) -> None:
		"""Stop the background loop and release the gRPC channel."""
		self._stop_event.set()

		if self._task is not None:
			self._task.cancel()
			try:
				await self._task
			except asyncio.CancelledError:
				pass
			finally:
				self._task = None

		await self._channel.close()

	async def _run(self) -> None:
		try:
			while not self._stop_event.is_set():
				await self._probe_once()
				try:
					await asyncio.wait_for(
						self._stop_event.wait(), timeout=self._heartbeat_interval_seconds
					)
				except asyncio.TimeoutError:
					continue
		except asyncio.CancelledError:
			raise
		except Exception:
			logger.exception("Failure detector for %s stopped unexpectedly", self._peer_node_id)

	async def _probe_once(self) -> None:
		request_ts = await self._lamport_clock.tick()
		log_lamport_event(
			timestamp=request_ts,
			node_id=self._sender_id or "unknown",
			event_type="heartbeat_send",
			message_id=self._peer_node_id,
			description="sent heartbeat",
		)
		request = health_pb2.Ping(
			sender_id=self._sender_id,
			timestamp_ms=_now_ms(),
			lamport_timestamp=request_ts,
		)
		try:
			if self._rpc_filter is not None and not self._rpc_filter("Heartbeat", self._peer_address):
				raise ConnectionError("partitioned")
			response = await asyncio.wait_for(
				self._stub.Heartbeat(request), timeout=self._heartbeat_timeout_seconds
			)
			recv_ts = await self._lamport_clock.receive(response.lamport_timestamp)
			log_lamport_event(
				timestamp=recv_ts,
				node_id=self._sender_id or "unknown",
				event_type="heartbeat_recv",
				message_id=response.responder_id,
				description="received heartbeat response",
			)
			self._missed_heartbeats = 0
			await self._set_state("alive")
		except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError):
			self._missed_heartbeats += 1
			new_state: NodeState = (
				"dead" if self._missed_heartbeats >= self._failure_threshold else "suspected"
			)
			await self._set_state(new_state)

	async def _set_state(self, new_state: NodeState) -> None:
		old_state = self._state
		if new_state == old_state:
			return

		self._state = new_state
		for callback in self._callbacks:
			try:
				result = callback(self._peer_node_id, old_state, new_state)
				if inspect.isawaitable(result):
					await result
			except Exception:
				logger.exception(
					"State change callback failed for peer %s (%s -> %s)",
					self._peer_node_id,
					old_state,
					new_state,
				)
