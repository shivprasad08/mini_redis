"""
Cluster-wide quorum helpers for mini-redis.

This module keeps the majority arithmetic in one place and provides a
small write-admission gate that nodes can use to reject client writes
whenever they lose contact with cluster majority. Reads intentionally
remain available during partitions, which is a deliberate CAP trade-off
for the course demo: availability for reads over freshness.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Literal, Mapping, Optional

import grpc

from miniredis.health.proto import health_pb2
from miniredis.health.proto import health_pb2_grpc

logger = logging.getLogger(__name__)

MajorityState = Literal["majority", "minority"]
StateChangeCallback = Callable[[MajorityState], object]


def majority_count(total_cluster_size: int) -> int:
    """Return the number of nodes required for a strict cluster majority."""
    if total_cluster_size <= 0:
        raise ValueError("total_cluster_size must be positive")
    return total_cluster_size // 2 + 1


@dataclass
class WriteAdmissionGate:
    """Node-local gate for accepting or rejecting client writes."""

    can_serve_writes: bool = True
    reason: str = ""

    def block_writes(self, reason: str) -> None:
        self.can_serve_writes = False
        self.reason = reason

    def allow_writes(self) -> None:
        self.can_serve_writes = True
        self.reason = ""


class ClusterMajorityMonitor:
    """Background monitor that keeps a node's write gate in sync with reachability.

    The monitor probes every known peer via the existing HealthService RPC.
    If the node can no longer reach a majority of the fixed cluster, the gate
    flips to minority mode and client writes are rejected until majority is
    restored.
    """

    def __init__(
        self,
        node_id: str,
        peer_health_addresses: Mapping[str, str],
        total_cluster_size: int,
        gate: WriteAdmissionGate,
        heartbeat_interval_seconds: float = 1.0,
        heartbeat_timeout_seconds: float = 0.5,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
    ) -> None:
        self._node_id = node_id
        self._peer_health_addresses = dict(peer_health_addresses)
        self._total_cluster_size = total_cluster_size
        self._gate = gate
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._rpc_filter = rpc_filter

        self._stubs: dict[str, health_pb2_grpc.HealthServiceStub] = {}
        self._channels: list[grpc.aio.Channel] = []
        for peer_id, peer_address in self._peer_health_addresses.items():
            if peer_id == self._node_id:
                continue
            channel = grpc.aio.insecure_channel(peer_address)
            self._channels.append(channel)
            self._stubs[peer_id] = health_pb2_grpc.HealthServiceStub(channel)

        self._state: MajorityState = "majority"
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._callbacks: list[StateChangeCallback] = []

    @property
    def state(self) -> MajorityState:
        return self._state

    def on_state_change(self, callback: StateChangeCallback) -> None:
        self._callbacks.append(callback)

    async def start(self) -> None:
        if self._task is not None:
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        for channel in self._channels:
            await channel.close()

    async def probe_majority(self) -> bool:
        """Probe peers once and update the gate based on current reachability."""
        reachable_peers = 0
        tasks = [asyncio.create_task(self._probe_peer(peer_id, stub)) for peer_id, stub in self._stubs.items()]
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

        has_majority = (reachable_peers + 1) >= majority_count(self._total_cluster_size)
        await self._set_state("majority" if has_majority else "minority")
        return has_majority

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                await self.probe_majority()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._heartbeat_interval_seconds
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Majority monitor for %s stopped unexpectedly", self._node_id)

    async def _probe_peer(self, peer_id: str, stub: health_pb2_grpc.HealthServiceStub) -> bool:
        if self._rpc_filter is not None and not self._rpc_filter("Heartbeat", self._peer_health_addresses[peer_id]):
            return False

        request = health_pb2.Ping(sender_id=self._node_id, timestamp_ms=0)
        try:
            await asyncio.wait_for(stub.Heartbeat(request), timeout=self._heartbeat_timeout_seconds)
            return True
        except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError):
            return False

    async def _set_state(self, new_state: MajorityState) -> None:
        if new_state == self._state:
            return

        self._state = new_state
        if new_state == "majority":
            self._gate.allow_writes()
        else:
            self._gate.block_writes("minority partition, refusing writes")

        for callback in self._callbacks:
            try:
                result = callback(new_state)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Majority state callback failed for %s", self._node_id)