"""Cristian-style gRPC time service and replica synchronizer."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import grpc

from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.time_sync.clock import SyncSample, SynchronizedClock
from miniredis.time_sync.proto import time_pb2, time_pb2_grpc

logger = logging.getLogger(__name__)


class TimeServicer(time_pb2_grpc.TimeServiceServicer):
    def __init__(
        self,
        *,
        node_id: str,
        clock: SynchronizedClock,
        lamport_clock: Optional[LamportClock] = None,
    ) -> None:
        self._node_id = node_id
        self._clock = clock
        self._lamport_clock = lamport_clock or LamportClock()

    async def GetTime(
        self,
        request: time_pb2.TimeRequest,
        context: grpc.aio.ServicerContext,
    ) -> time_pb2.TimeResponse:
        del context
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="time_recv",
            message_id=request.requester_id or "unknown",
            description="received time sync request",
        )
        send_ts = await self._lamport_clock.tick()
        return time_pb2.TimeResponse(
            responder_id=self._node_id,
            server_time_ms=self._clock.corrected_time_ms(),
            lamport_timestamp=send_ts,
        )


class TimeSynchronizer:
    """Replica-side Cristian synchronizer against the current primary."""

    def __init__(
        self,
        *,
        node_id: str,
        peer_address: str,
        clock: SynchronizedClock,
        interval_seconds: float = 1.0,
        timeout_seconds: float = 0.5,
        lamport_clock: Optional[LamportClock] = None,
    ) -> None:
        self._node_id = node_id
        self._peer_address = peer_address
        self._clock = clock
        self._interval_seconds = interval_seconds
        self._timeout_seconds = timeout_seconds
        self._lamport_clock = lamport_clock or LamportClock()
        self._channel = grpc.aio.insecure_channel(peer_address)
        self._stub = time_pb2_grpc.TimeServiceStub(self._channel)
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

    @property
    def peer_address(self) -> str:
        return self._peer_address

    @property
    def last_sample(self) -> Optional[SyncSample]:
        return self._clock.last_sample

    async def update_peer(self, peer_address: str) -> None:
        if peer_address == self._peer_address:
            return
        old_channel = self._channel
        self._peer_address = peer_address
        self._channel = grpc.aio.insecure_channel(peer_address)
        self._stub = time_pb2_grpc.TimeServiceStub(self._channel)
        await old_channel.close()

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
        await self._channel.close()

    async def sync_once(self) -> SyncSample:
        send_ms = self._clock.raw_time_ms()
        request_ts = await self._lamport_clock.tick()
        request = time_pb2.TimeRequest(
            requester_id=self._node_id,
            client_send_time_ms=send_ms,
            lamport_timestamp=request_ts,
        )
        try:
            response = await asyncio.wait_for(
                self._stub.GetTime(request), timeout=self._timeout_seconds
            )
            receive_ms = self._clock.raw_time_ms()
            await self._lamport_clock.receive(response.lamport_timestamp)
            return self._clock.apply_sample(
                client_send_ms=send_ms,
                server_time_ms=response.server_time_ms,
                client_receive_ms=receive_ms,
            )
        except (asyncio.TimeoutError, grpc.aio.AioRpcError, OSError):
            logger.warning("Time synchronization with %s failed", self._peer_address)
            return self._clock.mark_unsynchronized()

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                await self.sync_once()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Time synchronizer for %s stopped unexpectedly", self._node_id)
