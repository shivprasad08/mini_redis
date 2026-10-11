"""
gRPC MutexService servicer implementation for M16.
"""

from __future__ import annotations

import logging
from typing import Optional

import grpc

from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.mutex.manager import BaseLockManager, CentralizedLockManager, RicartAgrawalaManager
from miniredis.mutex.proto import mutex_pb2, mutex_pb2_grpc

logger = logging.getLogger(__name__)


class MutexServicer(mutex_pb2_grpc.MutexServiceServicer):
    """gRPC servicer for distributed mutual exclusion and centralized locking."""

    def __init__(
        self,
        manager: BaseLockManager,
        lamport_clock: Optional[LamportClock] = None,
    ) -> None:
        self._manager = manager
        self._lamport_clock = lamport_clock or getattr(manager, "lamport_clock", LamportClock())

    @property
    def manager(self) -> BaseLockManager:
        return self._manager

    def set_manager(self, manager: BaseLockManager) -> None:
        self._manager = manager

    async def RequestLock(
        self,
        request: mutex_pb2.LockRequest,
        context: grpc.aio.ServicerContext,
    ) -> mutex_pb2.LockResponse:
        del context
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        node_id = getattr(self._manager, "node_id", "local")

        log_lamport_event(
            timestamp=recv_ts,
            node_id=node_id,
            event_type="mutex_request_lock_recv",
            message_id=request.requesting_node_id,
            description=f"RequestLock for {request.resource_id} (centralized={request.centralized})",
        )

        if request.centralized and isinstance(self._manager, CentralizedLockManager):
            try:
                token = await self._manager.acquire(
                    resource_id=request.resource_id,
                    client_id=request.client_id,
                    timeout_seconds=5.0,
                )
                resp_ts = await self._lamport_clock.tick()
                return mutex_pb2.LockResponse(
                    granted=True,
                    deferred=False,
                    responder_node_id=node_id,
                    request_id=request.request_id,
                    lamport_timestamp=resp_ts,
                    fencing_token=str(token),
                )
            except Exception as exc:
                resp_ts = await self._lamport_clock.tick()
                return mutex_pb2.LockResponse(
                    granted=False,
                    deferred=False,
                    responder_node_id=node_id,
                    request_id=request.request_id,
                    lamport_timestamp=resp_ts,
                    error_message=str(exc),
                )

        if isinstance(self._manager, RicartAgrawalaManager):
            granted, deferred, err = await self._manager.handle_request_lock(
                requesting_node_id=request.requesting_node_id,
                resource_id=request.resource_id,
                request_id=request.request_id,
                election_epoch=request.election_epoch,
                lamport_timestamp=request.lamport_timestamp,
            )
            resp_ts = await self._lamport_clock.tick()
            return mutex_pb2.LockResponse(
                granted=granted,
                deferred=deferred,
                responder_node_id=node_id,
                request_id=request.request_id,
                lamport_timestamp=resp_ts,
                error_message=err,
            )

        resp_ts = await self._lamport_clock.tick()
        return mutex_pb2.LockResponse(
            granted=False,
            deferred=False,
            responder_node_id=node_id,
            request_id=request.request_id,
            lamport_timestamp=resp_ts,
            error_message="Unsupported lock manager type",
        )

    async def RequestReply(
        self,
        request: mutex_pb2.ReplyMessage,
        context: grpc.aio.ServicerContext,
    ) -> mutex_pb2.ReplyAck:
        del context
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        node_id = getattr(self._manager, "node_id", "local")

        log_lamport_event(
            timestamp=recv_ts,
            node_id=node_id,
            event_type="mutex_request_reply_recv",
            message_id=request.responder_node_id,
            description=f"RequestReply for {request.resource_id}",
        )

        if isinstance(self._manager, RicartAgrawalaManager):
            await self._manager.handle_request_reply(
                responder_node_id=request.responder_node_id,
                resource_id=request.resource_id,
                request_id=request.request_id,
                granted=request.granted,
                lamport_timestamp=request.lamport_timestamp,
            )

        ack_ts = await self._lamport_clock.tick()
        return mutex_pb2.ReplyAck(
            acknowledged=True,
            lamport_timestamp=ack_ts,
        )

    async def ReleaseLock(
        self,
        request: mutex_pb2.ReleaseRequest,
        context: grpc.aio.ServicerContext,
    ) -> mutex_pb2.ReleaseResponse:
        del context
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        node_id = getattr(self._manager, "node_id", "local")

        log_lamport_event(
            timestamp=recv_ts,
            node_id=node_id,
            event_type="mutex_release_lock_recv",
            message_id=request.requesting_node_id,
            description=f"ReleaseLock for {request.resource_id}",
        )

        try:
            await self._manager.release(
                resource_id=request.resource_id,
                token=request.fencing_token or None,
                client_id=request.client_id,
            )
            resp_ts = await self._lamport_clock.tick()
            return mutex_pb2.ReleaseResponse(
                success=True,
                lamport_timestamp=resp_ts,
            )
        except Exception as exc:
            resp_ts = await self._lamport_clock.tick()
            return mutex_pb2.ReleaseResponse(
                success=False,
                error_message=str(exc),
                lamport_timestamp=resp_ts,
            )

    async def GetLockStatus(
        self,
        request: mutex_pb2.LockStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> mutex_pb2.LockStatusResponse:
        del context
        await self._lamport_clock.receive(request.lamport_timestamp)
        status = self._manager.get_status(request.resource_id)
        resp_ts = await self._lamport_clock.tick()
        return mutex_pb2.LockStatusResponse(
            resource_id=request.resource_id,
            state=status.get("state", "RELEASED"),
            current_holder=status.get("current_holder", ""),
            fencing_token=status.get("fencing_token", ""),
            lamport_timestamp=resp_ts,
        )
