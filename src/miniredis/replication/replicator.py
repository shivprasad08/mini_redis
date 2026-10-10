"""
Internal node-to-node replication module for mini-redis.

This module uses gRPC for propagating writes from a primary node to
its replicas. This is distinct from the client-facing TCP protocol.

Roles:
- ReplicaServicer: Runs on a replica node, receiving writes and applying
    them to the local Store.
- Replicator: Runs on a primary node, holding gRPC channels to all
    replicas and fanning out writes either asynchronously or with a
    quorum requirement.

Async Mode Thesis Note:
The Replicator's legacy ``propagate`` path remains fire-and-forget.
Quorum mode adds a synchronous write path that waits for enough
replica acknowledgements before the primary is allowed to apply the
write locally.
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Callable, Literal, Optional

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.storage import Store
from miniredis.replication.proto import replication_pb2
from miniredis.replication.proto import replication_pb2_grpc

logger = logging.getLogger(__name__)


class ReplicaServicer(replication_pb2_grpc.ReplicationServiceServicer):
    """gRPC Servicer for the replica role."""

    def __init__(
        self,
        store: Store,
        node_id: str = "",
        lamport_clock: Optional[LamportClock] = None,
    ) -> None:
        self._store = store
        self._current_epoch = 0
        self._applied_sequence = 0
        self._node_id = node_id or "replica"
        self._lamport_clock = lamport_clock or LamportClock()

    async def ReplicateWrite(
        self,
        request: replication_pb2.WriteOp,
        context: grpc.aio.ServicerContext,
    ) -> replication_pb2.Ack:
        logger.info(
            f"[gRPC IN] ReplicateWrite received — op={request.op_type} "
            f"key={request.key!r} seq={request.sequence} from peer={context.peer()}"
        )
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="replication_recv",
            message_id=f"{request.epoch}:{request.sequence}",
            description=f"received {request.op_type}",
        )

        if request.epoch < self._current_epoch:
            logger.warning(
                "Rejecting obsolete replication epoch %s; current epoch is %s",
                request.epoch,
                self._current_epoch,
            )
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=self._current_epoch,
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        if request.epoch > self._current_epoch:
            # A coordinator announcement establishes the new epoch before
            # writes arrive. Reset only the per-epoch sequence, never the epoch.
            self._current_epoch = request.epoch
            self._applied_sequence = 0

        if request.sequence <= self._applied_sequence:
            logger.warning(
                "Rejecting duplicate replication write: epoch=%s sequence=%s; "
                "already applied through sequence %s",
                request.epoch,
                request.sequence,
                self._applied_sequence,
            )
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=self._current_epoch,
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        if request.sequence != self._applied_sequence + 1:
            logger.warning(
                "Rejecting replication sequence gap: epoch=%s got=%s expected=%s",
                request.epoch,
                request.sequence,
                self._applied_sequence + 1,
            )
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=self._current_epoch,
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        op_type = request.op_type.upper()
        key = request.key.encode("utf-8")
        
        expires_at = request.expiry_deadline_ms / 1000.0 if request.expiry_deadline_ms > 0 else None

        if op_type == "SET":
            ttl = request.ttl_seconds if request.ttl_seconds > 0 else None
            self._store.set(key, request.value, ttl_seconds=ttl, expires_at=expires_at)
        elif op_type == "DEL":
            self._store.delete(key)
        elif op_type == "EXPIRE":
            if expires_at is not None:
                self._store.expire_at(key, expires_at)
            else:
                self._store.expire(key, request.ttl_seconds)
        else:
            logger.error(f"Unknown operation type received: {op_type}")

        self._applied_sequence = request.sequence

        logger.info(
            f"[gRPC OUT] Sending Ack — success=True epoch={self._current_epoch} "
            f"applied_sequence={self._applied_sequence}"
        )

        ack_ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=ack_ts,
            node_id=self._node_id,
            event_type="replication_ack_send",
            message_id=f"{self._current_epoch}:{self._applied_sequence}",
            description="sent replication ack",
        )
        return replication_pb2.Ack(
            success=True,
            applied_sequence=self._applied_sequence,
            epoch=self._current_epoch,
            lamport_timestamp=ack_ts,
        )


class Replicator:
    """Primary node replicator that propagates writes to replicas."""

    def __init__(
        self,
        targets: list[str],
        total_cluster_size: int,
        write_mode: Literal["async", "quorum"] = "async",
        write_timeout_seconds: float = 2.0,
        simulate_latency_ms: float = 0.0,
        write_gate: Optional[WriteAdmissionGate] = None,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
        epoch: int = 0,
        node_id: str = "",
        lamport_clock: Optional[LamportClock] = None,
    ) -> None:
        """
        targets: list of "host:port" strings for replica gRPC servers.
        """
        if write_mode not in {"async", "quorum"}:
            raise ValueError(f"Unsupported write mode: {write_mode}")
        if not math.isfinite(simulate_latency_ms) or simulate_latency_ms < 0:
            raise ValueError("simulate_latency_ms must be a finite non-negative value")

        self._sequence = 0
        self._epoch = epoch
        self._targets = targets
        self._total_cluster_size = total_cluster_size
        self._write_mode = write_mode
        self._write_timeout_seconds = write_timeout_seconds
        self._simulate_latency_seconds = simulate_latency_ms / 1000.0
        self._write_gate = write_gate
        self._rpc_filter = rpc_filter
        self._node_id = node_id or "primary"
        self._lamport_clock = lamport_clock or LamportClock()
        self._channels: list[grpc.aio.Channel] = []
        self._stubs: list[replication_pb2_grpc.ReplicationServiceStub] = []

        for target in targets:
            channel = grpc.aio.insecure_channel(target)
            self._channels.append(channel)
            self._stubs.append(replication_pb2_grpc.ReplicationServiceStub(channel))

    async def propagate(
        self,
        op_type: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> None:
        """
        Increment sequence and push this write to all replicas.
        Fire-and-forget: doesn't block the caller waiting for acks.
        """
        if not self._stubs:
            return

        self._sequence += 1
        req = await self._build_write_op(op_type, key, value, ttl_seconds, expiry_deadline_ms)

        logger.info(
            f"[gRPC OUT] ReplicateWrite (async, fire-and-forget) — "
            f"op={op_type} key={key.decode(errors='replace')!r} seq={self._sequence} "
            f"→ {len(self._stubs)} replica(s): {self._targets}"
        )

        for index, stub in enumerate(self._stubs):
            asyncio.create_task(
                self._send_to_replica(stub, req, self._targets[index])
            )

    async def propagate_quorum(
        self,
        op_type: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> bool:
        """Replicate a write and wait for a cluster majority.

        Semantics:
        - The primary sends the write to every replica concurrently.
        - A replica response counts only when it returns Ack(success=True)
          before the per-call timeout expires.
        - The primary's own local apply is intentionally deferred until after
          this method returns True, so a failed quorum leaves the local Store
          unchanged and avoids rollback bookkeeping.

        Majority choice:
        - We treat the cluster size as ``len(replicas) + 1`` to include the
          primary node.
        - The primary's local apply supplies one vote, so this method waits
          for ``floor(cluster_size / 2)`` replica ACKs before returning True.
        """
        if self._write_gate is not None and not self._write_gate.can_serve_writes:
            return False

        self._sequence += 1
        req = await self._build_write_op(op_type, key, value, ttl_seconds, expiry_deadline_ms)

        if not self._stubs:
            return True

        required_replica_acks = majority_count(self._total_cluster_size) - 1
        if required_replica_acks <= 0:
            return True

        logger.info(
            f"[gRPC OUT] ReplicateWrite (quorum) — op={op_type} "
            f"key={key.decode(errors='replace')!r} seq={self._sequence} "
            f"→ {len(self._stubs)} replica(s), need {required_replica_acks} ack(s) for majority"
        )

        tasks = [
            asyncio.create_task(self._replica_ack(stub, req, self._targets[index]))
            for index, stub in enumerate(self._stubs)
        ]

        successes = 0
        completed = 0
        quorum_reached = False
        try:
            for task in asyncio.as_completed(tasks):
                if await task:
                    successes += 1
                completed += 1

                if successes >= required_replica_acks:
                    quorum_reached = True
                    return True

                remaining = len(tasks) - completed
                if successes + remaining < required_replica_acks:
                    return False

            return successes >= required_replica_acks
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            if not quorum_reached:
                # With zero successful acknowledgements, no replica has
                # evidence of this request. Releasing the reservation lets a
                # retry after a fully blocked partition reuse the next
                # contiguous sequence. Partial-ACK failures keep the sequence
                # consumed; silently rewriting those would risk divergence.
                if successes == 0 and self._sequence == req.sequence:
                    self._sequence -= 1

    async def propagate_write(
        self,
        op_type: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> bool:
        """Dispatch the write path selected at construction time.

        Dispatcher code calls this single entry point and does not need to know
        whether the primary is running in async or quorum mode.
        """
        if self._write_mode == "quorum":
            return await self.propagate_quorum(op_type, key, value, ttl_seconds, expiry_deadline_ms)

        await self.propagate(op_type, key, value, ttl_seconds, expiry_deadline_ms)
        return True

    async def _build_write_op(
        self,
        op_type: str,
        key: bytes,
        value: bytes,
        ttl_seconds: int,
        expiry_deadline_ms: int,
    ) -> replication_pb2.WriteOp:
        ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=ts,
            node_id=self._node_id,
            event_type="replication_send",
            message_id=f"{self._epoch}:{self._sequence}",
            description=f"sent {op_type}",
        )
        return replication_pb2.WriteOp(
            op_type=op_type,
            key=key.decode("utf-8", errors="replace"),
            value=value,
            ttl_seconds=ttl_seconds,
            sequence=self._sequence,
            epoch=self._epoch,
            lamport_timestamp=ts,
            expiry_deadline_ms=expiry_deadline_ms,
        )

    async def _send_to_replica(
        self,
        stub: replication_pb2_grpc.ReplicationServiceStub,
        req: replication_pb2.WriteOp,
        target: str,
    ) -> None:
        try:
            if self._rpc_filter is not None and not self._rpc_filter("ReplicateWrite", target):
                raise ConnectionError("partitioned")
            ack = await asyncio.wait_for(
                self._replicate_with_simulated_latency(stub, req),
                timeout=self._write_timeout_seconds,
            )
            logger.info(f"[gRPC IN] Ack received from {target} — seq={ack.applied_sequence}")
        except asyncio.TimeoutError:
            logger.warning(
                f"Failed to replicate sequence {req.sequence} to {target}: timed out"
            )
        except grpc.aio.AioRpcError as e:
            logger.warning(
                f"Failed to replicate sequence {req.sequence} to {target}: {e.details()}"
            )
        except Exception as e:
            logger.warning(
                f"Unexpected error replicating to {target}: {e}"
            )

    async def _record_ack(self, ack: replication_pb2.Ack) -> None:
        ts = await self._lamport_clock.receive(ack.lamport_timestamp)
        log_lamport_event(
            timestamp=ts,
            node_id=self._node_id,
            event_type="replication_ack_recv",
            message_id=f"{ack.epoch}:{ack.applied_sequence}",
            description="received replication ack",
        )

    async def _replica_ack(
        self,
        stub: replication_pb2_grpc.ReplicationServiceStub,
        req: replication_pb2.WriteOp,
        target: str,
    ) -> bool:
        try:
            if self._rpc_filter is not None and not self._rpc_filter("ReplicateWrite", target):
                raise ConnectionError("partitioned")
            ack = await asyncio.wait_for(
                self._replicate_with_simulated_latency(stub, req),
                timeout=self._write_timeout_seconds,
            )
            logger.debug(f"Replica {target} acked sequence {ack.applied_sequence}")
            return bool(ack.success)
        except asyncio.TimeoutError:
            logger.warning(
                f"Failed to replicate sequence {req.sequence} to {target}: timed out"
            )
        except grpc.aio.AioRpcError as e:
            logger.warning(
                f"Failed to replicate sequence {req.sequence} to {target}: {e.details()}"
            )
        except Exception as e:
            logger.warning(f"Unexpected error replicating to {target}: {e}")

        return False

    async def _replicate_with_simulated_latency(
        self,
        stub: replication_pb2_grpc.ReplicationServiceStub,
        req: replication_pb2.WriteOp,
    ) -> replication_pb2.Ack:
        if self._simulate_latency_seconds > 0:
            await asyncio.sleep(self._simulate_latency_seconds)
        ack = await stub.ReplicateWrite(req)
        await self._record_ack(ack)
        return ack

    async def aclose(self) -> None:
        """Close every replica channel this Replicator opened."""
        channels = list(self._channels)
        self._channels.clear()
        self._stubs.clear()
        if channels:
            await asyncio.gather(*(channel.close() for channel in channels), return_exceptions=True)
