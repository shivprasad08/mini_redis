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
from typing import Callable, Literal, Optional

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.storage import Store
from miniredis.replication.proto import replication_pb2
from miniredis.replication.proto import replication_pb2_grpc

logger = logging.getLogger(__name__)


class ReplicaServicer(replication_pb2_grpc.ReplicationServiceServicer):
    """gRPC Servicer for the replica role."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._applied_sequence = 0

    async def ReplicateWrite(
        self,
        request: replication_pb2.WriteOp,
        context: grpc.aio.ServicerContext,
    ) -> replication_pb2.Ack:
        # Check sequence
        if request.sequence <= self._applied_sequence:
            logger.warning(
                f"Duplicate or out-of-order write detected. "
                f"Got sequence {request.sequence}, but already at {self._applied_sequence}"
            )
        elif request.sequence > self._applied_sequence + 1:
            logger.warning(
                f"Sequence gap detected. "
                f"Got sequence {request.sequence}, expected {self._applied_sequence + 1}"
            )

        op_type = request.op_type.upper()
        key = request.key.encode("utf-8")
        
        if op_type == "SET":
            ttl = request.ttl_seconds if request.ttl_seconds > 0 else None
            self._store.set(key, request.value, ttl_seconds=ttl)
        elif op_type == "DEL":
            self._store.delete(key)
        elif op_type == "EXPIRE":
            self._store.expire(key, request.ttl_seconds)
        else:
            logger.error(f"Unknown operation type received: {op_type}")

        self._applied_sequence = max(self._applied_sequence, request.sequence)

        return replication_pb2.Ack(
            success=True,
            applied_sequence=self._applied_sequence
        )


class Replicator:
    """Primary node replicator that propagates writes to replicas."""

    def __init__(
        self,
        targets: list[str],
        total_cluster_size: int,
        write_mode: Literal["async", "quorum"] = "async",
        write_timeout_seconds: float = 2.0,
        write_gate: Optional[WriteAdmissionGate] = None,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
    ) -> None:
        """
        targets: list of "host:port" strings for replica gRPC servers.
        """
        if write_mode not in {"async", "quorum"}:
            raise ValueError(f"Unsupported write mode: {write_mode}")

        self._sequence = 0
        self._targets = targets
        self._total_cluster_size = total_cluster_size
        self._write_mode = write_mode
        self._write_timeout_seconds = write_timeout_seconds
        self._write_gate = write_gate
        self._rpc_filter = rpc_filter
        self._channels: list[grpc.aio.Channel] = []
        self._stubs: list[replication_pb2_grpc.ReplicationServiceStub] = []

        for target in targets:
            channel = grpc.aio.insecure_channel(target)
            self._channels.append(channel)
            self._stubs.append(replication_pb2_grpc.ReplicationServiceStub(channel))

    async def propagate(
        self, op_type: str, key: bytes, value: bytes = b"", ttl_seconds: int = 0
    ) -> None:
        """
        Increment sequence and push this write to all replicas.
        Fire-and-forget: doesn't block the caller waiting for acks.
        """
        if not self._stubs:
            return

        self._sequence += 1
        req = self._build_write_op(op_type, key, value, ttl_seconds)

        for index, stub in enumerate(self._stubs):
            asyncio.create_task(
                self._send_to_replica(stub, req, self._targets[index])
            )

    async def propagate_quorum(
        self, op_type: str, key: bytes, value: bytes = b"", ttl_seconds: int = 0
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
        req = self._build_write_op(op_type, key, value, ttl_seconds)

        if not self._stubs:
            return True

        required_replica_acks = majority_count(self._total_cluster_size) - 1
        if required_replica_acks <= 0:
            return True

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
                    if self._write_gate is not None:
                        self._write_gate.allow_writes()
                    quorum_reached = True
                    return True

                remaining = len(tasks) - completed
                if successes + remaining < required_replica_acks:
                    if self._write_gate is not None:
                        self._write_gate.block_writes("minority partition, refusing writes")
                    return False

            return successes >= required_replica_acks
        finally:
            if not quorum_reached:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if self._write_gate is not None:
                    self._write_gate.block_writes("minority partition, refusing writes")

    async def propagate_write(
        self, op_type: str, key: bytes, value: bytes = b"", ttl_seconds: int = 0
    ) -> bool:
        """Dispatch the write path selected at construction time.

        Dispatcher code calls this single entry point and does not need to know
        whether the primary is running in async or quorum mode.
        """
        if self._write_mode == "quorum":
            return await self.propagate_quorum(op_type, key, value, ttl_seconds)

        await self.propagate(op_type, key, value, ttl_seconds)
        return True

    def _build_write_op(
        self,
        op_type: str,
        key: bytes,
        value: bytes,
        ttl_seconds: int,
    ) -> replication_pb2.WriteOp:
        return replication_pb2.WriteOp(
            op_type=op_type,
            key=key.decode("utf-8", errors="replace"),
            value=value,
            ttl_seconds=ttl_seconds,
            sequence=self._sequence,
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
                stub.ReplicateWrite(req), timeout=self._write_timeout_seconds
            )
            logger.debug(f"Replica {target} acked sequence {ack.applied_sequence}")
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
                stub.ReplicateWrite(req), timeout=self._write_timeout_seconds
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

    async def aclose(self) -> None:
        """Close every replica channel this Replicator opened."""
        channels = list(self._channels)
        self._channels.clear()
        self._stubs.clear()
        if channels:
            await asyncio.gather(*(channel.close() for channel in channels), return_exceptions=True)    
