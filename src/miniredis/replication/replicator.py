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

M15 additions:
- ReplicaServicer accepts an optional ReplicationProgress tracker and
  records every successfully applied write into it.
- Replicator accepts an optional ReplicationProgress tracker for the
  primary node and records applied writes after quorum/async success.
- A new GetProgress RPC is handled by ReplicaServicer so the primary
  can query each replica's progress.
- Replicator.collect_progress() queries all replicas concurrently and
  returns their ProgressResponse objects.

Leadership fencing (safe rejoin):
- ReplicaServicer fences writes against the highest epoch this node has
  accepted (from replicated writes *or* a coordinator announcement), so a
  deposed primary cannot keep mutating replicas that already follow a newer
  leader. A node that is itself primary never applies replicated writes.
- Rejections carry the newer epoch back in the Ack; Replicator reports it
  through ``on_higher_epoch`` so the election layer can step down.
- sync_state_from_primary() restores a full leader snapshot, used when a
  node rejoins, after a sequence gap, or before a new leader starts serving.
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Callable, Literal, Optional

import grpc

from miniredis.cluster import WriteAdmissionGate, majority_count
from miniredis.peer_channel import open_peer_channel
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.replication.progress import ReplicationProgress
from miniredis.storage import Store
from miniredis.replication.proto import replication_pb2
from miniredis.replication.proto import replication_pb2_grpc

logger = logging.getLogger(__name__)

# Default timeout for GetProgress RPCs (seconds).
_PROGRESS_RPC_TIMEOUT = 2.0


class ReplicaServicer(replication_pb2_grpc.ReplicationServiceServicer):
    """gRPC Servicer for the replica role."""

    def __init__(
        self,
        store: Store,
        node_id: str = "",
        lamport_clock: Optional[LamportClock] = None,
        progress: Optional[ReplicationProgress] = None,
        primary_id_provider: Optional[Callable[[], str]] = None,
        role_provider: Optional[Callable[[], str]] = None,
        on_newer_epoch: Optional[Callable[[int], object]] = None,
    ) -> None:
        self._store = store
        self._current_epoch = 0
        self._applied_sequence = 0
        self._node_id = node_id or "replica"
        self._lamport_clock = lamport_clock or LamportClock()
        self._progress = progress
        self._primary_id_provider = primary_id_provider
        self._role_provider = role_provider
        self._on_newer_epoch = on_newer_epoch

    def reset_state(self, epoch: int, sequence: int) -> None:
        """Reset servicer epoch and sequence after catching up state."""
        self._current_epoch = epoch
        self._applied_sequence = sequence

    def _fence_epoch(self) -> int:
        """Highest leadership epoch this node has accepted.

        A coordinator announcement advances the progress tracker before the
        new leader's first write arrives, so both sources are consulted.
        """
        progress_epoch = self._progress.epoch if self._progress is not None else 0
        return max(self._current_epoch, progress_epoch)

    def _notify_newer_epoch(self, epoch: int) -> None:
        if self._on_newer_epoch is None:
            return
        try:
            self._on_newer_epoch(epoch)
        except Exception:
            logger.exception("on_newer_epoch callback failed")

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

        if self._role_provider is not None and self._role_provider() == "primary":
            # A primary's store is only mutated by its own client writes. Being
            # replicated to means another node also believes it leads; report
            # the sender's epoch so whichever side is stale gets fenced.
            logger.warning(
                "Primary %s rejecting replicated write from a competing leader (epoch %s)",
                self._node_id,
                request.epoch,
            )
            self._notify_newer_epoch(request.epoch)
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=self._fence_epoch(),
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        fence_epoch = self._fence_epoch()
        if request.epoch < fence_epoch:
            logger.warning(
                "Rejecting obsolete replication epoch %s; current epoch is %s",
                request.epoch,
                fence_epoch,
            )
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=fence_epoch,
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        if request.epoch > fence_epoch:
            # Writes from a leader we have not been told about yet: the epoch
            # is newer, so the sender is not stale. Ask the leadership layer
            # to confirm and formally follow that leader.
            self._notify_newer_epoch(request.epoch)

        if request.epoch > self._current_epoch:
            # A coordinator announcement establishes the new epoch before
            # writes arrive. Reset only the per-epoch sequence, never the epoch.
            self._current_epoch = request.epoch
            self._applied_sequence = 0
            if self._progress is not None:
                self._progress.advance_epoch(request.epoch)

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
            # Record the gap in the progress tracker.
            if self._progress is not None:
                self._progress.record_gap(
                    epoch=request.epoch,
                    expected_sequence=self._applied_sequence + 1,
                    observed_sequence=request.sequence,
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
            fencing_tok = getattr(request, "fencing_token", "")
            if fencing_tok:
                try:
                    self._store.fenced_set(key, request.value, fencing_tok, ttl_seconds=ttl, expires_at=expires_at)
                except Exception as exc:
                    logger.warning("Replica rejected stale fencing token %s: %s", fencing_tok, exc)
            else:
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
            # Unknown operations do not advance progress.
            return replication_pb2.Ack(
                success=False,
                applied_sequence=self._applied_sequence,
                epoch=self._current_epoch,
                lamport_timestamp=await self._lamport_clock.tick(),
            )

        self._applied_sequence = request.sequence

        # Record the applied write in the shared progress tracker.
        if self._progress is not None:
            self._progress.record_applied(
                epoch=request.epoch,
                sequence=request.sequence,
                op_type=request.op_type,
                key=request.key,
                value=request.value,
                ttl_seconds=request.ttl_seconds,
                expiry_deadline_ms=request.expiry_deadline_ms,
            )

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

    async def GetProgress(
        self,
        request: replication_pb2.ProgressRequest,
        context: grpc.aio.ServicerContext,
    ) -> replication_pb2.ProgressResponse:
        """Return a snapshot of this node's replication progress."""
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="progress_request_recv",
            message_id=request.requester_node_id,
            description="received GetProgress request",
        )

        if self._progress is not None:
            snap = self._progress.snapshot()
        else:
            # No tracker — return what the servicer itself knows.
            snap = None

        resp_ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=resp_ts,
            node_id=self._node_id,
            event_type="progress_response_send",
            message_id=request.requester_node_id,
            description="sent GetProgress response",
        )

        primary_id = ""
        if self._primary_id_provider is not None:
            primary_id = self._primary_id_provider()
        elif snap is not None and snap.role == "primary":
            primary_id = snap.node_id

        if snap is not None:
            return replication_pb2.ProgressResponse(
                node_id=snap.node_id,
                role=snap.role,
                epoch=snap.epoch,
                last_applied_sequence=snap.last_applied_sequence,
                history_digest=snap.history_digest,
                gap_detected=snap.gap_detected,
                gap_expected_sequence=snap.gap_expected_sequence,
                gap_observed_sequence=snap.gap_observed_sequence,
                lamport_timestamp=resp_ts,
                primary_node_id=primary_id,
            )
        else:
            return replication_pb2.ProgressResponse(
                node_id=self._node_id,
                role="replica",
                epoch=self._current_epoch,
                last_applied_sequence=self._applied_sequence,
                history_digest="",
                gap_detected=False,
                gap_expected_sequence=0,
                gap_observed_sequence=0,
                lamport_timestamp=resp_ts,
                primary_node_id=primary_id,
            )

    async def SyncSnapshot(
        self,
        request: replication_pb2.SyncSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> replication_pb2.SyncSnapshotResponse:
        """Serve a point-in-time state snapshot to a catching-up replica."""
        del context
        recv_ts = await self._lamport_clock.receive(request.lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="sync_snapshot_recv",
            message_id=request.requester_node_id or "unknown",
            description="received SyncSnapshot request",
        )
        # Store contents and progress are read without an intervening await,
        # so the snapshot is a consistent (state, epoch, sequence) triple.
        snap = self._progress.snapshot() if self._progress is not None else None
        epoch = snap.epoch if snap is not None else self._current_epoch
        seq = snap.last_applied_sequence if snap is not None else self._applied_sequence
        digest = snap.history_digest if snap is not None else ""
        if self._role_provider is not None:
            role = self._role_provider()
        else:
            role = snap.role if snap is not None else "replica"

        items = [
            replication_pb2.SnapshotItem(
                key=key.decode("utf-8", errors="replace"),
                value=value,
                expiry_deadline_ms=int(expires_at * 1000) if expires_at is not None else 0,
            )
            for key, value, expires_at in self._store.snapshot_items()
        ]
        watermarks = {
            key.decode("utf-8", errors="replace"): token
            for key, token in self._store.fencing_watermarks().items()
        }

        resp_ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=resp_ts,
            node_id=self._node_id,
            event_type="sync_snapshot_send",
            message_id=request.requester_node_id or "unknown",
            description="sent SyncSnapshot response",
        )
        return replication_pb2.SyncSnapshotResponse(
            node_id=self._node_id,
            epoch=epoch,
            sequence=seq,
            history_digest=digest,
            items=items,
            lamport_timestamp=resp_ts,
            role=role,
            fencing_watermarks=watermarks,
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
        progress: Optional[ReplicationProgress] = None,
        target_to_node_id: Optional[dict[str, str]] = None,
        on_higher_epoch: Optional[Callable[[int], object]] = None,
    ) -> None:
        """
        targets: list of "host:port" strings for replica gRPC servers.
        progress: shared progress tracker for this node (primary side).
        on_higher_epoch: called when a replica rejects a write because it
            already follows a newer leadership epoch (this primary is stale).
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
        self._progress = progress
        self._target_to_node_id = dict(target_to_node_id or {})
        self._on_higher_epoch = on_higher_epoch
        self._channels: list[grpc.aio.Channel] = []
        self._stubs: list[replication_pb2_grpc.ReplicationServiceStub] = []

        for target in targets:
            channel = open_peer_channel(target)
            self._channels.append(channel)
            self._stubs.append(replication_pb2_grpc.ReplicationServiceStub(channel))

    @property
    def target_to_node_id(self) -> dict[str, str]:
        """Mapping from replica target gRPC addresses to logical node IDs."""
        return dict(self._target_to_node_id)

    @property
    def targets(self) -> list[str]:
        """Return replica target addresses."""
        return list(self._targets)

    @property
    def epoch(self) -> int:
        return self._epoch

    async def propagate(
        self,
        op_type: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
        fencing_token: str = "",
    ) -> None:
        """
        Increment sequence and push this write to all replicas.
        Fire-and-forget: doesn't block the caller waiting for acks.
        """
        if not self._stubs:
            return

        self._sequence += 1
        req = await self._build_write_op(op_type, key, value, ttl_seconds, expiry_deadline_ms, fencing_token)

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
        fencing_token: str = "",
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
        seq = self._sequence
        req = await self._build_write_op(op_type, key, value, ttl_seconds, expiry_deadline_ms, fencing_token)

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
                if successes == 0 and self._sequence == seq:
                    self._sequence -= 1

    async def propagate_write(
        self,
        op_type: str,
        key: bytes,
        value: bytes = b"",
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
        fencing_token: str = "",
    ) -> bool:
        """Dispatch the write path selected at construction time.

        Dispatcher code calls this single entry point and does not need to know
        whether the primary is running in async or quorum mode.
        """
        if self._write_mode == "quorum":
            return await self.propagate_quorum(op_type, key, value, ttl_seconds, expiry_deadline_ms, fencing_token)

        await self.propagate(op_type, key, value, ttl_seconds, expiry_deadline_ms, fencing_token)
        return True

    def record_primary_applied(
        self,
        epoch: int,
        sequence: int,
        op_type: str,
        key: bytes,
        value: bytes,
        ttl_seconds: int = 0,
        expiry_deadline_ms: int = 0,
    ) -> None:
        """Record a write that the primary has locally applied.

        Called by the Dispatcher after a successful local mutation, using
        the epoch and sequence returned from the replication path.
        This ensures the primary's own progress tracker stays current.
        """
        if self._progress is not None:
            self._progress.record_applied(
                epoch=epoch,
                sequence=sequence,
                op_type=op_type,
                key=key.decode("utf-8", errors="replace"),
                value=value,
                ttl_seconds=ttl_seconds,
                expiry_deadline_ms=expiry_deadline_ms,
            )

    def last_sequence(self) -> int:
        """Return the highest sequence number allocated so far."""
        return self._sequence

    async def collect_progress(
        self, timeout_seconds: float = _PROGRESS_RPC_TIMEOUT
    ) -> list[replication_pb2.ProgressResponse | None]:
        """Query every replica's progress concurrently.

        Returns a list parallel to ``self._targets``.  Each element is
        either a ProgressResponse or ``None`` (unreachable).
        """
        tasks = [
            asyncio.create_task(
                self._query_replica_progress(stub, target, timeout_seconds)
            )
            for stub, target in zip(self._stubs, self._targets)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [
            r if isinstance(r, replication_pb2.ProgressResponse) else None
            for r in results
        ]

    async def _query_replica_progress(
        self,
        stub: replication_pb2_grpc.ReplicationServiceStub,
        target: str,
        timeout_seconds: float,
    ) -> replication_pb2.ProgressResponse:
        """Send a GetProgress RPC to a single replica."""
        if self._rpc_filter is not None and not self._rpc_filter("GetProgress", target):
            raise ConnectionError("partitioned")

        req_ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=req_ts,
            node_id=self._node_id,
            event_type="progress_request_send",
            message_id=target,
            description="sent GetProgress request",
        )
        req = replication_pb2.ProgressRequest(
            requester_node_id=self._node_id,
            lamport_timestamp=req_ts,
        )
        resp = await asyncio.wait_for(stub.GetProgress(req), timeout=timeout_seconds)
        resp_ts = await self._lamport_clock.receive(resp.lamport_timestamp)
        log_lamport_event(
            timestamp=resp_ts,
            node_id=self._node_id,
            event_type="progress_response_recv",
            message_id=target,
            description="received GetProgress response",
        )
        return resp

    async def _build_write_op(
        self,
        op_type: str,
        key: bytes,
        value: bytes,
        ttl_seconds: int,
        expiry_deadline_ms: int,
        fencing_token: str = "",
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
            fencing_token=fencing_token,
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
            self._check_ack_epoch(ack, target)
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

    def _check_ack_epoch(self, ack: replication_pb2.Ack, target: str) -> None:
        """Report a rejection proving that a newer leadership epoch exists."""
        if ack.success or ack.epoch <= self._epoch:
            return
        logger.warning(
            "Replica %s rejected write: it follows epoch %s, newer than this primary's epoch %s",
            target,
            ack.epoch,
            self._epoch,
        )
        if self._on_higher_epoch is not None:
            try:
                self._on_higher_epoch(int(ack.epoch))
            except Exception:
                logger.exception("on_higher_epoch callback failed")

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
            self._check_ack_epoch(ack, target)
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


def _describe_error(exc: BaseException) -> str:
    """Render an RPC failure for logging without retaining the exception.

    Passing the exception object itself as a log argument keeps its traceback
    (and every frame's locals, i.e. the whole node) alive for as long as a
    handler retains the record.
    """
    if isinstance(exc, grpc.aio.AioRpcError):
        return f"{exc.code().name}: {exc.details()}"
    return repr(exc)


async def sync_state_from_primary(
    primary_grpc_address: str,
    store: Store,
    replica_servicer: ReplicaServicer,
    progress: Optional[ReplicationProgress],
    lamport_clock: LamportClock,
    node_id: str = "",
    timeout_seconds: float = 3.0,
    require_primary: bool = True,
    rpc_filter: Optional[Callable[[str, str], bool]] = None,
) -> bool:
    """Pull a point-in-time snapshot from *primary_grpc_address* and adopt it.

    The source's keyspace, epoch, sequence and history digest replace the
    local ones, so afterwards the next contiguous replicated write applies
    cleanly and the fingerprint keeps matching the leader's.

    Safety rules:
    - With ``require_primary`` (the default) the snapshot is only adopted if
      the serving node still reports itself as primary.
    - A snapshot from an older epoch than this node has already accepted is
      refused; adopting it could roll back newer leadership state.

    Returns True when local state matches the source afterwards.
    """
    channel = open_peer_channel(primary_grpc_address)
    try:
        if rpc_filter is not None and not rpc_filter("SyncSnapshot", primary_grpc_address):
            raise ConnectionError("partitioned")
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)
        req_ts = await lamport_clock.tick()
        log_lamport_event(
            timestamp=req_ts,
            node_id=node_id or "replica",
            event_type="sync_snapshot_request_send",
            message_id=primary_grpc_address,
            description="requested SyncSnapshot",
        )
        req = replication_pb2.SyncSnapshotRequest(
            requester_node_id=node_id,
            lamport_timestamp=req_ts,
        )
        resp = await asyncio.wait_for(stub.SyncSnapshot(req), timeout=timeout_seconds)
        await lamport_clock.receive(resp.lamport_timestamp)
    except Exception as exc:
        logger.warning(
            "Failed to sync snapshot from %s: %s", primary_grpc_address, _describe_error(exc)
        )
        return False
    finally:
        await channel.close()

    if require_primary and resp.role != "primary":
        logger.warning(
            "Refusing snapshot from %s: it reports role %r, not primary",
            primary_grpc_address,
            resp.role,
        )
        return False

    if progress is not None:
        local = progress.snapshot()
        if resp.epoch < local.epoch:
            logger.warning(
                "Refusing snapshot from %s at epoch %s; this node already accepted epoch %s",
                primary_grpc_address,
                resp.epoch,
                local.epoch,
            )
            return False

    # No await between replacing the keyspace and resetting progress, so a
    # concurrent ReplicateWrite never observes a half-restored node.
    store.replace_all(
        [
            (
                item.key.encode("utf-8"),
                item.value,
                item.expiry_deadline_ms / 1000.0 if item.expiry_deadline_ms > 0 else None,
            )
            for item in resp.items
        ],
        {key.encode("utf-8"): token for key, token in resp.fencing_watermarks.items()},
    )
    replica_servicer.reset_state(resp.epoch, resp.sequence)
    if progress is not None:
        progress.restore_from_snapshot(
            epoch=resp.epoch,
            sequence=resp.sequence,
            history_digest=resp.history_digest,
            role="replica",
        )
    logger.info(
        "Synchronized snapshot from %s (%s): epoch=%s, seq=%s, %d keys",
        primary_grpc_address,
        resp.node_id,
        resp.epoch,
        resp.sequence,
        len(resp.items),
    )
    return True
