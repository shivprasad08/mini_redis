"""
Mutex managers for M16:
1. RicartAgrawalaManager — Distributed mutual exclusion with Lamport clock ordering
2. CentralizedLockManager — Primary-coordinated lock baseline
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Set

import grpc

from miniredis.cluster import WriteAdmissionGate
from miniredis.logical_clock import LamportClock, log_lamport_event
from miniredis.mutex.protocol import FencingToken, LockState
from miniredis.mutex.proto import mutex_pb2, mutex_pb2_grpc

logger = logging.getLogger(__name__)


@dataclass
class DeferredRequest:
    requesting_node_id: str
    request_id: str
    resource_id: str
    lamport_timestamp: int


@dataclass
class ResourceState:
    state: LockState = LockState.RELEASED
    our_ts: int = 0
    our_request_id: str = ""
    client_id: str = ""
    acquired_token: Optional[FencingToken] = None
    replies_needed: Set[str] = field(default_factory=set)
    deferred: list[DeferredRequest] = field(default_factory=list)
    waiter_event: Optional[asyncio.Event] = None
    deadlock_cycle: Optional[list[str]] = None  # set when chosen as M19 victim


class DeadlockError(RuntimeError):
    """M19: this acquisition was aborted to break a distributed wait-for cycle."""

    def __init__(self, cycle: list[str]) -> None:
        super().__init__("deadlock detected: wait-for cycle " + " -> ".join(cycle))
        self.cycle = cycle


class BaseLockManager:
    """Abstract interface for cluster lock management."""

    async def acquire(
        self,
        resource_id: str,
        client_id: str = "",
        timeout_seconds: float = 5.0,
    ) -> FencingToken:
        raise NotImplementedError

    async def release(
        self,
        resource_id: str,
        token: Optional[str] = None,
        client_id: str = "",
    ) -> bool:
        raise NotImplementedError

    def get_status(self, resource_id: str) -> dict[str, Any]:
        raise NotImplementedError

    async def stop(self) -> None:
        pass


class RicartAgrawalaManager(BaseLockManager):
    """Ricart-Agrawala distributed mutual exclusion algorithm over gRPC."""

    def __init__(
        self,
        node_id: str,
        peer_addresses: Mapping[str, str],
        lamport_clock: Optional[LamportClock] = None,
        epoch_provider: Optional[Callable[[], int]] = None,
        rpc_timeout_seconds: float = 2.0,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
        probe_interval_seconds: float = 0.5,
    ) -> None:
        self._node_id = node_id
        # M19 edge-chasing state
        self._probe_interval = probe_interval_seconds
        self._seen_probes: OrderedDict[tuple[str, str], None] = OrderedDict()
        self.deadlock_stats = {"probes_sent": 0, "probes_received": 0, "duplicates": 0,
                               "detected": 0, "aborted": 0}
        self._peer_addresses = dict(peer_addresses)  # peer_id -> grpc_address
        self._lamport_clock = lamport_clock or LamportClock()
        self._epoch_provider = epoch_provider or (lambda: 0)
        self._rpc_timeout_seconds = rpc_timeout_seconds
        self._rpc_filter = rpc_filter

        self._resources: dict[str, ResourceState] = {}
        self._lock = asyncio.Lock()
        self._counter = 0

        # gRPC channels and stubs to peers
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, mutex_pb2_grpc.MutexServiceStub] = {}
        for peer_id, addr in self._peer_addresses.items():
            if peer_id == self._node_id:
                continue
            channel = grpc.aio.insecure_channel(addr)
            self._channels[peer_id] = channel
            self._stubs[peer_id] = mutex_pb2_grpc.MutexServiceStub(channel)

        # Message metrics
        self._msg_sent = 0
        self._msg_recv = 0

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def epoch(self) -> int:
        return self._epoch_provider()

    @property
    def lamport_clock(self) -> LamportClock:
        return self._lamport_clock

    def get_metrics(self) -> dict[str, int]:
        return {
            "messages_sent": self._msg_sent,
            "messages_received": self._msg_recv,
        }

    # -- M19: Chandy-Misra-Haas edge-chasing deadlock detection --------
    #
    # Process = node. Node P waits for Q while some resource is WANTED at P
    # and the reply from Q is still missing (Q holds the resource, or wants it
    # with higher priority, and deferred P). A blocked node periodically sends
    # probe(initiator=P) to every node it waits for; a blocked receiver
    # forwards it once (duplicates by (initiator, probe_id) are dropped) to
    # the nodes it waits for; a non-blocked receiver drops it. Probes only
    # travel along wait-for edges and each node forwards a probe id once, so
    # every probe terminates. If the initiator receives its own probe while
    # still blocked, the path is a cycle; the highest node id in the cycle is
    # the victim and aborts its pending acquisitions (DeadlockError), so one
    # node aborts per cycle. Lost probes (node/network failure) can only
    # delay detection, never fabricate a cycle.

    def waiting_for(self) -> set[str]:
        """Nodes this node currently waits on (outgoing wait-for edges)."""
        return {
            p for r in self._resources.values() if r.state == LockState.WANTED for p in r.replies_needed
        }

    async def _probe_loop(self, res: ResourceState) -> None:
        while res.state == LockState.WANTED and res.waiter_event is not None and not res.waiter_event.is_set():
            await asyncio.sleep(self._probe_interval)
            if res.state != LockState.WANTED:
                return
            probe_id = uuid.uuid4().hex
            self._remember_probe(self._node_id, probe_id)
            for target in sorted(self.waiting_for()):
                asyncio.create_task(self._send_probe(target, self._node_id, probe_id, [self._node_id]))

    def _remember_probe(self, initiator: str, probe_id: str) -> bool:
        key = (initiator, probe_id)
        if key in self._seen_probes:
            return False
        self._seen_probes[key] = None
        while len(self._seen_probes) > 4096:  # bounded cleanup
            self._seen_probes.popitem(last=False)
        return True

    async def _send_probe(self, target: str, initiator: str, probe_id: str, path: list[str]) -> None:
        stub = self._stubs.get(target)
        if stub is None:
            return
        if self._rpc_filter is not None and not self._rpc_filter("Probe", self._peer_addresses.get(target, "")):
            return
        try:
            ts = await self._lamport_clock.tick()
            self.deadlock_stats["probes_sent"] += 1
            await asyncio.wait_for(
                stub.Probe(mutex_pb2.ProbeMessage(initiator_id=initiator, probe_id=probe_id,
                                                  sender_id=self._node_id, path=path, lamport_timestamp=ts)),
                timeout=self._rpc_timeout_seconds,
            )
        except Exception as exc:  # lost probe: detection is retried next interval
            logger.debug("[%s] probe to %s lost: %s", self._node_id, target, str(exc))

    async def handle_probe(self, initiator: str, probe_id: str, sender: str, path: list[str]) -> str:
        self.deadlock_stats["probes_received"] += 1
        waits = self.waiting_for()
        if initiator == self._node_id:
            if not waits:
                return "not_blocked"
            cycle = path + [self._node_id]
            self.deadlock_stats["detected"] += 1
            logger.warning("[%s] deadlock detected: %s", self._node_id, " -> ".join(cycle))
            if max(cycle) == self._node_id:
                await self._abort_as_victim(cycle)
            return "detected"
        if not waits:
            return "not_blocked"
        if len(path) > len(self._peer_addresses) + 1:
            return "dropped"
        if not self._remember_probe(initiator, probe_id):
            self.deadlock_stats["duplicates"] += 1
            return "duplicate"
        for target in sorted(waits):
            asyncio.create_task(self._send_probe(target, initiator, probe_id, path + [self._node_id]))
        return "forwarded"

    async def _abort_as_victim(self, cycle: list[str]) -> None:
        async with self._lock:
            for res in self._resources.values():
                if res.state == LockState.WANTED and res.replies_needed and res.waiter_event is not None:
                    res.deadlock_cycle = cycle
                    res.waiter_event.set()
                    self.deadlock_stats["aborted"] += 1

    def _get_resource(self, resource_id: str) -> ResourceState:
        if resource_id not in self._resources:
            self._resources[resource_id] = ResourceState()
        return self._resources[resource_id]

    async def stop(self) -> None:
        for channel in self._channels.values():
            await channel.close()

    def update_peers(self, peer_addresses: Mapping[str, str]) -> None:
        """Update peer address map dynamically upon cluster changes."""
        self._peer_addresses = dict(peer_addresses)
        # Recreate stubs for newly discovered peers
        for peer_id, addr in self._peer_addresses.items():
            if peer_id != self._node_id and peer_id not in self._stubs:
                channel = grpc.aio.insecure_channel(addr)
                self._channels[peer_id] = channel
                self._stubs[peer_id] = mutex_pb2_grpc.MutexServiceStub(channel)

    # ── Incoming RPC Handlers (invoked by MutexServicer) ────────────

    async def handle_request_lock(
        self,
        requesting_node_id: str,
        resource_id: str,
        request_id: str,
        election_epoch: int,
        lamport_timestamp: int,
    ) -> tuple[bool, bool, str]:
        """Handle incoming LockRequest from a peer.

        Returns (granted, deferred, error_message).
        """
        self._msg_recv += 1
        recv_ts = await self._lamport_clock.receive(lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="mutex_request_recv",
            message_id=requesting_node_id,
            description=f"received lock request for {resource_id}",
        )

        async with self._lock:
            res = self._get_resource(resource_id)

            # Check epoch compatibility
            current_epoch = self.epoch
            if election_epoch < current_epoch:
                return False, False, f"stale election epoch {election_epoch} < {current_epoch}"

            # Check Ricart-Agrawala condition
            defer = False
            if res.state == LockState.HELD:
                defer = True
            elif res.state == LockState.WANTED:
                # Compare priorities: (our_ts, our_node_id) vs (req_ts, req_node_id)
                our_prio = (res.our_ts, self._node_id)
                their_prio = (lamport_timestamp, requesting_node_id)
                if our_prio < their_prio:
                    defer = True

            if defer:
                # Add to deferred requests if not already queued
                if not any(d.request_id == request_id for d in res.deferred):
                    res.deferred.append(
                        DeferredRequest(
                            requesting_node_id=requesting_node_id,
                            request_id=request_id,
                            resource_id=resource_id,
                            lamport_timestamp=lamport_timestamp,
                        )
                    )
                logger.debug(
                    "[%s] Deferring lock reply for %s to %s (req_id=%s)",
                    self._node_id, resource_id, requesting_node_id, request_id
                )
                return False, True, ""
            else:
                # Grant immediately
                logger.debug(
                    "[%s] Granting lock reply for %s immediately to %s",
                    self._node_id, resource_id, requesting_node_id
                )
                return True, False, ""

    async def handle_request_reply(
        self,
        responder_node_id: str,
        resource_id: str,
        request_id: str,
        granted: bool,
        lamport_timestamp: int,
    ) -> None:
        """Handle incoming ReplyMessage (grant) from a peer."""
        self._msg_recv += 1
        recv_ts = await self._lamport_clock.receive(lamport_timestamp)
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="mutex_reply_recv",
            message_id=responder_node_id,
            description=f"received grant for {resource_id}",
        )

        async with self._lock:
            res = self._get_resource(resource_id)
            if res.state == LockState.WANTED and res.our_request_id == request_id:
                if granted and responder_node_id in res.replies_needed:
                    res.replies_needed.remove(responder_node_id)
                    logger.debug(
                        "[%s] Received grant from %s for %s. Remaining replies: %s",
                        self._node_id, responder_node_id, resource_id, res.replies_needed
                    )
                    if not res.replies_needed and res.waiter_event:
                        res.waiter_event.set()

    # ── Client-Facing API ───────────────────────────────────────────

    async def acquire(
        self,
        resource_id: str,
        client_id: str = "",
        timeout_seconds: float = 5.0,
    ) -> FencingToken:
        """Request the distributed lock using Ricart-Agrawala algorithm."""
        req_id = uuid.uuid4().hex
        peers = [p for p in self._peer_addresses.keys() if p != self._node_id]

        async with self._lock:
            res = self._get_resource(resource_id)
            if res.state == LockState.HELD:
                raise RuntimeError(f"Lock for {resource_id} is already held locally")
            if res.state == LockState.WANTED:
                raise RuntimeError(f"Lock acquisition for {resource_id} is already pending")

            our_ts = await self._lamport_clock.tick()
            res.state = LockState.WANTED
            res.our_ts = our_ts
            res.our_request_id = req_id
            res.client_id = client_id
            res.replies_needed = set(peers)
            res.waiter_event = asyncio.Event()
            res.deadlock_cycle = None

        # If no peers exist in cluster, acquire immediately!
        if not peers:
            async with self._lock:
                self._counter += 1
                token = FencingToken(
                    epoch=self.epoch,
                    lamport_ts=our_ts,
                    counter=self._counter,
                    node_id=self._node_id,
                )
                res.state = LockState.HELD
                res.acquired_token = token
                res.replies_needed.clear()
                return token

        # Send RequestLock to all participating peers
        tasks = []
        for peer_id in peers:
            stub = self._stubs.get(peer_id)
            tasks.append(
                self._send_request_lock(
                    peer_id=peer_id,
                    stub=stub,
                    resource_id=resource_id,
                    request_id=req_id,
                    lamport_ts=our_ts,
                )
            )

        # Broadcast requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        async with self._lock:
            # Check for any fatal RPC errors or unreachability
            for peer_id, result in zip(peers, results):
                if isinstance(result, Exception):
                    logger.warning(
                        "[%s] Peer %s failed during LockRequest for %s: %s",
                        # str(): a retained log record must not pin the
                        # exception's traceback (and its gRPC channel).
                        self._node_id, peer_id, resource_id, str(result)
                    )
                    # Classical Ricart-Agrawala requires affirmative replies from ALL nodes.
                    # Unreachable participant means acquisition failure.
                    await self._cleanup_failed_acquisition(resource_id)
                    raise RuntimeError(
                        f"Participant {peer_id} unreachable: Ricart-Agrawala requires all participants"
                    )
                granted, deferred = result
                if granted and peer_id in res.replies_needed:
                    res.replies_needed.remove(peer_id)

            if not res.replies_needed:
                # All granted immediately!
                self._counter += 1
                token = FencingToken(
                    epoch=self.epoch,
                    lamport_ts=our_ts,
                    counter=self._counter,
                    node_id=self._node_id,
                )
                res.state = LockState.HELD
                res.acquired_token = token
                return token

        # Wait for remaining deferred replies to arrive via RequestReply RPC,
        # probing for distributed deadlock meanwhile (M19).
        prober = asyncio.create_task(self._probe_loop(res))
        try:
            await asyncio.wait_for(res.waiter_event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            async with self._lock:
                logger.warning(
                    "[%s] Timed out waiting for replies for %s (needed: %s)",
                    self._node_id, resource_id, res.replies_needed
                )
                await self._cleanup_failed_acquisition(resource_id)
                raise TimeoutError(
                    f"Timed out waiting for Ricart-Agrawala replies from {res.replies_needed}"
                )
        finally:
            prober.cancel()

        async with self._lock:
            if res.deadlock_cycle is not None:
                cycle = res.deadlock_cycle
                await self._cleanup_failed_acquisition(resource_id)
                raise DeadlockError(cycle)
            self._counter += 1
            token = FencingToken(
                epoch=self.epoch,
                lamport_ts=our_ts,
                counter=self._counter,
                node_id=self._node_id,
            )
            res.state = LockState.HELD
            res.acquired_token = token
            return token

    async def _send_request_lock(
        self,
        peer_id: str,
        stub: Optional[mutex_pb2_grpc.MutexServiceStub],
        resource_id: str,
        request_id: str,
        lamport_ts: int,
    ) -> tuple[bool, bool]:
        if stub is None:
            raise ConnectionError(f"no stub for peer {peer_id}")

        if self._rpc_filter is not None:
            addr = self._peer_addresses.get(peer_id, "")
            if not self._rpc_filter("RequestLock", addr):
                raise ConnectionError(f"RPC filtered to {peer_id}")

        req_ts = await self._lamport_clock.tick()
        log_lamport_event(
            timestamp=req_ts,
            node_id=self._node_id,
            event_type="mutex_request_send",
            message_id=peer_id,
            description=f"sent lock request for {resource_id}",
        )
        self._msg_sent += 1

        req = mutex_pb2.LockRequest(
            requesting_node_id=self._node_id,
            resource_id=resource_id,
            request_id=request_id,
            election_epoch=self.epoch,
            lamport_timestamp=req_ts,
        )

        resp = await asyncio.wait_for(
            stub.RequestLock(req),
            timeout=self._rpc_timeout_seconds,
        )
        recv_ts = await self._lamport_clock.receive(resp.lamport_timestamp)
        self._msg_recv += 1
        log_lamport_event(
            timestamp=recv_ts,
            node_id=self._node_id,
            event_type="mutex_ack_recv",
            message_id=peer_id,
            description=f"received lock response from {peer_id}",
        )

        return resp.granted, resp.deferred

    async def _cleanup_failed_acquisition(self, resource_id: str) -> None:
        """Reset state on failed/cancelled acquisition and flush deferred replies."""
        res = self._get_resource(resource_id)
        res.state = LockState.RELEASED
        res.our_request_id = ""
        res.replies_needed.clear()
        res.acquired_token = None
        if res.waiter_event:
            res.waiter_event.set()
        await self._flush_deferred_replies(resource_id)

    async def _flush_deferred_replies(self, resource_id: str) -> None:
        """Send pending grant replies to all deferred peer requests."""
        res = self._get_resource(resource_id)
        pending = list(res.deferred)
        res.deferred.clear()

        for req in pending:
            asyncio.create_task(
                self._send_reply_message(
                    target_peer_id=req.requesting_node_id,
                    resource_id=req.resource_id,
                    request_id=req.request_id,
                )
            )

    async def _send_reply_message(
        self,
        target_peer_id: str,
        resource_id: str,
        request_id: str,
    ) -> None:
        stub = self._stubs.get(target_peer_id)
        if stub is None:
            return

        if self._rpc_filter is not None:
            addr = self._peer_addresses.get(target_peer_id, "")
            if not self._rpc_filter("RequestReply", addr):
                return

        try:
            req_ts = await self._lamport_clock.tick()
            log_lamport_event(
                timestamp=req_ts,
                node_id=self._node_id,
                event_type="mutex_reply_send",
                message_id=target_peer_id,
                description=f"sent deferred grant for {resource_id}",
            )
            self._msg_sent += 1

            msg = mutex_pb2.ReplyMessage(
                responder_node_id=self._node_id,
                requesting_node_id=target_peer_id,
                resource_id=resource_id,
                request_id=request_id,
                granted=True,
                lamport_timestamp=req_ts,
            )
            resp = await asyncio.wait_for(stub.RequestReply(msg), timeout=self._rpc_timeout_seconds)
            await self._lamport_clock.receive(resp.lamport_timestamp)
            self._msg_recv += 1
        except Exception as exc:
            logger.warning("[%s] Failed to send RequestReply to %s: %s", self._node_id, target_peer_id, exc)

    async def release(
        self,
        resource_id: str,
        token: Optional[str] = None,
        client_id: str = "",
    ) -> bool:
        """Release the held lock and grant access to deferred peer requests."""
        async with self._lock:
            res = self._get_resource(resource_id)
            if res.state != LockState.HELD:
                raise RuntimeError(f"Lock for {resource_id} is not held")

            if token is not None and res.acquired_token is not None:
                parsed_token = FencingToken.from_str(token)
                if parsed_token != res.acquired_token:
                    raise PermissionError(
                        f"Cannot release lock with token {token}: held by {res.acquired_token}"
                    )

            if client_id and res.client_id and client_id != res.client_id:
                raise PermissionError(
                    f"Cannot release lock owned by client {res.client_id}"
                )

            res.state = LockState.RELEASED
            res.acquired_token = None
            res.client_id = ""
            res.our_request_id = ""

            await self._flush_deferred_replies(resource_id)
            return True

    def get_status(self, resource_id: str) -> dict[str, Any]:
        res = self._get_resource(resource_id)
        return {
            "resource_id": resource_id,
            "state": res.state.value,
            "current_holder": self._node_id if res.state == LockState.HELD else "",
            "fencing_token": str(res.acquired_token) if res.acquired_token else "",
            "deferred_count": len(res.deferred),
        }


# ────────────────────────────────────────────────────────────────────
# Centralized Lock Manager (Primary Baseline)
# ────────────────────────────────────────────────────────────────────

@dataclass
class CentralizedHolder:
    resource_id: str
    owner_node_id: str
    client_id: str
    token: FencingToken
    request_id: str


class CentralizedLockManager(BaseLockManager):
    """Centralized primary-coordinated lock manager baseline."""

    def __init__(
        self,
        node_id: str,
        role: str,
        primary_address: str = "",
        write_gate: Optional[WriteAdmissionGate] = None,
        lamport_clock: Optional[LamportClock] = None,
        epoch_provider: Optional[Callable[[], int]] = None,
        rpc_timeout_seconds: float = 2.0,
        rpc_filter: Optional[Callable[[str, str], bool]] = None,
    ) -> None:
        self._node_id = node_id
        self._role = role  # "primary" or "replica"
        self._primary_address = primary_address
        self._write_gate = write_gate or WriteAdmissionGate(can_serve_writes=(role == "primary"))
        self._lamport_clock = lamport_clock or LamportClock()
        self._epoch_provider = epoch_provider or (lambda: 0)
        self._rpc_timeout_seconds = rpc_timeout_seconds
        self._rpc_filter = rpc_filter

        self._counter = 0
        self._held_locks: dict[str, CentralizedHolder] = {}
        self._lock_condition = asyncio.Condition()

        # Primary stub if replica
        self._primary_channel: Optional[grpc.aio.Channel] = None
        self._primary_stub: Optional[mutex_pb2_grpc.MutexServiceStub] = None
        if self._role != "primary" and self._primary_address:
            self._primary_channel = grpc.aio.insecure_channel(self._primary_address)
            self._primary_stub = mutex_pb2_grpc.MutexServiceStub(self._primary_channel)

        self._msg_sent = 0
        self._msg_recv = 0

    @property
    def role(self) -> str:
        return self._role

    def set_role(self, role: str, primary_address: str = "") -> None:
        self._role = role
        if primary_address:
            self._primary_address = primary_address
            if self._primary_channel:
                asyncio.create_task(self._primary_channel.close())
            self._primary_channel = grpc.aio.insecure_channel(primary_address)
            self._primary_stub = mutex_pb2_grpc.MutexServiceStub(self._primary_channel)

    @property
    def epoch(self) -> int:
        return self._epoch_provider()

    def get_metrics(self) -> dict[str, int]:
        return {
            "messages_sent": self._msg_sent,
            "messages_received": self._msg_recv,
        }

    async def stop(self) -> None:
        if self._primary_channel:
            await self._primary_channel.close()

    async def acquire(
        self,
        resource_id: str,
        client_id: str = "",
        timeout_seconds: float = 5.0,
    ) -> FencingToken:
        if self._role != "primary":
            # Forward to primary via gRPC
            if not self._primary_stub:
                raise RuntimeError("Replica has no primary configured for centralized lock")
            return await self._remote_acquire(resource_id, client_id, timeout_seconds)

        # Primary side:
        if not self._write_gate.can_serve_writes:
            raise PermissionError(f"Primary lost majority: {self._write_gate.reason}")

        req_id = uuid.uuid4().hex
        async with self._lock_condition:
            start_time = asyncio.get_event_loop().time()
            while resource_id in self._held_locks:
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining = timeout_seconds - elapsed
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for centralized lock on {resource_id}")
                try:
                    await asyncio.wait_for(self._lock_condition.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Timed out waiting for centralized lock on {resource_id}")

            self._counter += 1
            ts = await self._lamport_clock.tick()
            token = FencingToken(
                epoch=self.epoch,
                lamport_ts=ts,
                counter=self._counter,
                node_id=self._node_id,
            )
            self._held_locks[resource_id] = CentralizedHolder(
                resource_id=resource_id,
                owner_node_id=self._node_id,
                client_id=client_id,
                token=token,
                request_id=req_id,
            )
            return token

    async def _remote_acquire(
        self,
        resource_id: str,
        client_id: str,
        timeout_seconds: float,
    ) -> FencingToken:
        req_ts = await self._lamport_clock.tick()
        self._msg_sent += 1
        req = mutex_pb2.LockRequest(
            requesting_node_id=self._node_id,
            resource_id=resource_id,
            request_id=uuid.uuid4().hex,
            election_epoch=self.epoch,
            lamport_timestamp=req_ts,
            client_id=client_id,
            centralized=True,
        )
        resp = await asyncio.wait_for(self._primary_stub.RequestLock(req), timeout=timeout_seconds)
        self._msg_recv += 1
        await self._lamport_clock.receive(resp.lamport_timestamp)
        if not resp.granted:
            raise RuntimeError(f"Centralized lock denied by primary: {resp.error_message}")
        return FencingToken.from_str(resp.fencing_token)

    async def release(
        self,
        resource_id: str,
        token: Optional[str] = None,
        client_id: str = "",
    ) -> bool:
        if self._role != "primary":
            if not self._primary_stub:
                raise RuntimeError("Replica has no primary configured for centralized unlock")
            return await self._remote_release(resource_id, token, client_id)

        async with self._lock_condition:
            holder = self._held_locks.get(resource_id)
            if holder is None:
                raise RuntimeError(f"Lock for {resource_id} is not held")

            if token is not None:
                parsed_token = FencingToken.from_str(token)
                if parsed_token != holder.token:
                    raise PermissionError(
                        f"Cannot release lock: token {token} does not match holder token {holder.token}"
                    )

            if client_id and holder.client_id and client_id != holder.client_id:
                raise PermissionError(
                    f"Cannot release lock: owned by client {holder.client_id}"
                )

            del self._held_locks[resource_id]
            self._lock_condition.notify_all()
            return True

    async def _remote_release(
        self,
        resource_id: str,
        token: Optional[str],
        client_id: str,
    ) -> bool:
        req_ts = await self._lamport_clock.tick()
        self._msg_sent += 1
        req = mutex_pb2.ReleaseRequest(
            requesting_node_id=self._node_id,
            resource_id=resource_id,
            request_id="",
            fencing_token=token or "",
            client_id=client_id,
            election_epoch=self.epoch,
            lamport_timestamp=req_ts,
            centralized=True,
        )
        resp = await asyncio.wait_for(self._primary_stub.ReleaseLock(req), timeout=self._rpc_timeout_seconds)
        self._msg_recv += 1
        await self._lamport_clock.receive(resp.lamport_timestamp)
        if not resp.success:
            raise RuntimeError(f"Centralized release failed: {resp.error_message}")
        return True

    def get_status(self, resource_id: str) -> dict[str, Any]:
        holder = self._held_locks.get(resource_id)
        return {
            "resource_id": resource_id,
            "state": LockState.HELD.value if holder else LockState.RELEASED.value,
            "current_holder": holder.owner_node_id if holder else "",
            "fencing_token": str(holder.token) if holder else "",
        }
