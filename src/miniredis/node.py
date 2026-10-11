"""
Node assembly and lifecycle for one mini-redis process.

``MiniRedisNode`` wires the store, dispatcher, gRPC services, election,
majority monitor and leadership reconciler together. ``server.main`` builds
one from CLI flags; tests build several in-process on dynamic ports to
exercise the real startup, failover and rejoin paths.

Startup lifecycle
─────────────────
1. Every component starts in a *joining* state: role ``replica``, epoch 0,
   writes blocked. The configured ``--role`` is only a bootstrap hint.
2. The gRPC and client TCP servers start (PING/GET work, writes refused).
3. ``LeadershipReconciler.establish_initial_role`` decides, from what the
   peers report, whether this node follows an existing leader (and pulls its
   snapshot), bootstraps a brand-new cluster (configured primary + majority
   reachable + no established epoch anywhere), or waits.
4. The reconciler then keeps re-confirming leadership in the background.

With ``data_dir`` set (M20), the node first opens and verifies its ledger and
replays it into the store and progress tracker — before any server starts —
so the state it advertises to peers is the recovered one. A corrupted ledger
raises ``LedgerCorruptionError`` and the node does not start.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import grpc

from miniredis.cluster import ClusterMajorityMonitor, WriteAdmissionGate
from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.leadership import LeadershipReconciler
from miniredis.election.proto import election_pb2_grpc
from miniredis.election.ring import RingElection
from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.ledger import WriteLedger, replay
from miniredis.links import LinkGuard, LinkManager
from miniredis.logical_clock import LamportClock
from miniredis.mutex import CentralizedLockManager, MutexServicer, RicartAgrawalaManager
from miniredis.mutex.proto import mutex_pb2_grpc
from miniredis.pubsub import PubSub
from miniredis.replication.progress import ReplicationProgress
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, sync_state_from_primary
from miniredis.snapshot import SnapshotManager
from miniredis.storage import Store
from miniredis.time_sync.clock import SynchronizedClock
from miniredis.time_sync.proto import time_pb2_grpc
from miniredis.time_sync.service import TimeServicer, TimeSynchronizer

logger = logging.getLogger(__name__)


@dataclass
class NodeConfig:
    """Everything needed to run one node (mirrors the CLI flags)."""

    node_id: Optional[str] = None
    host: str = "127.0.0.1"
    port: int = 6380
    grpc_port: int = 6381
    role: str = "primary"  # bootstrap hint only — see module docstring
    replica_of: list[str] = field(default_factory=list)
    primary_address: Optional[str] = None
    primary_node_id: Optional[str] = None
    peers: dict[str, PeerEndpoint] = field(default_factory=dict)
    write_mode: str = "async"
    simulate_latency_ms: float = 0.0
    clock_skew_ms: float = 0.0
    mutex_algo: str = "ricart-agrawala"
    election_algo: str = "bully"
    reconcile_interval_seconds: float = 1.0
    # Test hook for simulating network faults: (rpc_name, target_address) -> allowed.
    rpc_filter: Optional[Callable[[str, str], bool]] = None
    # M20 persistence: None disables the ledger (in-memory only, as before).
    data_dir: Optional[str] = None
    fsync_policy: str = "always"
    fsync_interval_ms: Optional[int] = None
    # M17 global snapshots
    snapshot_timeout_ms: int = 5000


class MiniRedisNode:
    """One mini-redis process: servers, replication, election and recovery."""

    def __init__(self, config: NodeConfig) -> None:
        if config.role == "replica" and not config.primary_address:
            raise ValueError("--primary-address is required when --role=replica")

        self._config = config
        self.node_id = config.node_id or f"{config.host}:{config.grpc_port}"
        self.tcp_address = f"{config.host}:{config.port}"
        self.grpc_address = f"{config.host}:{config.grpc_port}"
        self._peers = {pid: peer for pid, peer in config.peers.items() if pid != self.node_id}
        self._total_cluster_size = len(self._peers) + 1

        self.physical_clock = SynchronizedClock(skew_ms=config.clock_skew_ms)
        self.store = Store(clock=self.physical_clock.corrected_time_seconds)
        self.lamport_clock = LamportClock()
        self.progress = ReplicationProgress(node_id=self.node_id, role="replica", epoch=0)
        self.ledger: Optional[WriteLedger] = (
            WriteLedger(config.data_dir, config.fsync_policy, config.fsync_interval_ms)
            if config.data_dir
            else None
        )
        self.links = LinkManager(self.node_id, self._peers, rpc_filter=config.rpc_filter)
        self.write_gate = WriteAdmissionGate(can_serve_writes=False)
        self.write_gate.block_writes("joining cluster, leader not established yet")

        self._grpc_server: Optional[grpc.aio.Server] = None
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._majority_monitor: Optional[ClusterMajorityMonitor] = None
        self._reconciler: Optional[LeadershipReconciler] = None
        self._lifecycle_task: Optional[asyncio.Task[None]] = None
        self._established = asyncio.Event()
        self._sync_lock = asyncio.Lock()
        self.election: BullyElection | RingElection | None = None
        self._build()

    # ── assembly ─────────────────────────────────────────────────────

    def _build(self) -> None:
        config = self._config
        epoch_provider = lambda: self.election.election_epoch if self.election is not None else 0

        if config.mutex_algo == "centralized":
            self.lock_manager = CentralizedLockManager(
                node_id=self.node_id,
                role="replica",
                primary_address=config.primary_address or "",
                write_gate=self.write_gate,
                lamport_clock=self.lamport_clock,
                epoch_provider=epoch_provider,
            )
        else:
            self.lock_manager = RicartAgrawalaManager(
                node_id=self.node_id,
                peer_addresses={pid: peer.grpc_address for pid, peer in self._peers.items()},
                lamport_clock=self.lamport_clock,
                epoch_provider=epoch_provider,
            )

        self.dispatcher = Dispatcher(
            self.store,
            PubSub(pubsub_message_framer),
            writes_enabled=False,
            write_gate=self.write_gate,
            clock=self.physical_clock.corrected_time_seconds,
            progress=self.progress,
            lock_manager=self.lock_manager,
        )

        peer_grpc_addresses = {peer.grpc_address for peer in self._peers.values()}
        election_cls = RingElection if config.election_algo == "ring" else BullyElection
        self.election = election_cls(
            node_id=self.node_id,
            tcp_address=self.tcp_address,
            grpc_address=self.grpc_address,
            peers=self._peers,
            dispatcher=self.dispatcher,
            total_cluster_size=self._total_cluster_size,
            failure_detector=None,
            write_gate=self.write_gate,
            role="replica",  # joining; never the configured role — see module docstring
            write_mode=config.write_mode,
            rpc_filter=config.rpc_filter,
            simulate_latency_ms=config.simulate_latency_ms,
            lamport_clock=self.lamport_clock,
            progress=self.progress,
            lock_manager=self.lock_manager,
            extra_replica_targets=[t for t in config.replica_of if t not in peer_grpc_addresses],
        )
        election = self.election

        self.replica_servicer = ReplicaServicer(
            self.store,
            node_id=self.node_id,
            lamport_clock=self.lamport_clock,
            progress=self.progress,
            primary_id_provider=lambda: election.current_primary_id,
            role_provider=lambda: election.role,
            on_newer_epoch=election.observe_higher_epoch,
            ledger=self.ledger,
            link_guard=LinkGuard(),
        )
        election.attach_state_syncer(self._sync_from)
        election.attach_links(self.links)
        self.dispatcher.set_ledger(self.ledger)

        self.snapshots = SnapshotManager(
            node_id=self.node_id,
            peers=self._peers,
            links=self.links,
            capture_state=self._capture_snapshot_state,
            epoch_provider=lambda: election.election_epoch,
            commit_lock=self.dispatcher.commit_lock,
            lamport_clock=self.lamport_clock,
            default_timeout_ms=config.snapshot_timeout_ms,
            rpc_filter=config.rpc_filter,
        )
        self.replica_servicer.attach_snapshot_manager(self.snapshots)
        self.dispatcher.set_snapshot_manager(self.snapshots)

        self._reconciler = LeadershipReconciler(
            election=election,
            progress=self.progress,
            peers=self._peers,
            node_id=self.node_id,
            lamport_clock=self.lamport_clock,
            state_syncer=self._sync_from,
            total_cluster_size=self._total_cluster_size,
            interval_seconds=config.reconcile_interval_seconds,
            rpc_filter=config.rpc_filter,
            on_follow=self._on_follow,
        )
        election.attach_leadership_listener(self._reconciler.wake)

    async def _sync_from(self, grpc_address: str, *, require_primary: bool = True) -> bool:
        async with self._sync_lock:
            return await sync_state_from_primary(
                primary_grpc_address=grpc_address,
                store=self.store,
                replica_servicer=self.replica_servicer,
                progress=self.progress,
                lamport_clock=self.lamport_clock,
                node_id=self.node_id,
                require_primary=require_primary,
                rpc_filter=self._config.rpc_filter,
                ledger=self.ledger,
                apply_guard=lambda: not self.snapshots.recording_active(),
            )

    def _capture_snapshot_state(self) -> dict[str, Any]:
        """Local state for a Chandy-Lamport snapshot (called synchronously)."""
        assert self.election is not None
        progress = self.progress.snapshot()
        keys = {
            key.decode("utf-8", errors="replace"): {
                "value": value.decode("utf-8", errors="replace"),
                "expires_at_ms": int(expires_at * 1000) if expires_at is not None else None,
            }
            for key, value, expires_at in self.store.snapshot_items()
        }
        return {
            "role": self.election.role,
            "election_epoch": self.election.election_epoch,
            "current_primary": self.election.current_primary_id or None,
            "epoch": progress.epoch,
            "last_applied_sequence": progress.last_applied_sequence,
            "history_digest": progress.history_digest,
            "ledger": self.ledger.head() if self.ledger is not None else None,
            "lamport": self.lamport_clock._value,
            "key_count": len(keys),
            "keys": keys,
        }

    async def _on_follow(self, leader_id: str, peer: PeerEndpoint) -> None:
        """Point M14 clock synchronization at the leader being followed."""
        assert self.election is not None
        synchronizer = self.election.time_synchronizer
        if synchronizer is None:
            synchronizer = TimeSynchronizer(
                node_id=self.node_id,
                peer_address=peer.grpc_address,
                clock=self.physical_clock,
                interval_seconds=1.0,
                timeout_seconds=0.5,
                lamport_clock=self.lamport_clock,
            )
            self.election.attach_time_synchronizer(synchronizer)
            await synchronizer.start()
        elif synchronizer.peer_address != peer.grpc_address:
            await synchronizer.update_peer(peer.grpc_address)

    def _watch_primary(self, primary_id: str, primary_address: str) -> FailureDetector:
        assert self.election is not None
        election = self.election
        detector = FailureDetector(
            peer_node_id=primary_id,
            peer_address=primary_address,
            heartbeat_interval_seconds=1.0,
            heartbeat_timeout_seconds=0.5,
            failure_threshold=3,
            sender_id=self.node_id,
            rpc_filter=self._config.rpc_filter,
            lamport_clock=self.lamport_clock,
        )

        def _log_state_change(peer_id: str, old_state: str, new_state: str) -> None:
            del peer_id, old_state
            if new_state == "suspected":
                logger.warning("PRIMARY SUSPECTED DOWN")
            elif new_state == "dead":
                logger.error("PRIMARY DECLARED DEAD")
                asyncio.create_task(election.start_election())
            elif new_state == "alive":
                logger.info("PRIMARY RECOVERED")

        detector.on_state_change(_log_state_change)
        return detector

    # ── lifecycle ────────────────────────────────────────────────────

    @property
    def established(self) -> bool:
        return self._established.is_set()

    async def start(self) -> None:
        """Start servers and begin joining the cluster (non-blocking)."""
        assert self.election is not None
        config = self._config

        if self.ledger is not None:
            # Recover before serving anything: peers and clients must only ever
            # see the replayed state. Corruption raises and aborts the start.
            records = self.ledger.open()
            epoch, seq = replay(records, self.store, self.progress)
            self.replica_servicer.reset_state(epoch, seq)
            await self.ledger.start()
            logger.info(
                "Node %s replayed %d ledger records: epoch=%s seq=%s, %d keys",
                self.node_id, len(records), epoch, seq, len(self.store.snapshot_items()),
            )

        self._grpc_server = grpc.aio.server()
        mutex_pb2_grpc.add_MutexServiceServicer_to_server(
            MutexServicer(self.lock_manager, lamport_clock=self.lamport_clock), self._grpc_server
        )
        election = self.election
        health_pb2_grpc.add_HealthServiceServicer_to_server(
            HealthServicer(
                node_id=self.node_id,
                role="replica",
                lamport_clock=self.lamport_clock,
                role_provider=lambda: election.role,
            ),
            self._grpc_server,
        )
        replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
            self.replica_servicer, self._grpc_server
        )
        time_pb2_grpc.add_TimeServiceServicer_to_server(
            TimeServicer(node_id=self.node_id, clock=self.physical_clock, lamport_clock=self.lamport_clock),
            self._grpc_server,
        )
        election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, self._grpc_server)
        self._grpc_server.add_insecure_port(self.grpc_address)
        await self._grpc_server.start()
        logger.info("gRPC server listening on %s", self.grpc_address)

        self._majority_monitor = ClusterMajorityMonitor(
            node_id=self.node_id,
            peer_health_addresses={pid: peer.grpc_address for pid, peer in self._peers.items()},
            total_cluster_size=self._total_cluster_size,
            gate=self.write_gate,
            rpc_filter=config.rpc_filter,
            lamport_clock=self.lamport_clock,
        )

        async def _on_majority_change(state: str) -> None:
            # The monitor reports "majority"/"minority"; both are truthy strings.
            await election.handle_majority_change(state == "majority")

        self._majority_monitor.on_state_change(_on_majority_change)
        await self._majority_monitor.start()

        # Local import: server.py imports this module from main().
        from miniredis.server import run_server

        # Clients (and Docker health checks) can connect while the node is
        # still joining; reads are served and writes refused until a role
        # is established.
        self._tcp_server = await run_server(
            host=config.host,
            port=config.port,
            handler=self.dispatcher.handle_command,
            on_disconnect=self.dispatcher.handle_disconnect,
        )

        self._lifecycle_task = asyncio.create_task(self._lifecycle())

    async def _lifecycle(self) -> None:
        assert self.election is not None and self._reconciler is not None
        config = self._config
        try:
            role = await self._reconciler.establish_initial_role(config.role)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Stay a write-blocked replica; the background reconciler below
            # keeps looking for a leader instead of the node silently stalling.
            logger.exception("Node %s failed to establish its initial role", self.node_id)
            role = "replica"

        if role != "primary" and self.election.role != "primary":
            if not self.election.current_primary_id and config.primary_address:
                # Brand-new cluster: watch the configured primary, as before.
                if self.election.failure_detector is None:
                    detector = self._watch_primary(
                        config.primary_node_id or config.primary_address, config.primary_address
                    )
                    self.election.attach_failure_detector(detector)
                    await detector.start()
                if self.election.time_synchronizer is None:
                    await self._on_follow(
                        config.primary_node_id or config.primary_address,
                        PeerEndpoint(
                            node_id=config.primary_node_id or config.primary_address,
                            grpc_address=config.primary_address,
                            tcp_address="",
                        ),
                    )

        logger.info(
            "Node %s established: role=%s epoch=%s primary=%s",
            self.node_id,
            self.election.role,
            self.election.election_epoch,
            self.election.current_primary_id or "<unknown>",
        )
        self._established.set()
        await self._reconciler.start()

    async def wait_established(self, timeout: float) -> None:
        await asyncio.wait_for(self._established.wait(), timeout=timeout)

    async def serve_forever(self) -> None:
        assert self._tcp_server is not None and self._grpc_server is not None
        async with self._tcp_server:
            await asyncio.gather(
                self._tcp_server.serve_forever(),
                self._grpc_server.wait_for_termination(),
            )

    async def stop(self) -> None:
        """Stop every background task and server (simulates a process exit)."""
        if self._reconciler is not None:
            await self._reconciler.stop()
        if self._lifecycle_task is not None and not self._lifecycle_task.done():
            self._lifecycle_task.cancel()
            try:
                await self._lifecycle_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._majority_monitor is not None:
            await self._majority_monitor.stop()
        if self.election is not None:
            if self.election.failure_detector is not None:
                await self.election.failure_detector.stop()
            if self.election.time_synchronizer is not None:
                await self.election.time_synchronizer.stop()
        replicator = self.dispatcher.replicator
        if replicator is not None:
            await replicator.aclose()
        await self.lock_manager.stop()
        if self.election is not None:
            await self.election.aclose()
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
        if self._grpc_server is not None:
            await self._grpc_server.stop(grace=0)
        await self.links.close()
        if self.ledger is not None:
            await self.ledger.close()
