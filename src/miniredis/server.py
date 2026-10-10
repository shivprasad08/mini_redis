"""
Asyncio TCP server for mini-redis.

Listens on a configurable host/port (default 127.0.0.1:6380) and drives
each client connection through the streaming ``CommandParser`` from
:mod:`miniredis.protocol`.

Architecture
────────────
* One asyncio task per client connection — no threads.
* Each connection owns its own ``CommandParser`` instance so that
  TCP-fragmented input is reassembled independently per client.
* Pipelined commands (multiple complete commands buffered in one read)
  are drained in a tight loop before awaiting the next read.
* Command execution is delegated to a *command handler* callback
  (``CommandHandler``) that is injected into the connection handler.
  Swapping in the real dispatcher later is a one-line change at
  the call-site in ``run_server``.

Connection lifecycle
────────────────────
* ``ProtocolError`` → send an encoded error response, then close the
  connection (the stream is likely desynchronised).
* Client disconnect (empty read / ``ConnectionError``) → close the
  writer, log, and exit quietly.
* Unhandled exceptions in one connection never propagate to the
  server or other connections.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
from typing import Awaitable, Callable, Optional

import grpc

from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.cluster import ClusterMajorityMonitor, WriteAdmissionGate
from miniredis.protocol import (
    Command,
    CommandParser,
    ProtocolError,
    encode_error,
    encode_simple_string,
)
from miniredis.election.election import BullyElection, PeerEndpoint
from miniredis.election.proto import election_pb2_grpc
from miniredis.logical_clock import LamportClock
from miniredis.storage import Store
from miniredis.pubsub import PubSub
from miniredis.health.heartbeat import FailureDetector, HealthServicer
from miniredis.health.proto import health_pb2_grpc
from miniredis.replication.replicator import ReplicaServicer, Replicator
from miniredis.replication.proto import replication_pb2_grpc
from miniredis.time_sync.clock import SynchronizedClock
from miniredis.time_sync.service import TimeServicer, TimeSynchronizer
from miniredis.time_sync.proto import time_pb2_grpc

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Type alias for the pluggable command handler
# ──────────────────────────────────────────────────────────────────────

CommandHandler = Callable[[Command, asyncio.StreamWriter], Awaitable[bytes]]
"""Signature for the function that executes a parsed command and returns
the wire-encoded response bytes.  This is the single integration point
that Module 3 (dispatcher) will replace."""

DisconnectHandler = Callable[[asyncio.StreamWriter], None]
"""Signature for the function called when a client connection closes."""


def non_negative_finite_float(value: str) -> float:
    """Parse CLI float options that represent milliseconds."""
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("value must be finite and non-negative")
    return parsed


def finite_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a number") from exc
    if not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("value must be finite")
    return parsed


# ──────────────────────────────────────────────────────────────────────
# Default command handler factory
# ──────────────────────────────────────────────────────────────────────

def _make_default_handler() -> tuple[CommandHandler, DisconnectHandler]:
    """Create a :class:`Dispatcher` backed by a fresh :class:`Store`
    and :class:`PubSub`, returning its ``handle_command`` and
    ``handle_disconnect`` methods as a tuple."""
    store = Store()
    pubsub = PubSub(pubsub_message_framer)
    dispatcher = Dispatcher(store, pubsub)
    return dispatcher.handle_command, dispatcher.handle_disconnect


# ──────────────────────────────────────────────────────────────────────
# Per-connection handler
# ──────────────────────────────────────────────────────────────────────

# Maximum bytes to read per iteration.  Large enough for typical
# pipelined bursts, small enough to avoid hogging the event loop.
_READ_SIZE: int = 64 * 1024


async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: CommandHandler,
    on_disconnect: Optional[DisconnectHandler] = None,
) -> None:
    """Drive a single client connection to completion.

    Parameters
    ----------
    reader, writer:
        The asyncio stream pair from ``asyncio.start_server``.
    handler:
        The command-execution callback (see :data:`CommandHandler`).
    """
    peer = writer.get_extra_info("peername")
    logger.info("Connection opened: %s", peer)

    parser = CommandParser()

    try:
        while True:
            # ── read ────────────────────────────────────────────────
            try:
                data = await reader.read(_READ_SIZE)
            except ConnectionError:
                logger.info("Connection reset by peer: %s", peer)
                break

            if not data:
                # Client closed the connection cleanly.
                logger.info("Client disconnected: %s", peer)
                break

            # ── parse & dispatch (drain all pipelined commands) ─────
            parser.feed(data)

            try:
                while True:
                    command = parser.try_parse()
                    if command is None:
                        break

                    response = await handler(command, writer)
                    writer.write(response)
                    await writer.drain()

            except ProtocolError as exc:
                logger.warning(
                    "Protocol error from %s: %s — closing connection",
                    peer,
                    exc,
                )
                writer.write(encode_error(f"ERR protocol error: {exc}"))
                try:
                    await writer.drain()
                except ConnectionError:
                    pass
                break

    except Exception:
        logger.exception("Unexpected error handling connection %s", peer)
    finally:
        if on_disconnect is not None:
            on_disconnect(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        logger.info("Connection closed: %s", peer)


# ──────────────────────────────────────────────────────────────────────
# Server lifecycle
# ──────────────────────────────────────────────────────────────────────

async def run_server(
    host: str = "127.0.0.1",
    port: int = 6380,
    handler: Optional[CommandHandler] = None,
    on_disconnect: Optional[DisconnectHandler] = None,
) -> asyncio.Server:
    """Create, start, and return the ``asyncio.Server``.

    Parameters
    ----------
    host, port:
        Bind address.  Defaults to ``127.0.0.1:6380``.
    handler:
        Command execution callback.  Falls back to
        :func:`placeholder_handle_command` when ``None``.

    Returns
    -------
    asyncio.Server
        The running server object (useful for tests that need to
        query the bound address or close the server).
    """
    if handler is None:
        handler, on_disconnect = _make_default_handler()

    # Capture *handler* in a closure so ``asyncio.start_server`` gets
    # the two-argument callback signature it expects.
    cmd_handler = handler  # local binding for the closure
    disconnect_handler = on_disconnect

    async def _client_connected(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_connection(reader, writer, cmd_handler, disconnect_handler)

    server = await asyncio.start_server(
        _client_connected,
        host=host,
        port=port,
    )

    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    logger.info("mini-redis server listening on %s", addrs)

    return server


# ──────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """Start the mini-redis server and block until Ctrl+C."""
    parser = argparse.ArgumentParser(description="mini-redis node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="TCP bind address")
    parser.add_argument("--port", type=int, default=6380, help="TCP bind port")
    parser.add_argument("--grpc-port", type=int, default=6381, help="gRPC bind port")
    parser.add_argument("--role", type=str, choices=["primary", "replica"], default="primary")
    parser.add_argument("--replica-of", action="append", default=[], help="Replica gRPC targets (host:port)")
    parser.add_argument("--node-id", type=str, default=None, help="Stable node identifier (defaults to host:grpc-port)")
    parser.add_argument("--primary-address", type=str, default=None, help="Primary gRPC address monitored by replicas")
    parser.add_argument(
        "--primary-node-id",
        type=str,
        default=None,
        help="Stable id of the primary node monitored by replicas",
    )
    parser.add_argument(
        "--peer",
        action="append",
        default=[],
        help="Known peer in the form node_id,grpc_host:grpc_port,tcp_host:tcp_port",
    )
    parser.add_argument(
        "--write-mode",
        type=str,
        choices=["async", "quorum"],
        default="async",
        help="Primary write mode: fire-and-forget or quorum",
    )
    parser.add_argument(
        "--simulate-latency-ms",
        type=non_negative_finite_float,
        default=0.0,
        help="Per-replication-RPC artificial one-way latency in milliseconds",
    )
    parser.add_argument(
        "--clock-skew-ms",
        type=finite_float,
        default=0.0,
        help="Simulated physical wall-clock skew for this node in milliseconds",
    )
    
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    def _parse_peer(descriptor: str) -> PeerEndpoint:
        try:
            node_id, grpc_address, tcp_address = descriptor.split(",", 2)
        except ValueError as exc:
            raise ValueError(
                "--peer must use node_id,grpc_host:grpc_port,tcp_host:tcp_port"
            ) from exc
        return PeerEndpoint(node_id=node_id, grpc_address=grpc_address, tcp_address=tcp_address)

    async def _run() -> None:
        physical_clock = SynchronizedClock(skew_ms=args.clock_skew_ms)
        store = Store(clock=physical_clock.corrected_time_seconds)
        pubsub = PubSub(pubsub_message_framer)
        node_id = args.node_id or f"{args.host}:{args.grpc_port}"
        tcp_address = f"{args.host}:{args.port}"
        grpc_address = f"{args.host}:{args.grpc_port}"
        peer_map = {peer.node_id: peer for peer in (_parse_peer(descriptor) for descriptor in args.peer)}
        total_cluster_size = len(peer_map) + 1
        write_gate = WriteAdmissionGate(can_serve_writes=(args.role == "primary"))
        lamport_clock = LamportClock()

        grpc_server = grpc.aio.server()
        replicator = None
        failure_detector: FailureDetector | None = None
        majority_monitor: ClusterMajorityMonitor | None = None
        time_synchronizer: TimeSynchronizer | None = None

        health_servicer = HealthServicer(node_id=node_id, role=args.role, lamport_clock=lamport_clock)
        health_pb2_grpc.add_HealthServiceServicer_to_server(health_servicer, grpc_server)

        replication_pb2_grpc.add_ReplicationServiceServicer_to_server(
            ReplicaServicer(store, node_id=node_id, lamport_clock=lamport_clock), grpc_server
        )
        time_pb2_grpc.add_TimeServiceServicer_to_server(
            TimeServicer(node_id=node_id, clock=physical_clock, lamport_clock=lamport_clock),
            grpc_server,
        )

        election = BullyElection(
            node_id=node_id,
            tcp_address=tcp_address,
            grpc_address=grpc_address,
            peers=peer_map,
            dispatcher=Dispatcher(
                store,
                pubsub,
                writes_enabled=(args.role == "primary"),
                write_gate=write_gate,
                clock=physical_clock.corrected_time_seconds,
            ),
            total_cluster_size=total_cluster_size,
            failure_detector=None,
            write_gate=write_gate,
            role=args.role,
            write_mode=args.write_mode,
            simulate_latency_ms=args.simulate_latency_ms,
            lamport_clock=lamport_clock,
        )
        election_pb2_grpc.add_ElectionServiceServicer_to_server(election.servicer, grpc_server)

        majority_monitor = ClusterMajorityMonitor(
            node_id=node_id,
            peer_health_addresses={peer_id: peer.grpc_address for peer_id, peer in peer_map.items()},
            total_cluster_size=total_cluster_size,
            gate=write_gate,
            lamport_clock=lamport_clock,
        )

        async def _on_majority_change(majority_reachable: bool) -> None:
            await election.handle_majority_change(majority_reachable)

        majority_monitor.on_state_change(_on_majority_change)

        grpc_server.add_insecure_port(grpc_address)
        await grpc_server.start()
        logger.info("gRPC server listening on %s", grpc_address)

        await majority_monitor.start()
        
        if args.role == "primary":
            replicator = Replicator(
                args.replica_of,
                total_cluster_size=total_cluster_size,
                write_mode=args.write_mode,
                simulate_latency_ms=args.simulate_latency_ms,
                write_gate=write_gate,
                node_id=node_id,
                lamport_clock=lamport_clock,
            )
            election.dispatcher.set_replicator(replicator)
        else:
            primary_address = args.primary_address
            if not primary_address:
                raise ValueError("--primary-address is required when --role=replica")

            primary_node_id = args.primary_node_id or primary_address

            failure_detector = FailureDetector(
                peer_node_id=primary_node_id,
                peer_address=primary_address,
                heartbeat_interval_seconds=1.0,
                heartbeat_timeout_seconds=0.5,
                failure_threshold=3,
                sender_id=node_id,
                rpc_filter=None,
                lamport_clock=lamport_clock,
            )

            election.attach_failure_detector(failure_detector)
            election.dispatcher.set_writes_enabled(False)

            def _log_state_change(peer_id: str, old_state: str, new_state: str) -> None:
                del peer_id, old_state
                if new_state == "suspected":
                    logger.warning("PRIMARY SUSPECTED DOWN")
                elif new_state == "dead":
                    logger.error("PRIMARY DECLARED DEAD")
                    asyncio.create_task(election.start_election())
                elif new_state == "alive":
                    logger.info("PRIMARY RECOVERED")

            failure_detector.on_state_change(_log_state_change)
            await failure_detector.start()

            time_synchronizer = TimeSynchronizer(
                node_id=node_id,
                peer_address=primary_address,
                clock=physical_clock,
                interval_seconds=1.0,
                timeout_seconds=0.5,
                lamport_clock=lamport_clock,
            )
            election.attach_time_synchronizer(time_synchronizer)
            await time_synchronizer.start()

        dispatcher = election.dispatcher
        if args.role == "primary":
            dispatcher.set_writes_enabled(True)
            dispatcher.set_replicator(replicator)

        tcp_server = await run_server(
            host=args.host, 
            port=args.port, 
            handler=dispatcher.handle_command,
            on_disconnect=dispatcher.handle_disconnect,
        )
        
        try:
            async with tcp_server:
                await asyncio.gather(
                    tcp_server.serve_forever(),
                    grpc_server.wait_for_termination(),
                )
        finally:
            if majority_monitor is not None:
                await majority_monitor.stop()
            if failure_detector is not None:
                await failure_detector.stop()
            if time_synchronizer is not None:
                await time_synchronizer.stop()
            if replicator is not None:
                await replicator.aclose()
            if grpc_server:
                await grpc_server.stop(grace=0)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()
