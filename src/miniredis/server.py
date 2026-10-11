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

from miniredis.dispatcher import Dispatcher, pubsub_message_framer
from miniredis.protocol import (
    Command,
    CommandParser,
    ProtocolError,
    encode_error,
    encode_simple_string,
)
from miniredis.election.election import PeerEndpoint
from miniredis.storage import Store
from miniredis.pubsub import PubSub

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
    parser.add_argument(
        "--role",
        type=str,
        choices=["primary", "replica"],
        default="primary",
        help=(
            "Bootstrap hint: 'primary' may initialize a brand-new cluster. On restart the node "
            "discovers and follows the current leader instead of trusting this flag."
        ),
    )
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
    parser.add_argument(
        "--mutex-algo",
        type=str,
        choices=["ricart-agrawala", "centralized"],
        default="ricart-agrawala",
        help="Distributed mutual exclusion algorithm: ricart-agrawala or centralized",
    )
    parser.add_argument(
        "--election-algo",
        type=str,
        choices=["bully", "ring"],
        default="bully",
        help="Distributed leader election algorithm: bully or ring (M18)",
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
        # Local import: node.py imports run_server from this module.
        from miniredis.node import MiniRedisNode, NodeConfig

        node = MiniRedisNode(
            NodeConfig(
                node_id=args.node_id,
                host=args.host,
                port=args.port,
                grpc_port=args.grpc_port,
                role=args.role,
                replica_of=list(args.replica_of),
                primary_address=args.primary_address,
                primary_node_id=args.primary_node_id,
                peers={peer.node_id: peer for peer in (_parse_peer(d) for d in args.peer)},
                write_mode=args.write_mode,
                simulate_latency_ms=args.simulate_latency_ms,
                clock_skew_ms=args.clock_skew_ms,
                mutex_algo=args.mutex_algo,
                election_algo=args.election_algo,
            )
        )
        await node.start()
        try:
            await node.serve_forever()
        finally:
            await node.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()
