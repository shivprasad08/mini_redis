"""
Standalone CLI REPL client for mini-redis.

Supports two connection modes:

1. **Single-server** (default):
   ``python -m miniredis.cli_client --host 127.0.0.1 --port 6380``

2. **Sharded** (consistent-hashing):
   ``python -m miniredis.cli_client --shard node-a=127.0.0.1:6380 --shard node-b=127.0.0.1:6381``

   In sharded mode the client builds a consistent hash ring,
   opens a persistent TCP connection to every shard at startup,
   and routes each key-based command (GET/SET/DEL/EXPIRE/EXISTS)
   to whichever shard owns the key.  Non-key commands (PING,
   SUBSCRIBE, PUBLISH) are sent to the first shard.

Demo script — Sharded mode
──────────────────────────
1. Start two independent single-node primaries:
   $ python -m miniredis.server --port 6380
   $ python -m miniredis.server --port 6381

2. Connect the CLI in sharded mode:
   $ python -m miniredis.cli_client \\
       --shard node-a=127.0.0.1:6380 \\
       --shard node-b=127.0.0.1:6381

3. Issue some writes and observe routing:
   mini-redis> SET user:1 alice
   mini-redis[node-a]> OK
   mini-redis> SET user:2 bob
   mini-redis[node-b]> OK
   mini-redis> GET user:1
   mini-redis[node-a]> alice
   mini-redis> GET user:2
   mini-redis[node-b]> bob

   Each response is prefixed with the shard that handled it.

Demo script — Single-server mode
─────────────────────────────────
1. Start the server in one terminal:
   $ python -m miniredis.server

2. Open two more terminals and start the CLI in both:
   $ python -m miniredis.cli_client

3. In Terminal A, subscribe to a channel:
   mini-redis> SUBSCRIBE news

4. In Terminal B, publish a message:
   mini-redis> PUBLISH news "Breaking story"

5. Observe Terminal A receiving the message asynchronously:
   [news] Breaking story

6. In Terminal B, test basic KV operations:
   mini-redis> SET mykey myvalue EX 60
   mini-redis> GET mykey
   mini-redis> EXISTS mykey
   mini-redis> DEL mykey
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any, Optional

from miniredis.protocol import (
    CommandParser,
    ErrorReply,
    ProtocolError,
    encode_array,
    encode_bulk_string,
)


# ── Response formatting ─────────────────────────────────────────────

def format_resp(val: Any, indent: str = "") -> str:
    """Recursively format a RESP parsed value for human-friendly display."""
    # Special handling for pub/sub push messages: ["message", channel, payload]
    if isinstance(val, list) and len(val) == 3 and val[0] == b"message":
        channel = format_resp(val[1])
        payload = format_resp(val[2])
        return f"{indent}[{channel}] {payload}"

    if val is None:
        return f"{indent}(nil)"
    elif isinstance(val, int):
        return f"{indent}(integer) {val}"
    elif isinstance(val, ErrorReply):
        return f"{indent}(error) {val.message}"
    elif isinstance(val, str):
        # Simple strings from the parser are already decoded to str
        return f"{indent}{val}"
    elif isinstance(val, bytes):
        try:
            return f"{indent}{val.decode('utf-8')}"
        except UnicodeDecodeError:
            return f"{indent}{repr(val)}"
    elif isinstance(val, list):
        if not val:
            return f"{indent}(empty array)"
        lines = []
        for i, item in enumerate(val):
            prefix = f"{indent}{i + 1}) "
            if isinstance(item, list):
                lines.append(prefix)
                lines.append(format_resp(item, indent + "   "))
            else:
                lines.append(f"{prefix}{format_resp(item, '')}")
        return "\n".join(lines)
    else:
        return f"{indent}{val}"


# ── Background read loop ────────────────────────────────────────────

async def read_loop(
    reader: asyncio.StreamReader,
    response_ready: asyncio.Event,
    label: str = "",
) -> None:
    """Continuously read from the server and print responses.

    This runs as a background task so that pub/sub messages can be
    printed independently of user input prompts.  Sets *response_ready*
    after every printed message so the foreground loop knows it is
    safe to show the next prompt.

    *label* is an optional prefix (e.g. ``"[node-a]"``) prepended to
    each printed response in sharded mode.
    """
    parser = CommandParser()

    try:
        while True:
            data = await reader.read(4096)
            if not data:
                print(f"\n{label}Connection closed by server.")
                break

            parser.feed(data)

            while True:
                try:
                    response = parser.try_parse_response()
                    if response is CommandParser._INCOMPLETE:
                        break  # Need more data
                    print(f"{label}{format_resp(response)}")
                    response_ready.set()
                except ProtocolError as e:
                    print(f"{label}(protocol error) {e}")
                    response_ready.set()
                    break
    except ConnectionError:
        print(f"\n{label}Connection lost.")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"\n{label}Unexpected error reading from server: {e}")


# ── Helpers ──────────────────────────────────────────────────────────

async def async_input(prompt: str) -> str:
    """Read a line from stdin without blocking the asyncio event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, input, prompt)


# Commands whose first argument is the key used for shard routing.
_KEY_COMMANDS = frozenset({"GET", "SET", "DEL", "EXPIRE", "EXISTS"})


# ── Single-server REPL ──────────────────────────────────────────────

async def run_client(host: str, port: int) -> None:
    """Connect to a single server and run the REPL loop."""
    print(f"Connecting to {host}:{port}...")
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Could not connect to server at {host}:{port}. Is it running?")
        sys.exit(1)

    print("Connected. Type 'help' for commands, 'quit' to exit.")

    response_ready = asyncio.Event()
    response_ready.set()  # nothing pending yet, first prompt can show immediately

    read_task = asyncio.create_task(read_loop(reader, response_ready))

    try:
        while True:
            try:
                line = await async_input("mini-redis> ")
            except EOFError:
                break

            line = line.strip()
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd in ("quit", "exit"):
                break
            elif cmd == "help":
                print("Supported commands: GET, SET, DEL, EXPIRE, EXISTS, SUBSCRIBE, PUBLISH, PING")
                print("Client commands: quit, exit, help")
                print("Note: Quoted arguments with spaces are not supported in this simple REPL.")
                continue

            encoded = encode_array([
                encode_bulk_string(arg.encode("utf-8")) for arg in parts
            ])

            response_ready.clear()
            writer.write(encoded)
            try:
                await writer.drain()
            except ConnectionError:
                print("Connection lost while sending data.")
                break

            try:
                await asyncio.wait_for(response_ready.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass  # don't hang forever if the server never replies

    except KeyboardInterrupt:
        pass
    finally:
        read_task.cancel()
        writer.close()
        await writer.wait_closed()


# ── Sharded REPL ────────────────────────────────────────────────────

async def run_sharded_client(shards: dict[str, tuple[str, int]]) -> None:
    """Connect to multiple shard servers and route commands by key.

    *shards* is a dict mapping ``node_id`` → ``(host, port)``.
    """
    from miniredis.sharding.hashring import ConsistentHashRing

    ring = ConsistentHashRing()
    connections: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
    read_tasks: list[asyncio.Task] = []
    # One event per shard so we can wait for the correct shard's reply.
    shard_events: dict[str, asyncio.Event] = {}

    # Build the ring and open connections.
    for node_id, (host, port) in shards.items():
        ring.add_node(node_id)
        print(f"Connecting to shard {node_id} at {host}:{port}...")
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except ConnectionRefusedError:
            print(f"Could not connect to shard {node_id} at {host}:{port}. Is it running?")
            # Clean up already-opened connections.
            for _, (_, w) in connections.items():
                w.close()
            sys.exit(1)
        connections[node_id] = (reader, writer)
        ev = asyncio.Event()
        ev.set()
        shard_events[node_id] = ev
        label = f"mini-redis[{node_id}]> "
        read_tasks.append(asyncio.create_task(read_loop(reader, ev, label)))

    # Order-preserving list of node_ids so we have a "default" shard.
    ordered_nodes = list(shards.keys())
    default_node = ordered_nodes[0]

    print(f"Connected to {len(shards)} shards. Type 'help' for commands, 'quit' to exit.")

    try:
        while True:
            try:
                line = await async_input("mini-redis> ")
            except EOFError:
                break

            line = line.strip()
            if not line:
                continue

            parts = line.split()
            cmd_name = parts[0].upper()

            if cmd_name in ("QUIT", "EXIT"):
                break
            elif cmd_name == "HELP":
                print("Supported commands: GET, SET, DEL, EXPIRE, EXISTS, SUBSCRIBE, PUBLISH, PING")
                print("Client commands: quit, exit, help")
                print(f"Shards: {', '.join(ordered_nodes)}")
                continue

            # Decide which shard gets this command.
            if cmd_name in _KEY_COMMANDS and len(parts) >= 2:
                key = parts[1]
                target_node = ring.get_node(key)
                if target_node is None:
                    print("(error) No shards available on the ring.")
                    continue
            else:
                # Non-key commands (PING, SUBSCRIBE, PUBLISH) go to
                # the first shard — they aren't naturally key-shardable.
                target_node = default_node

            reader, writer = connections[target_node]
            ev = shard_events[target_node]

            encoded = encode_array([
                encode_bulk_string(arg.encode("utf-8")) for arg in parts
            ])

            ev.clear()
            writer.write(encoded)
            try:
                await writer.drain()
            except ConnectionError:
                print(f"Connection lost to shard {target_node}.")
                break

            try:
                await asyncio.wait_for(ev.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    except KeyboardInterrupt:
        pass
    finally:
        for task in read_tasks:
            task.cancel()
        for _, (_, writer) in connections.items():
            writer.close()
            await writer.wait_closed()


# ── CLI entry point ──────────────────────────────────────────────────

def _parse_shard(value: str) -> tuple[str, str, int]:
    """Parse a ``node_id=host:port`` string.

    Returns ``(node_id, host, port)``.
    """
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            f"Invalid shard format: {value!r}. Expected node_id=host:port"
        )
    node_id, addr = value.split("=", 1)
    if ":" not in addr:
        raise argparse.ArgumentTypeError(
            f"Invalid address in shard: {addr!r}. Expected host:port"
        )
    host, port_str = addr.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid port in shard: {port_str!r}"
        )
    return node_id, host, port


def main() -> None:
    parser = argparse.ArgumentParser(description="mini-redis CLI client")
    parser.add_argument(
        "--host", type=str, default="127.0.0.1",
        help="Server hostname (default: 127.0.0.1). Used in single-server mode.",
    )
    parser.add_argument(
        "--port", type=int, default=6380,
        help="Server port (default: 6380). Used in single-server mode.",
    )
    parser.add_argument(
        "--shard", action="append", default=[],
        metavar="NODE_ID=HOST:PORT",
        help=(
            "Add a shard for consistent-hash routing. Repeatable. "
            "Example: --shard node-a=127.0.0.1:6380 --shard node-b=127.0.0.1:6381"
        ),
    )
    args = parser.parse_args()

    try:
        if args.shard:
            # Parse all --shard arguments into a dict.
            shards: dict[str, tuple[str, int]] = {}
            for raw in args.shard:
                node_id, host, port = _parse_shard(raw)
                shards[node_id] = (host, port)
            asyncio.run(run_sharded_client(shards))
        else:
            asyncio.run(run_client(args.host, args.port))
    except KeyboardInterrupt:
        print("\nExiting.")


if __name__ == "__main__":
    main()
