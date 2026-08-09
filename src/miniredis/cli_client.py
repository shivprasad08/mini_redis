"""
Standalone CLI REPL client for mini-redis.

Demo script (Manual testing):
─────────────────────────────
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
from typing import Any

from miniredis.protocol import (
    CommandParser,
    ErrorReply,
    ProtocolError,
    encode_array,
    encode_bulk_string,
)


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


async def read_loop(reader: asyncio.StreamReader, response_ready: asyncio.Event) -> None:
    """Continuously read from the server and print responses.
    
    This runs as a background task so that pub/sub messages can be
    printed independently of user input prompts. Sets response_ready
    after every printed message so the foreground loop knows it is
    safe to show the next prompt.
    """
    parser = CommandParser()
    
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                print("\nConnection closed by server.")
                break
                
            parser.feed(data)
            
            while True:
                try:
                    response = parser.try_parse_response()
                    if response is CommandParser._INCOMPLETE:
                        break  # Need more data
                    print(format_resp(response))
                    response_ready.set()
                except ProtocolError as e:
                    print(f"(protocol error) {e}")
                    response_ready.set()
                    break
    except ConnectionError:
        print("\nConnection lost.")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"\nUnexpected error reading from server: {e}")


async def async_input(prompt: str) -> str:
    """Read a line from stdin without blocking the asyncio event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, input, prompt)


async def run_client(host: str, port: int) -> None:
    """Connect to the server and run the REPL loop."""
    print(f"Connecting to {host}:{port}...")
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        print(f"Could not connect to server at {host}:{port}. Is it running?")
        sys.exit(1)
        
    print("Connected. Type 'help' for commands, 'quit' to exit.")

    response_ready = asyncio.Event()
    response_ready.set()  # nothing pending yet, first prompt can show immediately

    # Start the background task to print server responses and pub/sub messages
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
                # Known limitation: whitespace splitting breaks quoted args
                print("Note: Quoted arguments with spaces are not supported in this simple REPL.")
                continue

            # Encode arguments as bulk strings in a RESP array
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

            # Wait for read_loop to print this command's response before
            # showing the next prompt, so output never interleaves with
            # an unrelated push message or the next input line.
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


def main() -> None:
    parser = argparse.ArgumentParser(description="mini-redis CLI client")
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Server hostname (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=6380, help="Server port (default: 6380)"
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_client(args.host, args.port))
    except KeyboardInterrupt:
        print("\nExiting.")


if __name__ == "__main__":
    main()
