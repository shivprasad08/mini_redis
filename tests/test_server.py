"""
Integration tests for the mini-redis asyncio TCP server.

Each test spins up a real server on an ephemeral port (port 0),
connects as a raw TCP client, and exercises the full network path:
client bytes → TCP → StreamReader → CommandParser → handler → response.
"""

from __future__ import annotations

import asyncio

import pytest

from miniredis.protocol import (
    encode_array,
    encode_bulk_string,
)
from miniredis.server import run_server


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _make_command(*args: bytes) -> bytes:
    """Build a wire-encoded command (array of bulk strings)."""
    return encode_array([encode_bulk_string(a) for a in args])


async def _open_client(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open a raw TCP connection to the server."""
    return await asyncio.open_connection(host, port)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
async def server_addr():
    """Start a server on an ephemeral port, yield (host, port), then
    tear it down."""
    server = await run_server(host="127.0.0.1", port=0)
    # Extract the actual bound port.
    sock = server.sockets[0]
    host, port = sock.getsockname()[:2]

    yield host, port

    server.close()
    await server.wait_closed()


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ping_pong(server_addr: tuple[str, int]) -> None:
    """Send a PING command, expect +PONG\\r\\n back."""
    host, port = server_addr
    reader, writer = await _open_client(host, port)

    try:
        writer.write(_make_command(b"PING"))
        await writer.drain()

        response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
        assert response == b"+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_unknown_command_returns_error(
    server_addr: tuple[str, int],
) -> None:
    """An unrecognised command should return an ERR response."""
    host, port = server_addr
    reader, writer = await _open_client(host, port)

    try:
        writer.write(_make_command(b"FROBNICATE", b"foo"))
        await writer.drain()

        response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
        assert response.startswith(b"-ERR unknown command 'FROBNICATE'")
        assert response.endswith(b"\r\n")
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_partial_command_across_two_writes(
    server_addr: tuple[str, int],
) -> None:
    """Split a command across two TCP writes to prove the server's
    streaming parser reassembles it correctly."""
    host, port = server_addr
    reader, writer = await _open_client(host, port)

    try:
        full = _make_command(b"PING")
        mid = len(full) // 2

        # First half.
        writer.write(full[:mid])
        await writer.drain()

        # Brief pause so the server processes the partial chunk.
        await asyncio.sleep(0.05)

        # Second half.
        writer.write(full[mid:])
        await writer.drain()

        response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
        assert response == b"+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_pipelined_commands(server_addr: tuple[str, int]) -> None:
    """Send two commands in a single write; the server must respond
    to both."""
    host, port = server_addr
    reader, writer = await _open_client(host, port)

    try:
        data = _make_command(b"PING") + _make_command(b"PING")
        writer.write(data)
        await writer.drain()

        # Read enough for two responses.
        response = b""
        while response.count(b"+PONG\r\n") < 2:
            chunk = await asyncio.wait_for(reader.read(1024), timeout=2.0)
            assert chunk  # non-empty
            response += chunk

        assert response == b"+PONG\r\n+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_malformed_input_closes_connection(
    server_addr: tuple[str, int],
) -> None:
    """Sending garbage bytes should elicit an error and a closed connection."""
    host, port = server_addr
    reader, writer = await _open_client(host, port)

    try:
        writer.write(b"!GARBAGE\r\n")
        await writer.drain()

        response = await asyncio.wait_for(reader.read(1024), timeout=2.0)
        assert response.startswith(b"-ERR")

        # The server should close the connection after the error.
        remaining = await asyncio.wait_for(reader.read(1024), timeout=2.0)
        assert remaining == b""  # EOF
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_multiple_concurrent_clients(
    server_addr: tuple[str, int],
) -> None:
    """Multiple clients connected simultaneously must each get their
    own independent responses."""
    host, port = server_addr

    async def _client_ping(client_id: int) -> bytes:
        reader, writer = await _open_client(host, port)
        try:
            writer.write(_make_command(b"PING"))
            await writer.drain()
            return await asyncio.wait_for(reader.read(1024), timeout=2.0)
        finally:
            writer.close()
            await writer.wait_closed()

    results = await asyncio.gather(
        *[_client_ping(i) for i in range(5)]
    )
    assert all(r == b"+PONG\r\n" for r in results)


@pytest.mark.asyncio
async def test_client_disconnect_does_not_crash_server(
    server_addr: tuple[str, int],
) -> None:
    """Abruptly closing a client connection must not crash the server
    or affect other clients."""
    host, port = server_addr

    # Client 1: connect and immediately close.
    _, writer1 = await _open_client(host, port)
    writer1.close()
    await writer1.wait_closed()

    # Brief pause for the server to process the disconnect.
    await asyncio.sleep(0.05)

    # Client 2: normal PING should still work.
    reader2, writer2 = await _open_client(host, port)
    try:
        writer2.write(_make_command(b"PING"))
        await writer2.drain()
        response = await asyncio.wait_for(reader2.read(1024), timeout=2.0)
        assert response == b"+PONG\r\n"
    finally:
        writer2.close()
        await writer2.wait_closed()
