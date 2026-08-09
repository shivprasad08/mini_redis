"""
Tests for the command dispatcher.

Each test constructs a :class:`~miniredis.protocol.Command`, passes it
to :meth:`Dispatcher.handle_command`, and asserts that the returned
wire bytes match the expected RESP encoding.
"""

from __future__ import annotations

import asyncio
import pytest

from miniredis.dispatcher import Dispatcher
from miniredis.protocol import (
    Command,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
)
from miniredis.storage import Store
from miniredis.pubsub import PubSub


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

class MockWriter:
    """A minimal mock asyncio.StreamWriter for testing dispatcher."""
    def __init__(self) -> None:
        pass


def _cmd(name: str, *args: bytes) -> Command:
    return Command(name=name.upper(), args=list(args))


@pytest.fixture
def dispatcher() -> Dispatcher:
    # Use a dummy framer since we don't test PubSub internals here
    def dummy_framer(c: str, m: bytes) -> bytes: return b""
    return Dispatcher(Store(), PubSub(dummy_framer))


# ──────────────────────────────────────────────────────────────────────
# PING
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ping(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("PING"), MockWriter()) # type: ignore
    assert result == encode_simple_string("PONG")


# ──────────────────────────────────────────────────────────────────────
# GET
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_missing(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("GET", b"nokey"), MockWriter()) # type: ignore
    assert result == encode_bulk_string(None)


@pytest.mark.asyncio
async def test_get_existing(dispatcher: Dispatcher) -> None:
    await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    result = await dispatcher.handle_command(_cmd("GET", b"k"), MockWriter()) # type: ignore
    assert result == encode_bulk_string(b"v")


# ──────────────────────────────────────────────────────────────────────
# SET
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_basic(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    assert result == encode_simple_string("OK")


@pytest.mark.asyncio
async def test_set_with_ex(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(
        _cmd("SET", b"k", b"v", b"EX", b"60"), MockWriter() # type: ignore
    )
    assert result == encode_simple_string("OK")

    # Key should be readable.
    get_result = await dispatcher.handle_command(_cmd("GET", b"k"), MockWriter()) # type: ignore
    assert get_result == encode_bulk_string(b"v")


@pytest.mark.asyncio
async def test_set_ex_case_insensitive(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(
        _cmd("SET", b"k", b"v", b"ex", b"10"), MockWriter() # type: ignore
    )
    assert result == encode_simple_string("OK")


@pytest.mark.asyncio
async def test_set_ex_invalid_seconds(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(
        _cmd("SET", b"k", b"v", b"EX", b"notanum"), MockWriter() # type: ignore
    )
    assert result.startswith(b"-ERR")


@pytest.mark.asyncio
async def test_set_ex_zero_seconds(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(
        _cmd("SET", b"k", b"v", b"EX", b"0"), MockWriter() # type: ignore
    )
    assert result.startswith(b"-ERR")


@pytest.mark.asyncio
async def test_set_unsupported_option(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(
        _cmd("SET", b"k", b"v", b"PX", b"100"), MockWriter() # type: ignore
    )
    assert result.startswith(b"-ERR")


# ──────────────────────────────────────────────────────────────────────
# DEL
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_del_existing(dispatcher: Dispatcher) -> None:
    await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    result = await dispatcher.handle_command(_cmd("DEL", b"k"), MockWriter()) # type: ignore
    assert result == encode_integer(1)


@pytest.mark.asyncio
async def test_del_missing(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("DEL", b"nope"), MockWriter()) # type: ignore
    assert result == encode_integer(0)


# ──────────────────────────────────────────────────────────────────────
# EXPIRE
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_expire_existing(dispatcher: Dispatcher) -> None:
    await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    result = await dispatcher.handle_command(_cmd("EXPIRE", b"k", b"60"), MockWriter()) # type: ignore
    assert result == encode_integer(1)


@pytest.mark.asyncio
async def test_expire_missing(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("EXPIRE", b"nope", b"60"), MockWriter()) # type: ignore
    assert result == encode_integer(0)


@pytest.mark.asyncio
async def test_expire_invalid_seconds(dispatcher: Dispatcher) -> None:
    await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    result = await dispatcher.handle_command(
        _cmd("EXPIRE", b"k", b"notanum"), MockWriter() # type: ignore
    )
    assert result.startswith(b"-ERR")


# ──────────────────────────────────────────────────────────────────────
# EXISTS
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_exists_true(dispatcher: Dispatcher) -> None:
    await dispatcher.handle_command(_cmd("SET", b"k", b"v"), MockWriter()) # type: ignore
    result = await dispatcher.handle_command(_cmd("EXISTS", b"k"), MockWriter()) # type: ignore
    assert result == encode_integer(1)


@pytest.mark.asyncio
async def test_exists_false(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("EXISTS", b"nope"), MockWriter()) # type: ignore
    assert result == encode_integer(0)


# ──────────────────────────────────────────────────────────────────────
# Arity / unknown-command errors
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_wrong_arity_get(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("GET"), MockWriter()) # type: ignore
    assert b"wrong number of arguments" in result
    assert b"GET" in result


@pytest.mark.asyncio
async def test_wrong_arity_set(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("SET", b"key"), MockWriter()) # type: ignore
    assert b"wrong number of arguments" in result


@pytest.mark.asyncio
async def test_wrong_arity_del(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("DEL"), MockWriter()) # type: ignore
    assert b"wrong number of arguments" in result


@pytest.mark.asyncio
async def test_wrong_arity_expire(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("EXPIRE", b"k"), MockWriter()) # type: ignore
    assert b"wrong number of arguments" in result


@pytest.mark.asyncio
async def test_wrong_arity_exists(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("EXISTS"), MockWriter()) # type: ignore
    assert b"wrong number of arguments" in result


@pytest.mark.asyncio
async def test_unknown_command(dispatcher: Dispatcher) -> None:
    result = await dispatcher.handle_command(_cmd("FLUSHALL"), MockWriter()) # type: ignore
    assert result == encode_error("ERR unknown command 'FLUSHALL'")
