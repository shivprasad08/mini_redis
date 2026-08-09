"""
Integration tests for the PubSub subsystem.
"""

from __future__ import annotations

import asyncio
import pytest

from miniredis.pubsub import PubSub
from miniredis.protocol import encode_array, encode_bulk_string


def dummy_framer(channel: str, message: bytes) -> bytes:
    """Mock framer that prefixes messages."""
    return encode_array([
        encode_bulk_string(b"message"),
        encode_bulk_string(channel.encode("utf-8")),
        encode_bulk_string(message)
    ])


class MockWriter:
    """A mock asyncio.StreamWriter for testing PubSub."""
    def __init__(self, raise_on_write: bool = False) -> None:
        self.buffer = bytearray()
        self.raise_on_write = raise_on_write

    def write(self, data: bytes) -> None:
        if self.raise_on_write:
            raise ConnectionError("Mock disconnected")
        self.buffer.extend(data)

    async def drain(self) -> None:
        if self.raise_on_write:
            raise ConnectionError("Mock disconnected")

    def get_extra_info(self, name: str) -> str:
        if name == "peername":
            return "127.0.0.1:mock"
        return ""


@pytest.mark.asyncio
async def test_subscribe_publish() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    writer = MockWriter()
    
    pubsub.subscribe("news", writer)
    assert pubsub.subscription_count(writer) == 1
    
    delivered = await pubsub.publish("news", b"hello world")
    assert delivered == 1
    
    expected = dummy_framer("news", b"hello world")
    assert bytes(writer.buffer) == expected


@pytest.mark.asyncio
async def test_publish_no_subscribers() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    
    delivered = await pubsub.publish("news", b"hello world")
    assert delivered == 0


@pytest.mark.asyncio
async def test_multiple_subscribers() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    w1 = MockWriter()
    w2 = MockWriter()
    w3 = MockWriter()
    
    pubsub.subscribe("news", w1)
    pubsub.subscribe("news", w2)
    pubsub.subscribe("sports", w3)
    
    delivered = await pubsub.publish("news", b"breaking")
    assert delivered == 2
    
    expected = dummy_framer("news", b"breaking")
    assert bytes(w1.buffer) == expected
    assert bytes(w2.buffer) == expected
    assert len(w3.buffer) == 0


@pytest.mark.asyncio
async def test_unsubscribe() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    writer = MockWriter()
    
    pubsub.subscribe("news", writer)
    pubsub.unsubscribe("news", writer)
    
    assert pubsub.subscription_count(writer) == 0
    delivered = await pubsub.publish("news", b"hello")
    assert delivered == 0
    assert len(writer.buffer) == 0


@pytest.mark.asyncio
async def test_unsubscribe_all() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    writer = MockWriter()
    
    pubsub.subscribe("news", writer)
    pubsub.subscribe("sports", writer)
    assert pubsub.subscription_count(writer) == 2
    
    pubsub.unsubscribe_all(writer)
    assert pubsub.subscription_count(writer) == 0
    
    assert await pubsub.publish("news", b"a") == 0
    assert await pubsub.publish("sports", b"b") == 0


@pytest.mark.asyncio
async def test_writer_exception_skipped() -> None:
    pubsub = PubSub(message_framer=dummy_framer)
    w_good = MockWriter()
    w_bad = MockWriter(raise_on_write=True)
    
    pubsub.subscribe("news", w_good)
    pubsub.subscribe("news", w_bad)
    
    # Should deliver to w_good and skip w_bad without crashing
    delivered = await pubsub.publish("news", b"alert")
    assert delivered == 1
    
    expected = dummy_framer("news", b"alert")
    assert bytes(w_good.buffer) == expected
    assert len(w_bad.buffer) == 0
