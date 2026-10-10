"""Lamport logical-clock utilities for cluster RPC tracing."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

logger = logging.getLogger("miniredis.lamport")


class LamportClock:
    """Small asyncio-safe Lamport clock.

    The methods are async because cluster code can stamp messages from several
    background tasks in the same event loop.
    """

    def __init__(self, initial: int = 0) -> None:
        if not isinstance(initial, int) or initial < 0:
            raise ValueError("initial Lamport timestamp must be a non-negative integer")
        self._value = initial
        self._lock = asyncio.Lock()

    async def tick(self) -> int:
        async with self._lock:
            self._value += 1
            return self._value

    async def receive(self, remote_timestamp: int) -> int:
        if remote_timestamp < 0:
            raise ValueError("remote Lamport timestamp must be non-negative")
        async with self._lock:
            self._value = max(self._value, int(remote_timestamp)) + 1
            return self._value

    async def current(self) -> int:
        async with self._lock:
            return self._value


@dataclass(frozen=True)
class LamportEvent:
    timestamp: int
    node_id: str
    event_type: str
    message_id: str
    description: str


def log_lamport_event(
    *,
    timestamp: int,
    node_id: str,
    event_type: str,
    message_id: str,
    description: str,
) -> None:
    logger.info(
        "lamport_event ts=%s node=%s event=%s message=%s description=%s",
        timestamp,
        node_id,
        event_type,
        message_id,
        description.replace(" ", "_"),
    )
