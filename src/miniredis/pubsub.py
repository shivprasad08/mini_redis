"""
Pub/Sub channel registry for mini-redis.

This module implements a protocol-agnostic channel/subscriber registry.
It deals in raw ``bytes`` payloads and ``asyncio.StreamWriter`` handles —
it does **not** know about RESP framing.  The RESP-specific message
envelope (the ``["message", channel, payload]`` array) is constructed by
the *caller* (the dispatcher) before calling :meth:`PubSub.publish`,
keeping this class reusable as a generic async fan-out primitive.

Subscriber handles
──────────────────
We store ``asyncio.StreamWriter`` directly as the subscriber handle.
This is the simplest approach: the writer is already the per-connection
object we need to push bytes to, it's hashable (identity-based), and
the server already has it in scope.  A wrapper object would add
indirection without benefit at this stage.

Known simplification
────────────────────
Real Redis enforces that a connection in subscribe mode can only issue
``SUBSCRIBE``, ``UNSUBSCRIBE``, ``PSUBSCRIBE``, ``PUNSUBSCRIBE``, and
``PING``.  This implementation does **not** enforce that restriction —
a subscribed client may still issue data commands.  This is documented
as a deliberate simplification for the course project; enforcing it
would require per-connection modal state tracked in the server layer.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

# Type alias for the function that builds wire-format bytes from a
# channel name and raw payload.  The dispatcher injects this so that
# PubSub itself stays protocol-agnostic.
MessageFramer = Callable[[str, bytes], bytes]


class PubSub:
    """Async fan-out channel registry.

    Parameters
    ----------
    message_framer:
        A callable ``(channel: str, payload: bytes) -> bytes`` that
        wraps the channel name and payload in whatever wire format the
        protocol requires (e.g. a RESP array).  This is injected by
        the dispatcher at construction time so that PubSub itself has
        no dependency on :mod:`miniredis.protocol`.
    """

    def __init__(self, message_framer: MessageFramer) -> None:
        # channel name (str) -> set of asyncio.StreamWriter
        self._channels: dict[str, set[asyncio.StreamWriter]] = {}
        self._frame: MessageFramer = message_framer

    # ── subscription management ─────────────────────────────────────

    def subscribe(self, channel: str, subscriber: asyncio.StreamWriter) -> None:
        """Register *subscriber* to receive messages on *channel*."""
        if channel not in self._channels:
            self._channels[channel] = set()
        self._channels[channel].add(subscriber)

    def unsubscribe(self, channel: str, subscriber: asyncio.StreamWriter) -> None:
        """Remove *subscriber* from *channel*.

        If the channel's subscriber set becomes empty, the channel
        entry is deleted entirely to avoid leaking empty sets.
        """
        subs = self._channels.get(channel)
        if subs is None:
            return
        subs.discard(subscriber)
        if not subs:
            del self._channels[channel]

    def unsubscribe_all(self, subscriber: asyncio.StreamWriter) -> None:
        """Remove *subscriber* from every channel it belongs to.

        Called on client disconnect so pub/sub state doesn't leak.
        """
        empty: list[str] = []
        for channel, subs in self._channels.items():
            subs.discard(subscriber)
            if not subs:
                empty.append(channel)
        for channel in empty:
            del self._channels[channel]

    # ── publishing ──────────────────────────────────────────────────

    async def publish(self, channel: str, message: bytes) -> int:
        """Deliver *message* to every subscriber of *channel*.

        Returns the number of subscribers the message was successfully
        written to.  A failed write to one subscriber (e.g. a dropped
        connection) is logged and skipped — delivery continues to the
        remaining subscribers.
        """
        subs = self._channels.get(channel)
        if not subs:
            return 0

        frame = self._frame(channel, message)
        delivered = 0

        for writer in list(subs):  # snapshot — set may mutate
            try:
                writer.write(frame)
                await writer.drain()
                delivered += 1
            except Exception:
                logger.warning(
                    "Failed to deliver pub/sub message on channel %r "
                    "to %s — skipping",
                    channel,
                    writer.get_extra_info("peername"),
                )
        return delivered

    # ── introspection (useful for subscribe-ack counts) ─────────────

    def subscription_count(self, subscriber: asyncio.StreamWriter) -> int:
        """Return the number of channels *subscriber* is currently in."""
        return sum(
            1
            for subs in self._channels.values()
            if subscriber in subs
        )
