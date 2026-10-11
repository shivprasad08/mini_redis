"""
FIFO node-to-node links for replication and snapshot markers (M17).

Concurrent unary gRPC calls give no ordering guarantee: two ``ReplicateWrite``
calls issued back to back can be handled by the receiver in either order.
Chandy–Lamport needs FIFO channels — a marker must reach the receiver after
every message the sender sent before it — so every node keeps one
``PeerLink`` per peer:

* **Sender side** — a single task per link drains a queue and sends one RPC
  at a time, waiting for its response before sending the next
  (stop-and-wait). Each request is stamped with ``sender_id``, a per-process
  ``link_incarnation`` and a strictly increasing ``link_seq``.
* **Receiver side** — ``LinkGuard`` accepts a stamped message only if its
  ``link_seq`` is greater than the last one accepted from that sender
  incarnation. A request whose sender gave up after a timeout but which is
  delivered late (after a newer one) is therefore rejected instead of being
  processed out of order.

Replication writes and snapshot markers share the link, so their relative
order on each directed channel is exactly the order in which they were
submitted.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from miniredis.peer_channel import open_peer_channel
from miniredis.replication.proto import replication_pb2_grpc

logger = logging.getLogger(__name__)

RpcFilter = Callable[[str, str], bool]

# Bounded backlog per link; beyond it submissions fail immediately (a peer that
# is down for long recovers through snapshot re-sync, not through replay).
MAX_LINK_BACKLOG = 10_000


@dataclass
class _Outbound:
    rpc_name: str
    request: Any
    timeout_seconds: float
    delay_seconds: float
    future: "asyncio.Future[Any]"


class PeerLink:
    """Ordered (FIFO) outbound channel from this node to one peer."""

    def __init__(
        self,
        sender_id: str,
        peer_id: str,
        address: str,
        incarnation: str,
        rpc_filter: Optional[RpcFilter] = None,
    ) -> None:
        self.sender_id = sender_id
        self.peer_id = peer_id
        self.address = address
        self._incarnation = incarnation
        self._rpc_filter = rpc_filter
        self._channel = open_peer_channel(address)
        self._stub = replication_pb2_grpc.ReplicationServiceStub(self._channel)
        self._queue: asyncio.Queue[Optional[_Outbound]] = asyncio.Queue(maxsize=MAX_LINK_BACKLOG)
        self._task: Optional[asyncio.Task[None]] = None
        self._next_seq = 0
        self._closed = False

    @property
    def backlog(self) -> int:
        return self._queue.qsize()

    def submit(
        self,
        rpc_name: str,
        request: Any,
        timeout_seconds: float = 2.0,
        delay_seconds: float = 0.0,
    ) -> "asyncio.Future[Any]":
        """Queue *request* behind everything submitted earlier; returns its response future.

        Enqueueing is synchronous, so callers that submit while holding a lock
        fix the channel order at that point.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        if self._closed:
            future.set_exception(ConnectionError(f"link to {self.peer_id} is closed"))
            return future
        try:
            self._queue.put_nowait(_Outbound(rpc_name, request, timeout_seconds, delay_seconds, future))
        except asyncio.QueueFull:
            future.set_exception(ConnectionError(f"link to {self.peer_id} backlog is full"))
            return future
        if self._task is None:
            self._task = loop.create_task(self._run())
        return future

    async def _run(self) -> None:
        while not self._closed:
            item = await self._queue.get()
            if item is None or self._closed:
                if item is not None and not item.future.done():
                    item.future.set_exception(ConnectionError(f"link to {self.peer_id} closed"))
                return
            if item.future.cancelled():
                continue
            try:
                response = await self._send(item)
            except asyncio.CancelledError:
                if not item.future.done():
                    item.future.cancel()
                raise
            except Exception as exc:  # delivered to the submitter, never raised here
                if not item.future.done():
                    item.future.set_exception(exc)
            else:
                if not item.future.done():
                    item.future.set_result(response)

    async def _send(self, item: _Outbound) -> Any:
        if self._rpc_filter is not None and not self._rpc_filter(item.rpc_name, self.address):
            raise ConnectionError("partitioned")
        if item.delay_seconds > 0:
            await asyncio.sleep(item.delay_seconds)
        self._next_seq += 1
        item.request.sender_id = self.sender_id
        item.request.link_incarnation = self._incarnation
        item.request.link_seq = self._next_seq
        method = getattr(self._stub, item.rpc_name)
        return await asyncio.wait_for(method(item.request), timeout=item.timeout_seconds)

    async def close(self) -> None:
        self._closed = True
        task = self._task
        if task is not None:
            # Python 3.11's wait_for can swallow a cancellation that races with
            # the RPC completing, so do not rely on a single cancel(): the loop
            # also checks _closed, and a sentinel wakes it from queue.get().
            while not task.done():
                task.cancel()
                try:
                    self._queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass
                await asyncio.wait({task}, timeout=0.2)
            self._task = None
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if item is not None and not item.future.done():
                item.future.set_exception(ConnectionError(f"link to {self.peer_id} closed"))
        await self._channel.close()


class LinkManager:
    """All outbound links of one node, keyed by peer id and by gRPC address."""

    def __init__(
        self,
        node_id: str,
        peers: Mapping[str, Any],
        rpc_filter: Optional[RpcFilter] = None,
    ) -> None:
        self.node_id = node_id
        # A fresh incarnation per process start resets receivers' ordering state.
        self.incarnation = uuid.uuid4().hex
        self._by_peer = {
            peer_id: PeerLink(node_id, peer_id, peer.grpc_address, self.incarnation, rpc_filter)
            for peer_id, peer in peers.items()
            if peer_id != node_id
        }

    def get(self, peer_id: str) -> Optional[PeerLink]:
        return self._by_peer.get(peer_id)

    def for_address(self, address: str) -> Optional[PeerLink]:
        for link in self._by_peer.values():
            if link.address == address:
                return link
        return None

    def peers(self) -> list[str]:
        return sorted(self._by_peer)

    async def close(self) -> None:
        await asyncio.gather(*(link.close() for link in self._by_peer.values()), return_exceptions=True)


class LinkGuard:
    """Receiver-side FIFO enforcement for stamped messages."""

    def __init__(self) -> None:
        self._last: dict[str, tuple[str, int]] = {}
        self.rejected = 0

    def accept(self, sender_id: str, incarnation: str, link_seq: int) -> bool:
        """True if this message is newer than everything accepted from the sender.

        Unstamped messages (``link_seq == 0``, e.g. from callers that do not use
        links) are always accepted; their ordering is not guaranteed.
        """
        if not link_seq or not sender_id:
            return True
        last = self._last.get(sender_id)
        if last is not None and last[0] == incarnation and link_seq <= last[1]:
            self.rejected += 1
            logger.warning(
                "Rejecting out-of-order link message from %s: link_seq %s <= %s",
                sender_id,
                link_seq,
                last[1],
            )
            return False
        self._last[sender_id] = (incarnation, link_seq)
        return True
