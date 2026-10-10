"""
Consistent hash ring for client-side key sharding.

This module implements a consistent hashing ring that maps arbitrary
keys to a set of physical nodes.  It is used by the CLI client to
route each KV command to the correct shard without a central proxy.

Why consistent hashing?
───────────────────────
Naive modulo hashing (``hash(key) % N``) remaps nearly every key when
a node is added or removed.  Consistent hashing minimises disruption:
only keys that were mapped to the changed node need to move, while
the vast majority of keys stay on the same node they were on before.

Deterministic hashing
─────────────────────
We use ``hashlib.md5`` (truncated to 8 bytes → 64-bit integer) instead
of Python's built-in ``hash()``.  Python randomises ``hash()`` across
processes via ``PYTHONHASHSEED``, which would cause different client
processes (or a client vs. a server) to disagree on which node owns a
given key.  ``md5`` is deterministic and portable.

Virtual nodes
─────────────
Each physical node is represented by ``VIRTUAL_NODES_PER_PHYSICAL``
(150) positions on the ring.  This scatters each node's ownership
across the hash space, dramatically improving balance when there are
only a handful of physical nodes.
"""

from __future__ import annotations

import bisect
import hashlib
from typing import Optional


# Number of virtual-node positions per physical node on the ring.
VIRTUAL_NODES_PER_PHYSICAL: int = 150


def _hash_key(data: bytes) -> int:
    """Return a deterministic 64-bit integer hash of *data*.

    Uses MD5 (truncated to the first 8 bytes) so the result is
    identical across Python processes regardless of PYTHONHASHSEED.
    """
    digest = hashlib.md5(data).digest()
    return int.from_bytes(digest[:8], "big")


class ConsistentHashRing:
    """A consistent hash ring mapping keys to physical node IDs.

    Usage::

        ring = ConsistentHashRing()
        ring.add_node("node-a")
        ring.add_node("node-b")

        owner = ring.get_node(b"mykey")   # → "node-a" or "node-b"
    """

    def __init__(self) -> None:
        # Sorted list of (hash_position, physical_node_id) tuples.
        # Kept sorted by hash_position for O(log n) bisect lookups.
        self._ring: list[tuple[int, str]] = []
        # Track which physical nodes have been added so add_node is
        # idempotent and remove_node can find all virtual entries.
        self._nodes: set[str] = set()

    # ── public API ──────────────────────────────────────────────────

    def add_node(self, node_id: str) -> None:
        """Add a physical node and its virtual nodes to the ring.

        Idempotent: calling ``add_node("x")`` twice has no effect the
        second time — virtual nodes are not duplicated.
        """
        if node_id in self._nodes:
            return  # already present, nothing to do

        self._nodes.add(node_id)

        for i in range(VIRTUAL_NODES_PER_PHYSICAL):
            vnode_key = f"{node_id}#{i}".encode("utf-8")
            pos = _hash_key(vnode_key)
            # Insert into sorted position.
            bisect.insort(self._ring, (pos, node_id))

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual nodes from the ring."""
        if node_id not in self._nodes:
            return

        self._nodes.discard(node_id)
        self._ring = [
            (pos, nid) for pos, nid in self._ring if nid != node_id
        ]

    def get_node(self, key: bytes | str) -> Optional[str]:
        """Return the physical node responsible for *key*.

        Walks clockwise from the key's hash position to the nearest
        virtual node on the ring.  Returns ``None`` if the ring is
        empty.
        """
        if not self._ring:
            return None

        if isinstance(key, str):
            key = key.encode("utf-8")

        pos = _hash_key(key)

        # bisect_right finds the first ring entry strictly greater
        # than our key's position — i.e. the next node clockwise.
        idx = bisect.bisect_right(self._ring, (pos,))

        # Wrap around if we've gone past the end of the ring.
        if idx >= len(self._ring):
            idx = 0

        return self._ring[idx][1]

    @property
    def nodes(self) -> frozenset[str]:
        """Return the set of physical node IDs currently on the ring."""
        return frozenset(self._nodes)

    def __len__(self) -> int:
        """Return the number of virtual-node positions on the ring."""
        return len(self._ring)

    def __bool__(self) -> bool:
        return bool(self._ring)
