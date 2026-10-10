"""
Tests for the consistent hash ring (src/miniredis/sharding/hashring.py).

Validates:
- Statistical distribution balance across multiple nodes.
- Minimal disruption property when a node is removed.
- Empty ring edge case.
- Idempotent add_node behaviour.
"""

from __future__ import annotations

import collections

import pytest

from miniredis.sharding.hashring import ConsistentHashRing, VIRTUAL_NODES_PER_PHYSICAL


class TestDistribution:
    """Verify that keys are reasonably balanced across physical nodes."""

    def test_balanced_distribution_across_four_nodes(self) -> None:
        """Distribute 10,000 keys across 4 nodes.

        Each node should receive roughly 25% of the keys.  We allow
        a generous ±20% tolerance (i.e. each node should get between
        5% and 45% of the total) because consistent hashing with a
        small number of physical nodes is not perfectly uniform.
        """
        ring = ConsistentHashRing()
        for nid in ("node-a", "node-b", "node-c", "node-d"):
            ring.add_node(nid)

        total = 10_000
        counts: dict[str, int] = collections.Counter()
        for i in range(total):
            key = f"key-{i}".encode()
            owner = ring.get_node(key)
            assert owner is not None
            counts[owner] += 1

        # Every node must have been assigned at least some keys.
        assert len(counts) == 4, f"Expected 4 nodes, got {len(counts)}: {counts}"

        ideal = total / 4  # 2500
        tolerance = 0.20
        lower = ideal * (1 - tolerance)
        upper = ideal * (1 + tolerance)
        for nid, count in counts.items():
            assert lower <= count <= upper, (
                f"Node {nid} got {count} keys, expected between "
                f"{lower:.0f} and {upper:.0f}"
            )


class TestMinimalDisruption:
    """Verify the core property of consistent hashing: removing one
    node only remaps a small fraction of keys."""

    def test_most_keys_stay_on_same_node_after_removal(self) -> None:
        """Remove 1 node from a 4-node ring.

        More than 70% of the original 10,000 key assignments should
        remain unchanged — this is the minimal-disruption guarantee
        that distinguishes consistent hashing from naive modulo.
        """
        ring = ConsistentHashRing()
        nodes = ["node-a", "node-b", "node-c", "node-d"]
        for nid in nodes:
            ring.add_node(nid)

        total = 10_000
        before: dict[bytes, str] = {}
        for i in range(total):
            key = f"key-{i}".encode()
            owner = ring.get_node(key)
            assert owner is not None
            before[key] = owner

        # Remove one node.
        ring.remove_node("node-c")

        unchanged = 0
        for key, old_owner in before.items():
            new_owner = ring.get_node(key)
            assert new_owner is not None
            # Keys that belonged to node-c MUST move, others should stay.
            if new_owner == old_owner:
                unchanged += 1

        ratio = unchanged / total
        assert ratio > 0.70, (
            f"Only {ratio:.1%} of keys stayed on the same node after "
            f"removing one of four — expected >70%"
        )


class TestEdgeCases:
    """Empty ring and idempotent add_node."""

    def test_get_node_empty_ring_returns_none(self) -> None:
        ring = ConsistentHashRing()
        assert ring.get_node(b"anything") is None

    def test_add_node_idempotent(self) -> None:
        """Adding the same node_id twice must not duplicate virtual nodes."""
        ring = ConsistentHashRing()
        ring.add_node("node-a")
        expected_len = len(ring)
        assert expected_len == VIRTUAL_NODES_PER_PHYSICAL

        ring.add_node("node-a")  # second add — should be a no-op
        assert len(ring) == expected_len

    def test_remove_nonexistent_node_is_noop(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("node-a")
        ring.remove_node("node-z")  # doesn't exist — should not crash
        assert len(ring) == VIRTUAL_NODES_PER_PHYSICAL

    def test_get_node_accepts_str_key(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("only-node")
        assert ring.get_node("hello") == "only-node"

    def test_single_node_gets_all_keys(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("solo")
        for i in range(100):
            assert ring.get_node(f"k{i}") == "solo"

    def test_nodes_property(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("a")
        ring.add_node("b")
        assert ring.nodes == frozenset({"a", "b"})
        ring.remove_node("a")
        assert ring.nodes == frozenset({"b"})
