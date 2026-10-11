"""
M15 — Replica staleness classification and cluster progress reporting.

This module is intentionally independent of network transport so that
classification logic can be unit-tested deterministically without
spinning up gRPC servers.

Status values
─────────────
LOCAL_PRIMARY   The entry describes the primary node itself.
IN_SYNC         Same epoch, same contiguous sequence, no gap, matching fingerprint.
LAGGING         Same epoch, replica is behind the primary (no evidence of divergence).
STALE_EPOCH     Replica's epoch is older than the primary's.
FUTURE_EPOCH    Replica's epoch is newer — suggests the queried node is not the leader.
DIVERGENT_GAP   A sequence gap or rejected operation has been recorded.
DIVERGENT_HISTORY  Same epoch+sequence but fingerprints differ.
UNREACHABLE     GetProgress RPC failed or timed out.
STALE_VIEW      The primary's epoch changed while collecting responses; report is incoherent.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from miniredis.replication.progress import ProgressSnapshot
from miniredis.replication.proto import replication_pb2

logger = logging.getLogger(__name__)

# Canonical status strings.
STATUS_LOCAL_PRIMARY = "LOCAL_PRIMARY"
STATUS_IN_SYNC = "IN_SYNC"
STATUS_LAGGING = "LAGGING"
STATUS_STALE_EPOCH = "STALE_EPOCH"
STATUS_FUTURE_EPOCH = "FUTURE_EPOCH"
STATUS_DIVERGENT_GAP = "DIVERGENT_GAP"
STATUS_DIVERGENT_HISTORY = "DIVERGENT_HISTORY"
STATUS_UNREACHABLE = "UNREACHABLE"
STATUS_STALE_VIEW = "STALE_VIEW"


def classify_replica(
    primary: ProgressSnapshot,
    replica_response: Optional[replication_pb2.ProgressResponse],
) -> dict[str, Any]:
    """Classify one replica against a primary progress snapshot.

    Parameters
    ----------
    primary:
        An immutable snapshot of the current primary's progress.
    replica_response:
        The ProgressResponse received from the replica's GetProgress RPC,
        or ``None`` if the replica was unreachable.

    Returns
    -------
    A JSON-serialisable dict describing the replica's status relative to
    the primary.
    """
    if replica_response is None:
        return {
            "node_id": "unknown",
            "status": STATUS_UNREACHABLE,
            "message": "GetProgress RPC failed or timed out",
        }

    node_id = replica_response.node_id
    r_epoch = int(replica_response.epoch)
    r_seq = int(replica_response.last_applied_sequence)
    r_digest = replica_response.history_digest
    r_gap = replica_response.gap_detected

    p_epoch = primary.epoch
    p_seq = primary.last_applied_sequence
    p_digest = primary.history_digest

    # Highest priority: gap takes precedence over other checks.
    if r_gap:
        return {
            "node_id": node_id,
            "role": replica_response.role,
            "epoch": r_epoch,
            "last_applied_sequence": r_seq,
            "status": STATUS_DIVERGENT_GAP,
            "gap_expected_sequence": int(replica_response.gap_expected_sequence),
            "gap_observed_sequence": int(replica_response.gap_observed_sequence),
            "lag": p_seq - r_seq,
            "message": (
                f"Sequence gap: expected={replica_response.gap_expected_sequence} "
                f"observed={replica_response.gap_observed_sequence}"
            ),
        }

    if r_epoch > p_epoch:
        return {
            "node_id": node_id,
            "role": replica_response.role,
            "epoch": r_epoch,
            "last_applied_sequence": r_seq,
            "status": STATUS_FUTURE_EPOCH,
            "message": (
                f"Replica epoch {r_epoch} is newer than queried primary epoch {p_epoch}. "
                "This node may not be the current leader."
            ),
        }

    if r_epoch < p_epoch:
        return {
            "node_id": node_id,
            "role": replica_response.role,
            "epoch": r_epoch,
            "last_applied_sequence": r_seq,
            "status": STATUS_STALE_EPOCH,
            "message": (
                f"Replica is in epoch {r_epoch}; primary is in epoch {p_epoch}. "
                "Sequence numbers are not directly comparable across epochs."
            ),
        }

    # Same epoch from here on.
    if r_seq == p_seq:
        if r_digest != p_digest and r_digest != "" and p_digest != "":
            return {
                "node_id": node_id,
                "role": replica_response.role,
                "epoch": r_epoch,
                "last_applied_sequence": r_seq,
                "status": STATUS_DIVERGENT_HISTORY,
                "primary_history_digest": p_digest,
                "replica_history_digest": r_digest,
                "lag": 0,
                "message": "Same epoch and sequence but history fingerprints differ.",
            }
        return {
            "node_id": node_id,
            "role": replica_response.role,
            "epoch": r_epoch,
            "last_applied_sequence": r_seq,
            "status": STATUS_IN_SYNC,
            "lag": 0,
            "message": "Replica is fully synchronised.",
        }

    # Same epoch, replica lags.
    lag = p_seq - r_seq
    return {
        "node_id": node_id,
        "role": replica_response.role,
        "epoch": r_epoch,
        "last_applied_sequence": r_seq,
        "status": STATUS_LAGGING,
        "lag": lag,
        "message": f"Replica is {lag} write(s) behind the primary.",
    }


def build_cluster_report(
    primary_snapshot: ProgressSnapshot,
    replica_targets: list[str],
    replica_responses: list[Optional[replication_pb2.ProgressResponse]],
    stale_view: bool = False,
    target_to_node_id: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Build a full JSON-serialisable cluster staleness report.

    Parameters
    ----------
    primary_snapshot:
        Snapshot taken from the primary's progress tracker at the start
        of collection (before any RPC was sent).
    replica_targets:
        Ordered list of gRPC addresses that were queried.
    replica_responses:
        Parallel list of ProgressResponse objects (or ``None`` for
        unreachable replicas), in the same order as ``replica_targets``.
    stale_view:
        Set to True when the primary epoch changed during collection,
        making the report incoherent.
    target_to_node_id:
        Optional mapping from replica target address to logical node ID.
    """
    nodes: list[dict[str, Any]] = []

    # Primary entry first.
    nodes.append({
        "node_id": primary_snapshot.node_id,
        "role": "primary",
        "epoch": primary_snapshot.epoch,
        "last_applied_sequence": primary_snapshot.last_applied_sequence,
        "history_digest": primary_snapshot.history_digest,
        "status": STATUS_LOCAL_PRIMARY,
        "lag": 0,
    })

    for target, response in zip(replica_targets, replica_responses):
        if response is None:
            resolved_id = target_to_node_id.get(target, target) if target_to_node_id else target
            entry = {
                "node_id": resolved_id,
                "status": STATUS_UNREACHABLE,
                "message": "GetProgress RPC failed or timed out",
            }
        else:
            entry = classify_replica(primary_snapshot, response)
            entry.setdefault("history_digest", response.history_digest)

        nodes.append(entry)

    report: dict[str, Any] = {
        "primary_node_id": primary_snapshot.node_id,
        "primary_epoch": primary_snapshot.epoch,
        "primary_last_applied_sequence": primary_snapshot.last_applied_sequence,
        "nodes": nodes,
    }

    if stale_view:
        report["warning"] = (
            STATUS_STALE_VIEW + ": The primary epoch changed during collection. "
            "This report may mix progress from different primary epochs."
        )

    return report


async def collect_staleness_report(
    primary_snapshot: ProgressSnapshot,
    replicator,  # Replicator — avoid circular import
    progress_timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """High-level coroutine that queries all replicas and builds the report.

    The primary epoch is snapshotted before any RPC is sent. If the epoch
    has advanced by the time all responses are collected, ``stale_view``
    is set in the report.

    Parameters
    ----------
    primary_snapshot:
        Pre-collected snapshot of the primary's progress.
    replicator:
        The primary's Replicator instance (used for targets and RPCs).
    progress_timeout_seconds:
        Per-RPC timeout.
    """
    from miniredis.replication.replicator import Replicator  # local import to avoid cycle

    targets = replicator.targets
    responses = await replicator.collect_progress(timeout_seconds=progress_timeout_seconds)

    # Check whether the primary epoch advanced while we were collecting.
    stale_view = False
    if replicator.epoch != primary_snapshot.epoch:
        stale_view = True
        logger.warning(
            "Primary epoch changed from %s to %s during staleness collection",
            primary_snapshot.epoch,
            replicator.epoch,
        )

    return build_cluster_report(
        primary_snapshot=primary_snapshot,
        replica_targets=targets,
        replica_responses=responses,
        stale_view=stale_view,
        target_to_node_id=getattr(replicator, "target_to_node_id", None),
    )

