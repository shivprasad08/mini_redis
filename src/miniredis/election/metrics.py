"""
Election message metrics instrumentation for FA-2 M18.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ElectionMetrics:
    """Detailed message and timing metrics for a single election execution."""

    algorithm: str                      # "bully" or "ring"
    election_id: str
    initiator_id: str
    elected_leader: str
    epoch: int
    duration_seconds: float
    initiation_messages: int            # Initial trigger/broadcast messages
    candidate_messages: int             # Forwarded votes / candidate exchanges
    coordinator_messages: int           # Victory announcements
    retry_or_skip_messages: int         # Fault-tolerant skip/retry attempts
    total_messages: int                 # Sum of all election protocol messages
    participating_nodes: int            # Number of responding active nodes
    unavailable_nodes: int              # Number of dead/unreachable nodes encountered

    def to_dict(self) -> Dict[str, Any]:
        return {
            "algorithm": self.algorithm,
            "election_id": self.election_id,
            "initiator_id": self.initiator_id,
            "elected_leader": self.elected_leader,
            "epoch": self.epoch,
            "duration_seconds": round(self.duration_seconds, 4),
            "initiation_messages": self.initiation_messages,
            "candidate_messages": self.candidate_messages,
            "coordinator_messages": self.coordinator_messages,
            "retry_or_skip_messages": self.retry_or_skip_messages,
            "total_messages": self.total_messages,
            "participating_nodes": self.participating_nodes,
            "unavailable_nodes": self.unavailable_nodes,
        }
