"""
Protocol definitions, states, and fencing token semantics for M16.

Fencing Tokens
──────────────
A fencing token must provide an unambiguous, strictly monotonic order
across nodes, election epochs, and Lamport logical time.

Structure of a FencingToken:
- epoch: uint64 election epoch under which the lock was allocated.
  Higher epoch strictly dominates lower epochs.
- lamport_ts: uint64 Lamport logical timestamp of the lock request.
- counter: uint64 monotonic local sequence/counter.
- node_id: string node identity for deterministic tie-breaking.

Total ordering:
  (epoch, lamport_ts, counter, node_id)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Union


class LockState(str, Enum):
    """Ricart-Agrawala node state for a specific lock resource."""
    RELEASED = "RELEASED"
    WANTED = "WANTED"
    HELD = "HELD"


class FencingWatermarkError(Exception):
    """Raised when an operation presents a stale or invalid fencing token."""
    pass


@dataclass(frozen=True)
class FencingToken:
    """Monotonically ordered fencing token."""

    epoch: int
    lamport_ts: int
    counter: int
    node_id: str = ""

    def to_str(self) -> str:
        return f"{self.epoch}:{self.lamport_ts}:{self.counter}:{self.node_id}"

    def __str__(self) -> str:
        return self.to_str()

    @classmethod
    def from_str(cls, s: Union[str, int, "FencingToken"]) -> "FencingToken":
        if isinstance(s, FencingToken):
            return s
        if isinstance(s, int):
            return cls(epoch=0, lamport_ts=0, counter=s, node_id="")
        
        s = str(s).strip()
        # Support colon or hyphen delimiter
        delim = ":" if ":" in s else ("-" if "-" in s else None)
        if delim:
            parts = s.split(delim)
            if len(parts) >= 4:
                return cls(
                    epoch=int(parts[0]),
                    lamport_ts=int(parts[1]),
                    counter=int(parts[2]),
                    node_id=":".join(parts[3:]),
                )
            elif len(parts) == 3:
                return cls(
                    epoch=int(parts[0]),
                    lamport_ts=int(parts[1]),
                    counter=int(parts[2]),
                    node_id="",
                )
            elif len(parts) == 2:
                return cls(
                    epoch=int(parts[0]),
                    lamport_ts=int(parts[1]),
                    counter=0,
                    node_id="",
                )
        try:
            return cls(epoch=0, lamport_ts=0, counter=int(s), node_id="")
        except ValueError:
            return cls(epoch=0, lamport_ts=0, counter=0, node_id=s)

    def _sort_key(self) -> tuple[int, int, int, str]:
        return (self.epoch, self.lamport_ts, self.counter, self.node_id)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, FencingToken):
            try:
                other = FencingToken.from_str(other)
            except Exception:
                return NotImplemented
        return self._sort_key() < other._sort_key()

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, FencingToken):
            try:
                other = FencingToken.from_str(other)
            except Exception:
                return NotImplemented
        return self._sort_key() <= other._sort_key()

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, FencingToken):
            try:
                other = FencingToken.from_str(other)
            except Exception:
                return NotImplemented
        return self._sort_key() > other._sort_key()

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, FencingToken):
            try:
                other = FencingToken.from_str(other)
            except Exception:
                return NotImplemented
        return self._sort_key() >= other._sort_key()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FencingToken):
            try:
                other = FencingToken.from_str(other)
            except Exception:
                return False
        return self._sort_key() == other._sort_key()
