"""Simulated and corrected physical clocks for TTL synchronization."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class SyncSample:
    offset_ms: float
    rtt_ms: float
    synchronized: bool


class SynchronizedClock:
    """Wall-clock abstraction with simulated skew and Cristian correction."""

    def __init__(
        self,
        *,
        skew_ms: float = 0.0,
        source: Optional[Callable[[], float]] = None,
    ) -> None:
        if not math.isfinite(skew_ms):
            raise ValueError("clock skew must be finite")
        self._source = source or time.time
        self._skew_ms = skew_ms
        self._offset_ms = 0.0
        self._last_sample: Optional[SyncSample] = None

    @property
    def skew_ms(self) -> float:
        return self._skew_ms

    @property
    def offset_ms(self) -> float:
        return self._offset_ms

    @property
    def last_sample(self) -> Optional[SyncSample]:
        return self._last_sample

    @property
    def synchronized(self) -> bool:
        return self._last_sample is not None and self._last_sample.synchronized

    def raw_time_ms(self) -> int:
        return int((self._source() * 1000.0) + self._skew_ms)

    def corrected_time_ms(self) -> int:
        return int(self.raw_time_ms() + self._offset_ms)

    def corrected_time_seconds(self) -> float:
        return self.corrected_time_ms() / 1000.0

    def apply_sample(self, *, client_send_ms: int, server_time_ms: int, client_receive_ms: int) -> SyncSample:
        if client_receive_ms < client_send_ms:
            raise ValueError("client receive time cannot be before send time")
        rtt_ms = client_receive_ms - client_send_ms
        midpoint_ms = client_send_ms + (rtt_ms / 2.0)
        offset_ms = server_time_ms - midpoint_ms
        sample = SyncSample(offset_ms=offset_ms, rtt_ms=float(rtt_ms), synchronized=True)
        self._offset_ms = offset_ms
        self._last_sample = sample
        return sample

    def mark_unsynchronized(self) -> SyncSample:
        sample = SyncSample(offset_ms=self._offset_ms, rtt_ms=0.0, synchronized=False)
        self._last_sample = sample
        return sample
