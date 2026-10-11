"""
Shared gRPC channel factory for node-to-node traffic.

Long-lived peer channels must notice a restarted peer quickly. gRPC's
default reconnect backoff grows to 120 s, so a container that was stopped
(and dropped out of Docker DNS) could stay UNREACHABLE for minutes after it
came back. Capping the backoff keeps reachability reports honest.

Kept dependency-free so every networking module can import it without
creating import cycles.
"""

from __future__ import annotations

import grpc

PEER_CHANNEL_OPTIONS: tuple[tuple[str, int], ...] = (
    ("grpc.initial_reconnect_backoff_ms", 250),
    ("grpc.min_reconnect_backoff_ms", 250),
    ("grpc.max_reconnect_backoff_ms", 1000),
)


def open_peer_channel(address: str) -> grpc.aio.Channel:
    """Open an insecure channel to a cluster peer with bounded reconnect backoff."""
    return grpc.aio.insecure_channel(address, options=list(PEER_CHANNEL_OPTIONS))
