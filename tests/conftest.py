"""
Shared pytest configuration.

Every async test runs on one session-wide event loop. grpc.aio keeps
process-global poller state bound to an event loop; with pytest-asyncio's
default of a new loop per test, gRPC objects that outlive a test can leave
that poller calling into a closed loop ("RuntimeError: Event loop is
closed" in PollerCompletionQueue._poll_wrapper). After that, gRPC calls
without their own timeout (server start/stop, channel close) never complete
and the run hangs. A single loop for the whole session removes that hazard.
(Recipe from the pytest-asyncio 0.24 docs: "Run all tests in the session in
a single event loop".)
"""

from __future__ import annotations

import pytest
from pytest_asyncio import is_async_test


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    session_loop = pytest.mark.asyncio(loop_scope="session")
    for item in items:
        if is_async_test(item):
            item.add_marker(session_loop, append=False)
