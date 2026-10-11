"""
M19 tests: edge-chasing (Chandy-Misra-Haas) deadlock detection over real gRPC
between Ricart-Agrawala lock managers.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import grpc
import pytest

from miniredis.mutex import DeadlockError, LockState, MutexServicer, RicartAgrawalaManager
from miniredis.mutex.proto import mutex_pb2_grpc
from miniredis.mutex.protocol import FencingToken
from tests.test_leader_recovery import _free_ports


class LockCluster:
    def __init__(self, ids: list[str], probe_filter=None) -> None:
        ports = _free_ports(len(ids))
        self.addresses = {i: f"127.0.0.1:{p}" for i, p in zip(ids, ports)}
        self.managers = {
            i: RicartAgrawalaManager(i, self.addresses, rpc_timeout_seconds=1.0,
                                     rpc_filter=probe_filter, probe_interval_seconds=0.1)
            for i in ids
        }
        self.servers: list[grpc.aio.Server] = []

    async def start(self) -> "LockCluster":
        for node_id, manager in self.managers.items():
            server = grpc.aio.server()
            mutex_pb2_grpc.add_MutexServiceServicer_to_server(MutexServicer(manager), server)
            server.add_insecure_port(self.addresses[node_id])
            await server.start()
            self.servers.append(server)
        return self

    async def close(self) -> None:
        for manager in self.managers.values():
            await manager.stop()
        for server in self.servers:
            await server.stop(grace=0)


@pytest.fixture
async def locks():
    clusters: list[LockCluster] = []

    async def make(ids=("node-a", "node-b"), probe_filter=None) -> LockCluster:
        cluster = await LockCluster(list(ids), probe_filter).start()
        clusters.append(cluster)
        return cluster

    yield make
    for cluster in clusters:
        await cluster.close()


async def _result(task: asyncio.Task) -> object:
    try:
        return await task
    except Exception as exc:  # noqa: BLE001 - the test inspects the outcome
        return exc


async def test_two_node_deadlock_is_detected_and_one_victim_aborts(locks) -> None:
    c = await locks()
    a, b = c.managers["node-a"], c.managers["node-b"]
    await a.acquire("R1", client_id="x")
    await b.acquire("R2", client_id="y")

    t_a = asyncio.create_task(a.acquire("R2", client_id="x", timeout_seconds=8))
    t_b = asyncio.create_task(b.acquire("R1", client_id="y", timeout_seconds=8))
    started = asyncio.get_running_loop().time()
    victim = await _result(t_b)
    assert isinstance(victim, DeadlockError), victim  # node-b = highest id in the cycle
    assert victim.cycle[0] == victim.cycle[-1] == "node-b" and "node-a" in victim.cycle
    assert asyncio.get_running_loop().time() - started < 4, "detected well before the lock timeout"
    assert not t_a.done(), "only the victim aborts"
    assert b.get_status("R1")["state"] == LockState.RELEASED.value  # victim's pending state cleaned up

    # The victim's client gives up its held lock; the survivor proceeds with a valid token.
    await b.release("R2", client_id="y")
    token = await asyncio.wait_for(t_a, 5)
    assert isinstance(token, FencingToken) and a.get_status("R2")["state"] == LockState.HELD.value
    assert a.deadlock_stats["aborted"] == 0 and b.deadlock_stats["aborted"] == 1


async def test_three_node_cycle_detected_across_nodes(locks) -> None:
    c = await locks(("node-a", "node-b", "node-c"))
    m = c.managers
    await m["node-a"].acquire("R1")
    await m["node-b"].acquire("R2")
    await m["node-c"].acquire("R3")
    tasks = {
        "node-a": asyncio.create_task(m["node-a"].acquire("R2", timeout_seconds=8)),
        "node-b": asyncio.create_task(m["node-b"].acquire("R3", timeout_seconds=8)),
        "node-c": asyncio.create_task(m["node-c"].acquire("R1", timeout_seconds=8)),
    }
    victim = await _result(tasks["node-c"])
    assert isinstance(victim, DeadlockError)
    assert set(victim.cycle) == {"node-a", "node-b", "node-c"}
    assert m["node-b"].deadlock_stats["probes_received"] > 0, "the probe was forwarded through other nodes"
    await m["node-c"].release("R3")
    assert isinstance(await asyncio.wait_for(tasks["node-b"], 5), FencingToken)
    await m["node-b"].release("R2")
    await m["node-b"].release("R3")
    assert isinstance(await asyncio.wait_for(tasks["node-a"], 5), FencingToken)


async def test_plain_waiting_is_not_a_deadlock(locks) -> None:
    c = await locks()
    a, b = c.managers["node-a"], c.managers["node-b"]
    await a.acquire("R1")
    waiter = asyncio.create_task(b.acquire("R1", timeout_seconds=5))
    await asyncio.sleep(0.6)  # several probe rounds while b waits on a (a is not blocked)
    assert not waiter.done()
    assert a.deadlock_stats["probes_received"] > 0
    assert a.deadlock_stats["detected"] == b.deadlock_stats["detected"] == 0
    await a.release("R1")
    assert isinstance(await asyncio.wait_for(waiter, 5), FencingToken)


async def test_probe_duplicates_termination_and_non_blocked_nodes(locks) -> None:
    c = await locks()
    a, b = c.managers["node-a"], c.managers["node-b"]
    assert await a.handle_probe("node-x", "p1", "node-x", ["node-x"]) == "not_blocked"
    await b.acquire("R1")
    waiter = asyncio.create_task(a.acquire("R1", timeout_seconds=5))
    await asyncio.sleep(0.05)
    assert a.waiting_for() == {"node-b"}
    assert await a.handle_probe("node-x", "p2", "node-x", ["node-x"]) == "forwarded"
    assert await a.handle_probe("node-x", "p2", "node-x", ["node-x"]) == "duplicate"
    long_path = ["n"] * 10
    assert await a.handle_probe("node-x", "p3", "n", long_path) == "dropped"
    await b.release("R1")
    await asyncio.wait_for(waiter, 5)
    assert a.waiting_for() == set(), "no wait-for edges once the lock is held"


async def test_lost_probes_never_fabricate_a_deadlock(locks) -> None:
    blocked = {"on": True}

    def no_probes(rpc_name: str, address: str) -> bool:
        return not (blocked["on"] and rpc_name == "Probe")

    c = await locks(probe_filter=no_probes)
    a, b = c.managers["node-a"], c.managers["node-b"]
    await a.acquire("R1")
    await b.acquire("R2")
    t_a = asyncio.create_task(a.acquire("R2", timeout_seconds=1.0))
    t_b = asyncio.create_task(b.acquire("R1", timeout_seconds=1.0))
    results = [await _result(t_a), await _result(t_b)]
    assert all(isinstance(r, TimeoutError) for r in results), results  # undetected, but safe
    assert a.deadlock_stats["detected"] == b.deadlock_stats["detected"] == 0
    assert a.get_status("R2")["state"] == b.get_status("R1")["state"] == LockState.RELEASED.value

    blocked["on"] = False  # network heals: the same cycle is now detected
    t_b = asyncio.create_task(b.acquire("R1", timeout_seconds=6))
    t_a = asyncio.create_task(a.acquire("R2", timeout_seconds=6))
    assert isinstance(await _result(t_b), DeadlockError)
    await b.release("R2")
    await asyncio.wait_for(t_a, 5)


async def test_dead_node_in_the_cycle_is_not_reported(locks) -> None:
    c = await locks(("node-a", "node-b", "node-c"))
    m = c.managers
    await m["node-a"].acquire("R1")
    await m["node-b"].acquire("R2")
    t_a = asyncio.create_task(m["node-a"].acquire("R2", timeout_seconds=1.5))
    await asyncio.sleep(0.05)
    await c.servers[1].stop(grace=0)  # node-b crashes while a waits on it
    outcome = await _result(t_a)
    assert isinstance(outcome, TimeoutError)
    assert m["node-a"].deadlock_stats["detected"] == 0
