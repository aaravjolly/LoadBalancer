import asyncio

import pytest

from balancer.scheduler import (
    LeastConnectionsScheduler,
    RoundRobinScheduler,
    build_scheduler,
)
from balancer.state import BackendPool

URLS = ["http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"]


async def pick_names(scheduler, n):
    out = []
    for _ in range(n):
        b = await scheduler.next_backend()
        out.append(b.name if b else None)
    return out


async def test_rotation_three_healthy():
    rr = RoundRobinScheduler(BackendPool(URLS))
    assert await pick_names(rr, 9) == list("ABCABCABC")


async def test_empty_pool_returns_none():
    assert await RoundRobinScheduler(BackendPool([])).next_backend() is None
    assert await LeastConnectionsScheduler(BackendPool([])).next_backend() is None


def test_settings_reject_empty_backend_list():
    from balancer.config import Settings

    with pytest.raises(ValueError):
        Settings(backends=[])


async def test_no_eligible_backend_returns_none():
    pool = BackendPool(URLS)
    for b in pool:
        b.healthy = False
    assert await RoundRobinScheduler(pool).next_backend() is None
    assert await LeastConnectionsScheduler(pool).next_backend() is None


async def test_single_backend():
    rr = RoundRobinScheduler(BackendPool(URLS[:1]))
    assert await pick_names(rr, 4) == ["A"] * 4


async def test_only_one_healthy_backend_gets_everything():
    pool = BackendPool(URLS)
    pool.get("A").healthy = False
    pool.get("C").healthy = False
    assert await pick_names(RoundRobinScheduler(pool), 5) == ["B"] * 5


async def test_unhealthy_backend_is_skipped_and_order_kept():
    pool = BackendPool(URLS)
    pool.get("B").healthy = False
    assert await pick_names(RoundRobinScheduler(pool), 6) == list("ACACAC")


async def test_recovered_backend_rejoins_rotation():
    pool = BackendPool(URLS)
    rr = RoundRobinScheduler(pool)
    pool.get("B").healthy = False
    assert await pick_names(rr, 2) == ["A", "C"]
    pool.get("B").healthy = True
    assert await pick_names(rr, 3) == ["A", "B", "C"]


async def test_exclude_skips_already_tried_backend():
    pool = BackendPool(URLS)
    rr = RoundRobinScheduler(pool)
    first = await rr.next_backend()
    second = await rr.next_backend(exclude=[first])
    assert second is not None and second is not first
    all_out = await rr.next_backend(exclude=list(pool))
    assert all_out is None


async def test_assignment_counter_increments():
    pool = BackendPool(URLS)
    await pick_names(RoundRobinScheduler(pool), 6)
    assert [b.total_assigned for b in pool] == [2, 2, 2]


async def test_concurrent_selection_is_consistent():
    pool = BackendPool(URLS)
    rr = RoundRobinScheduler(pool)
    picks = await asyncio.gather(*(rr.next_backend() for _ in range(300)))
    names = [p.name for p in picks]
    assert names.count("A") == names.count("B") == names.count("C") == 100


async def test_least_connections_prefers_idle_backend():
    pool = BackendPool(URLS)
    pool.get("A").active_requests = 5
    pool.get("B").active_requests = 1
    pool.get("C").active_requests = 3
    lc = LeastConnectionsScheduler(pool)
    assert (await lc.next_backend()).name == "B"


async def test_least_connections_spreads_ties_round_robin():
    lc = LeastConnectionsScheduler(BackendPool(URLS))
    assert await pick_names(lc, 6) == list("ABCABC")


def test_build_scheduler_rejects_unknown():
    with pytest.raises(ValueError):
        build_scheduler("random", BackendPool(URLS))
