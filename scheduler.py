"""Backend selection strategies.

Both schedulers only ever return a backend that is currently marked healthy,
and return ``None`` when nothing is eligible (the proxy turns that into 503).
"""

from __future__ import annotations

from typing import Iterable, Protocol

from .state import Backend, BackendPool


class Scheduler(Protocol):
    async def next_backend(self, exclude: Iterable[Backend] = ()) -> Backend | None: ...


class RoundRobinScheduler:
    """Rotate over the fixed backend list, skipping unhealthy entries.

    The cursor walks the *full* list rather than the healthy subset, so when a
    backend drops out the remaining ones keep their relative order
    (A, C, A, C ...) and when it returns it slots back in (A, B, C ...).
    """

    def __init__(self, pool: BackendPool) -> None:
        self.pool = pool
        self._index = 0

    async def next_backend(self, exclude: Iterable[Backend] = ()) -> Backend | None:
        excluded = {id(b) for b in exclude}
        async with self.pool.lock:
            n = len(self.pool.backends)
            for step in range(n):
                idx = (self._index + step) % n
                candidate = self.pool.backends[idx]
                if candidate.healthy and id(candidate) not in excluded:
                    self._index = (idx + 1) % n
                    candidate.total_assigned += 1
                    return candidate
            return None


class LeastConnectionsScheduler:
    """Pick the healthy backend with the fewest in-flight requests.

    Ties are broken round-robin so an idle cluster still spreads load evenly.
    Selection is an O(n) scan, which is fine for a handful of backends.
    """

    def __init__(self, pool: BackendPool) -> None:
        self.pool = pool
        self._index = 0

    async def next_backend(self, exclude: Iterable[Backend] = ()) -> Backend | None:
        excluded = {id(b) for b in exclude}
        async with self.pool.lock:
            n = len(self.pool.backends)
            best: Backend | None = None
            best_idx = -1
            for step in range(n):
                idx = (self._index + step) % n
                c = self.pool.backends[idx]
                if not c.healthy or id(c) in excluded:
                    continue
                if best is None or c.active_requests < best.active_requests:
                    best, best_idx = c, idx
            if best is None:
                return None
            self._index = (best_idx + 1) % n
            best.total_assigned += 1
            return best


def build_scheduler(kind: str, pool: BackendPool) -> RoundRobinScheduler | LeastConnectionsScheduler:
    if kind == "round_robin":
        return RoundRobinScheduler(pool)
    if kind == "least_connections":
        return LeastConnectionsScheduler(pool)
    raise ValueError(f"Unknown scheduler: {kind!r}")
