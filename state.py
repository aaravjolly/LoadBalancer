"""Backend model and the shared pool that the scheduler, health monitor and
metrics all read and write.

Concurrency model: the balancer runs as a single process with one asyncio
event loop. Every mutation of shared state happens under ``BackendPool.lock``
(an ``asyncio.Lock``), so interleaved coroutines never observe a half-updated
backend. This does NOT make the state safe across multiple worker processes;
run uvicorn with a single worker.
"""

from __future__ import annotations

import asyncio
import string
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Backend:
    url: str
    name: str
    healthy: bool = True
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    active_requests: int = 0

    # Counters
    total_assigned: int = 0
    total_responses: int = 0          # got an HTTP response (any status)
    total_upstream_errors: int = 0    # network failure / timeout
    total_5xx: int = 0
    total_latency_ms: float = 0.0
    last_checked: float | None = None
    last_state_change: float = field(default_factory=time.time)
    last_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        avg = self.total_latency_ms / self.total_responses if self.total_responses else 0.0
        return {
            "name": self.name,
            "url": self.url,
            "healthy": self.healthy,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "active_requests": self.active_requests,
            "total_assigned": self.total_assigned,
            "total_responses": self.total_responses,
            "total_upstream_errors": self.total_upstream_errors,
            "total_5xx": self.total_5xx,
            "avg_latency_ms": round(avg, 2),
            "last_checked": self.last_checked,
            "last_state_change": self.last_state_change,
            "last_error": self.last_error,
        }


def _default_names(n: int) -> list[str]:
    letters = string.ascii_uppercase
    return [letters[i] if i < len(letters) else f"B{i}" for i in range(n)]


class BackendPool:
    def __init__(self, urls: list[str], names: list[str] | None = None) -> None:
        names = names or _default_names(len(urls))
        if len(names) != len(urls):
            raise ValueError("names and urls must have the same length")
        self.backends: list[Backend] = [Backend(url=u, name=n) for u, n in zip(urls, names)]
        self.lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self.backends)

    def __iter__(self):
        return iter(self.backends)

    def healthy(self) -> list[Backend]:
        return [b for b in self.backends if b.healthy]

    def get(self, name: str) -> Backend:
        for b in self.backends:
            if b.name == name:
                return b
        raise KeyError(name)

    async def acquire(self, backend: Backend) -> None:
        async with self.lock:
            backend.active_requests += 1

    async def release(self, backend: Backend) -> None:
        async with self.lock:
            backend.active_requests = max(0, backend.active_requests - 1)

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self.lock:
            return [b.snapshot() for b in self.backends]
