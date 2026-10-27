"""Active health checking.

One background coroutine probes every backend's ``/health`` endpoint on a
fixed interval. A backend is marked unhealthy after ``unhealthy_threshold``
consecutive failed probes and restored after ``healthy_threshold``
consecutive successes, so a single blip never flaps it in or out.
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from .state import Backend, BackendPool

log = logging.getLogger("balancer.health")


def apply_probe_result(
    backend: Backend,
    ok: bool,
    *,
    unhealthy_threshold: int,
    healthy_threshold: int,
    error: str | None = None,
) -> str | None:
    """Update one backend's counters for a probe result.

    Pure state-transition logic (no I/O) so it can be unit-tested directly.
    Caller must hold the pool lock. Returns ``"down"`` or ``"up"`` when the
    backend's health flips, otherwise ``None``.
    """
    backend.last_checked = time.time()
    if ok:
        backend.consecutive_successes += 1
        backend.consecutive_failures = 0
        if not backend.healthy and backend.consecutive_successes >= healthy_threshold:
            backend.healthy = True
            backend.last_state_change = time.time()
            backend.last_error = None
            return "up"
    else:
        backend.consecutive_failures += 1
        backend.consecutive_successes = 0
        backend.last_error = error
        if backend.healthy and backend.consecutive_failures >= unhealthy_threshold:
            backend.healthy = False
            backend.last_state_change = time.time()
            return "down"
    return None


class HealthMonitor:
    def __init__(
        self,
        pool: BackendPool,
        client: httpx.AsyncClient,
        *,
        path: str = "/health",
        interval: float = 2.0,
        timeout: float = 1.0,
        unhealthy_threshold: int = 2,
        healthy_threshold: int = 2,
    ) -> None:
        self.pool = pool
        self.client = client
        self.path = path
        self.interval = interval
        self.timeout = timeout
        self.unhealthy_threshold = unhealthy_threshold
        self.healthy_threshold = healthy_threshold
        self._task: asyncio.Task | None = None

    async def probe(self, backend: Backend) -> tuple[bool, str | None]:
        try:
            resp = await self.client.get(backend.url + self.path, timeout=self.timeout)
        except httpx.TimeoutException:
            return False, "health probe timed out"
        except httpx.HTTPError as exc:
            return False, f"health probe failed: {type(exc).__name__}"
        if resp.status_code == 200:
            return True, None
        return False, f"health probe returned {resp.status_code}"

    async def check_all(self) -> None:
        """Probe every backend concurrently, then apply all results under the lock."""
        results = await asyncio.gather(*(self.probe(b) for b in self.pool.backends))
        async with self.pool.lock:
            for backend, (ok, err) in zip(self.pool.backends, results):
                change = apply_probe_result(
                    backend,
                    ok,
                    unhealthy_threshold=self.unhealthy_threshold,
                    healthy_threshold=self.healthy_threshold,
                    error=err,
                )
                if change == "down":
                    log.warning("event=backend_down backend=%s url=%s reason=%r",
                                backend.name, backend.url, err)
                elif change == "up":
                    log.warning("event=backend_up backend=%s url=%s", backend.name, backend.url)

    async def run(self) -> None:
        while True:
            started = time.monotonic()
            try:
                await self.check_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # never let the monitor die silently
                log.exception("event=health_loop_error")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self.interval - elapsed))

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="health-monitor")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
