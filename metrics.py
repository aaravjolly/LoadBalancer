"""Process-wide request counters and latency stats.

All updates happen on the single event loop between awaits, so plain integer
increments are race-free here; no lock needed.
"""

from __future__ import annotations

import time
from collections import Counter, deque
from typing import Any


class Metrics:
    def __init__(self, latency_window: int = 1000) -> None:
        self.started_at = time.time()
        self.total_requests = 0
        self.successes = 0             # upstream replied with < 500
        self.upstream_http_errors = 0  # upstream replied with >= 500
        self.upstream_failures = 0     # network error / timeout (502/504)
        self.no_backend = 0            # 503: nothing healthy
        self.connect_retries = 0
        self.status_codes: Counter[int] = Counter()
        self.assignments: Counter[str] = Counter()
        self._latency_sum_ms = 0.0
        self._latency_count = 0
        self._recent: deque[float] = deque(maxlen=latency_window)

    def record(self, *, status: int, elapsed_ms: float, backend: str | None, kind: str) -> None:
        self.total_requests += 1
        self.status_codes[status] += 1
        if backend:
            self.assignments[backend] += 1
        if kind == "ok":
            self.successes += 1
        elif kind == "upstream_5xx":
            self.upstream_http_errors += 1
        elif kind == "upstream_failure":
            self.upstream_failures += 1
        elif kind == "no_backend":
            self.no_backend += 1
        self._latency_sum_ms += elapsed_ms
        self._latency_count += 1
        self._recent.append(elapsed_ms)

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        k = max(0, min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1))))
        return ordered[k]

    def snapshot(self) -> dict[str, Any]:
        recent = list(self._recent)
        avg = self._latency_sum_ms / self._latency_count if self._latency_count else 0.0
        return {
            "uptime_seconds": round(time.time() - self.started_at, 1),
            "total_requests": self.total_requests,
            "successes": self.successes,
            "upstream_http_errors": self.upstream_http_errors,
            "upstream_failures": self.upstream_failures,
            "no_backend_available": self.no_backend,
            "connect_retries": self.connect_retries,
            "status_codes": {str(k): v for k, v in sorted(self.status_codes.items())},
            "assignments": dict(sorted(self.assignments.items())),
            "latency_ms": {
                "avg": round(avg, 2),
                "p50_recent": round(self._percentile(recent, 50), 2),
                "p95_recent": round(self._percentile(recent, 95), 2),
                "window": len(recent),
            },
        }
