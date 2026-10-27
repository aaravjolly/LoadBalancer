"""Static configuration.

Backends are a fixed, hard-coded list. The proxy only ever forwards to these
URLs; nothing a client sends can change the upstream destination.

Every setting can be overridden with an environment variable (handy for tests
and demos), but the defaults match the roadmap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_BACKENDS = [
    "http://127.0.0.1:9001",
    "http://127.0.0.1:9002",
    "http://127.0.0.1:9003",
]

# Hop-by-hop headers (RFC 9110 §7.6.1) apply to a single connection and must
# never be forwarded by a proxy. Host is handled separately.
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_backends() -> list[str]:
    raw = os.getenv("LB_BACKENDS")
    if not raw:
        return list(DEFAULT_BACKENDS)
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]


@dataclass
class Settings:
    backends: list[str] = field(default_factory=_env_backends)

    # Scheduling: "round_robin" (default) or "least_connections" (stretch goal A).
    scheduler: str = field(default_factory=lambda: os.getenv("LB_SCHEDULER", "round_robin"))

    # Upstream timeouts (seconds).
    connect_timeout: float = field(default_factory=lambda: _env_float("LB_CONNECT_TIMEOUT", 1.0))
    read_timeout: float = field(default_factory=lambda: _env_float("LB_READ_TIMEOUT", 5.0))
    write_timeout: float = field(default_factory=lambda: _env_float("LB_WRITE_TIMEOUT", 5.0))
    pool_timeout: float = field(default_factory=lambda: _env_float("LB_POOL_TIMEOUT", 2.0))

    # Health checking.
    health_enabled: bool = field(default_factory=lambda: _env_bool("LB_HEALTH_ENABLED", True))
    health_path: str = field(default_factory=lambda: os.getenv("LB_HEALTH_PATH", "/health"))
    health_interval: float = field(default_factory=lambda: _env_float("LB_HEALTH_INTERVAL", 2.0))
    health_timeout: float = field(default_factory=lambda: _env_float("LB_HEALTH_TIMEOUT", 1.0))
    unhealthy_threshold: int = field(default_factory=lambda: _env_int("LB_UNHEALTHY_THRESHOLD", 2))
    healthy_threshold: int = field(default_factory=lambda: _env_int("LB_HEALTHY_THRESHOLD", 2))

    # If the TCP connection to a backend is refused, the request never left the
    # proxy, so trying the next healthy backend is safe for every method,
    # including POST. Anything that may have reached the backend is never replayed.
    retry_on_connect_error: bool = field(
        default_factory=lambda: _env_bool("LB_RETRY_ON_CONNECT_ERROR", True)
    )

    # Connection-pool sizing for the shared httpx.AsyncClient.
    max_connections: int = field(default_factory=lambda: _env_int("LB_MAX_CONNECTIONS", 200))
    max_keepalive_connections: int = field(
        default_factory=lambda: _env_int("LB_MAX_KEEPALIVE", 50)
    )

    def __post_init__(self) -> None:
        if not self.backends:
            raise ValueError("At least one backend URL is required")
        if self.scheduler not in {"round_robin", "least_connections"}:
            raise ValueError(f"Unknown scheduler: {self.scheduler!r}")
        if self.unhealthy_threshold < 1 or self.healthy_threshold < 1:
            raise ValueError("Health thresholds must be >= 1")
        self.backends = [u.rstrip("/") for u in self.backends]
