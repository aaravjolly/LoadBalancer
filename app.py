"""FastAPI entry point.

    uvicorn balancer.app:app --port 8000

Routes under ``/_admin/`` are answered by the balancer itself; every other
path and method is forwarded to a backend.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import Settings
from .health import HealthMonitor
from .metrics import Metrics
from .proxy import Proxy
from .scheduler import build_scheduler
from .state import BackendPool

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def _configure_logging() -> None:
    root = logging.getLogger("balancer")
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False


def create_app(
    settings: Settings | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    backend_names: list[str] | None = None,
) -> FastAPI:
    """Build an app. ``transport`` lets tests swap in an httpx.MockTransport."""
    settings = settings or Settings()
    _configure_logging()
    log = logging.getLogger("balancer")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = BackendPool(settings.backends, backend_names)
        scheduler = build_scheduler(settings.scheduler, pool)
        metrics = Metrics()
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout,
                read=settings.read_timeout,
                write=settings.write_timeout,
                pool=settings.pool_timeout,
            ),
            limits=httpx.Limits(
                max_connections=settings.max_connections,
                max_keepalive_connections=settings.max_keepalive_connections,
            ),
            follow_redirects=False,  # a proxy passes redirects through, it doesn't chase them
            trust_env=False,         # never route backend traffic via HTTP(S)_PROXY env vars
            transport=transport,
        )
        monitor = HealthMonitor(
            pool,
            client,
            path=settings.health_path,
            interval=settings.health_interval,
            timeout=settings.health_timeout,
            unhealthy_threshold=settings.unhealthy_threshold,
            healthy_threshold=settings.healthy_threshold,
        )
        app.state.settings = settings
        app.state.pool = pool
        app.state.metrics = metrics
        app.state.monitor = monitor
        app.state.proxy = Proxy(settings, pool, scheduler, client, metrics)

        log.info("event=startup scheduler=%s backends=%s health_interval=%ss thresholds=%d/%d",
                 settings.scheduler, ",".join(f"{b.name}={b.url}" for b in pool),
                 settings.health_interval, settings.unhealthy_threshold, settings.healthy_threshold)
        if settings.health_enabled:
            monitor.start()
        try:
            yield
        finally:
            await monitor.stop()
            await client.aclose()
            log.info("event=shutdown")

    app = FastAPI(title="MiniLoadBalancer", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/_admin/backends")
    async def admin_backends(request: Request):
        s = request.app.state
        backends = await s.pool.snapshot()
        return {
            "scheduler": s.settings.scheduler,
            "healthy_count": sum(1 for b in backends if b["healthy"]),
            "total_count": len(backends),
            "health_check": {
                "enabled": s.settings.health_enabled,
                "path": s.settings.health_path,
                "interval_s": s.settings.health_interval,
                "timeout_s": s.settings.health_timeout,
                "unhealthy_threshold": s.settings.unhealthy_threshold,
                "healthy_threshold": s.settings.healthy_threshold,
            },
            "backends": backends,
        }

    @app.get("/_admin/metrics")
    async def admin_metrics(request: Request):
        return request.app.state.metrics.snapshot()

    @app.api_route("/_admin/{rest:path}", methods=PROXY_METHODS, include_in_schema=False)
    async def admin_not_found(rest: str):
        return JSONResponse({"error": "unknown admin endpoint"}, status_code=404)

    @app.api_route("/{full_path:path}", methods=PROXY_METHODS, include_in_schema=False)
    async def proxy_all(request: Request, full_path: str):
        return await request.app.state.proxy.handle(request)

    return app


app = create_app()
