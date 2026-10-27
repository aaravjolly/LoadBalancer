"""Request/response forwarding.

Responsibilities:
* pick a backend via the scheduler (503 if none is healthy)
* rebuild the upstream request: same method, raw path, query string and body;
  end-to-end headers only; httpx sets Host/Content-Length for the upstream
* map failures: connect error -> 502, timeout -> 504; an upstream HTTP error
  status (404, 500, ...) is a *response* and is passed through untouched
* retry on another backend ONLY when the connection was never established,
  so a request that may have reached a backend is never replayed

Whole bodies are buffered in memory; streaming is out of scope.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass

import httpx
from starlette.requests import Request
from starlette.responses import Response

from .config import HOP_BY_HOP_HEADERS, Settings
from .metrics import Metrics
from .scheduler import Scheduler
from .state import Backend, BackendPool

log = logging.getLogger("balancer.access")

# Headers the proxy computes itself and must not copy from either side.
_REQUEST_DROP = HOP_BY_HOP_HEADERS | {"host", "content-length"}
# httpx transparently decodes gzip/deflate/br bodies, so the upstream
# Content-Encoding no longer describes what we send. Date/Server are set by
# uvicorn for our own response; copying them would duplicate them.
_RESPONSE_DROP = HOP_BY_HOP_HEADERS | {"content-length", "content-encoding", "date", "server"}


def _connection_listed(headers) -> set[str]:
    """Header names nominated as hop-by-hop via the Connection header."""
    listed: set[str] = set()
    for value in headers.getlist("connection") if hasattr(headers, "getlist") else headers.get_list("connection"):
        listed.update(token.strip().lower() for token in value.split(",") if token.strip())
    return listed


def build_upstream_headers(request: Request, request_id: str) -> list[tuple[str, str]]:
    drop = _REQUEST_DROP | _connection_listed(request.headers)
    out = [(k, v) for k, v in request.headers.items() if k.lower() not in drop]

    client_ip = request.client.host if request.client else "unknown"
    prior_xff = request.headers.get("x-forwarded-for")
    out = [(k, v) for k, v in out if k.lower() not in {
        "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-request-id"}]
    out.append(("x-forwarded-for", f"{prior_xff}, {client_ip}" if prior_xff else client_ip))
    if "host" in request.headers:
        out.append(("x-forwarded-host", request.headers["host"]))
    out.append(("x-forwarded-proto", request.url.scheme))
    out.append(("x-request-id", request_id))
    return out


def build_client_response(upstream: httpx.Response, method: str, backend: Backend,
                          request_id: str) -> Response:
    body = upstream.content
    drop = _RESPONSE_DROP | _connection_listed(upstream.headers)
    raw: list[tuple[bytes, bytes]] = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in upstream.headers.multi_items()  # keeps repeated Set-Cookie
        if k.lower() not in drop and k.lower() not in {"x-backend", "x-request-id"}
    ]

    status = upstream.status_code
    if method == "HEAD":
        # No body, but advertise what a GET would have returned.
        if "content-length" in upstream.headers:
            raw.append((b"content-length", upstream.headers["content-length"].encode()))
        body = b""
    elif not (100 <= status < 200 or status in (204, 304)):
        raw.append((b"content-length", str(len(body)).encode()))
    else:
        body = b""

    raw.append((b"x-backend", backend.name.encode()))
    raw.append((b"x-request-id", request_id.encode()))

    response = Response(content=body, status_code=status)
    response.raw_headers = raw
    return response


def error_response(status: int, error: str, request_id: str, backend: str | None = None) -> Response:
    payload = {"error": error, "status": status, "request_id": request_id}
    if backend:
        payload["backend"] = backend
    headers = {"x-request-id": request_id}
    if backend:
        headers["x-backend"] = backend
    if status == 503:
        headers["retry-after"] = "2"
    return Response(content=json.dumps(payload), status_code=status,
                    media_type="application/json", headers=headers)


@dataclass
class Proxy:
    settings: Settings
    pool: BackendPool
    scheduler: Scheduler
    client: httpx.AsyncClient
    metrics: Metrics

    async def handle(self, request: Request) -> Response:
        started = time.perf_counter()
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        method = request.method
        raw_path = request.scope.get("raw_path") or request.url.path.encode()
        path = raw_path.decode("latin-1")
        query = request.scope.get("query_string", b"").decode("latin-1")
        body = await request.body()
        headers = build_upstream_headers(request, request_id)

        tried: list[Backend] = []
        last_error: tuple[int, str] | None = None

        while True:
            backend = await self.scheduler.next_backend(exclude=tried)
            if backend is None:
                if last_error is None:
                    return self._finish(started, request_id, method, path, None,
                                        error_response(503, "no healthy backends available", request_id),
                                        kind="no_backend")
                status, msg = last_error
                failed = tried[-1].name
                return self._finish(started, request_id, method, path, tried[-1],
                                    error_response(status, msg, request_id, failed),
                                    kind="upstream_failure")

            tried.append(backend)
            url = backend.url + path + (f"?{query}" if query else "")
            await self.pool.acquire(backend)
            t0 = time.perf_counter()
            try:
                upstream = await self.client.request(
                    method, url, headers=headers, content=body or None)
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                # Connection never established: the backend did not see the request.
                kind = "connect timeout" if isinstance(exc, httpx.ConnectTimeout) else "connection failed"
                last_error = (504 if isinstance(exc, httpx.ConnectTimeout) else 502,
                              f"upstream {kind}")
                await self._note_failure(backend, f"{kind}: {type(exc).__name__}")
                if self.settings.retry_on_connect_error:
                    self.metrics.connect_retries += 1
                    log.info("event=connect_retry request_id=%s backend=%s reason=%r",
                             request_id, backend.name, kind)
                    continue
                return self._finish(started, request_id, method, path, backend,
                                    error_response(last_error[0], last_error[1], request_id, backend.name),
                                    kind="upstream_failure")
            except httpx.TimeoutException as exc:
                # Read/write/pool timeout: the request may have been processed. Never replay.
                await self._note_failure(backend, f"timeout: {type(exc).__name__}")
                return self._finish(started, request_id, method, path, backend,
                                    error_response(504, "upstream timed out", request_id, backend.name),
                                    kind="upstream_failure")
            except httpx.RequestError as exc:
                await self._note_failure(backend, f"request error: {type(exc).__name__}")
                return self._finish(started, request_id, method, path, backend,
                                    error_response(502, "bad gateway", request_id, backend.name),
                                    kind="upstream_failure")
            finally:
                await self.pool.release(backend)

            latency_ms = (time.perf_counter() - t0) * 1000
            async with self.pool.lock:
                backend.total_responses += 1
                backend.total_latency_ms += latency_ms
                if upstream.status_code >= 500:
                    backend.total_5xx += 1
            response = build_client_response(upstream, method, backend, request_id)
            kind = "upstream_5xx" if upstream.status_code >= 500 else "ok"
            return self._finish(started, request_id, method, path, backend, response, kind=kind)

    async def _note_failure(self, backend: Backend, reason: str) -> None:
        async with self.pool.lock:
            backend.total_upstream_errors += 1
            backend.last_error = reason

    def _finish(self, started: float, request_id: str, method: str, path: str,
                backend: Backend | None, response: Response, *, kind: str) -> Response:
        elapsed_ms = (time.perf_counter() - started) * 1000
        name = backend.name if backend else None
        self.metrics.record(status=response.status_code, elapsed_ms=elapsed_ms,
                            backend=name, kind=kind)
        log.info("request_id=%s backend=%s method=%s path=%s status=%d elapsed_ms=%.1f",
                 request_id, name or "-", method, path, response.status_code, elapsed_ms)
        return response
