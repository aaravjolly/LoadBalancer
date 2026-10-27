"""Proxy behaviour against a mocked upstream (no sockets)."""

import json
from contextlib import asynccontextmanager

import httpx
import pytest

from balancer.app import create_app
from balancer.config import Settings

URLS = ["http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"]
NAME = {9001: "A", 9002: "B", 9003: "C"}


def default_handler(request: httpx.Request) -> httpx.Response:
    server = NAME[request.url.port]
    return httpx.Response(
        200,
        json={
            "server": server,
            "method": request.method,
            "path": request.url.raw_path.decode(),
            "headers": dict(request.headers),
            "body": request.content.decode(),
        },
    )


@asynccontextmanager
async def proxy_client(handler=default_handler, **overrides):
    settings = Settings(backends=URLS, health_enabled=False, **overrides)
    app = create_app(settings, transport=httpx.MockTransport(handler))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://lb.test") as client:
            client.app = app
            yield client


async def test_round_robin_through_proxy():
    async with proxy_client() as c:
        servers = [(await c.get("/")).json()["server"] for _ in range(9)]
    assert servers == list("ABCABCABC")


async def test_path_query_and_method_preserved():
    async with proxy_client() as c:
        r = await c.get("/a/b%20c/d?x=1&y=two&y=three")
    data = r.json()
    assert data["method"] == "GET"
    assert data["path"] == "/a/b%20c/d?x=1&y=two&y=three"


async def test_post_body_preserved():
    async with proxy_client() as c:
        r = await c.post("/echo", json={"hello": "world", "n": [1, 2]})
    data = r.json()
    assert data["method"] == "POST"
    assert json.loads(data["body"]) == {"hello": "world", "n": [1, 2]}
    assert data["headers"]["content-type"] == "application/json"
    assert data["headers"]["content-length"] == str(len(data["body"]))


async def test_hop_by_hop_and_host_headers_are_not_forwarded():
    async with proxy_client() as c:
        r = await c.get("/", headers={
            "Connection": "close, X-Private",
            "X-Private": "secret",
            "Keep-Alive": "timeout=5",
            "TE": "trailers",
            "Proxy-Authorization": "Basic abc",
            "X-End-To-End": "kept",
            "Authorization": "Bearer t",
        })
    h = r.json()["headers"]
    assert "x-private" not in h
    assert "keep-alive" not in h
    assert "te" not in h
    assert "proxy-authorization" not in h
    assert h["x-end-to-end"] == "kept"
    assert h["authorization"] == "Bearer t"
    assert h["host"] == "127.0.0.1:9001"  # upstream host, not the client's
    assert h["x-forwarded-host"] == "lb.test"
    assert h["x-forwarded-proto"] == "http"
    assert "x-forwarded-for" in h


async def test_x_forwarded_for_is_appended():
    async with proxy_client() as c:
        r = await c.get("/", headers={"X-Forwarded-For": "10.0.0.1"})
    assert r.json()["headers"]["x-forwarded-for"].startswith("10.0.0.1, ")


async def test_request_id_generated_and_propagated():
    async with proxy_client() as c:
        r1 = await c.get("/")
        r2 = await c.get("/", headers={"X-Request-ID": "abc123"})
    assert r1.headers["x-request-id"] == r1.json()["headers"]["x-request-id"]
    assert r2.headers["x-request-id"] == "abc123"
    assert r2.json()["headers"]["x-request-id"] == "abc123"


@pytest.mark.parametrize("status", [201, 301, 404, 418, 500, 503])
async def test_upstream_status_and_body_passed_through(status):
    def handler(request):
        headers = {"location": "/elsewhere"} if status == 301 else {}
        return httpx.Response(status, content=b"upstream-body", headers=headers)

    async with proxy_client(handler) as c:
        r = await c.get("/")
    assert r.status_code == status
    assert r.content == b"upstream-body"
    assert r.headers["x-backend"] == "A"
    if status == 301:
        assert r.headers["location"] == "/elsewhere"  # redirect not followed


async def test_response_headers_filtered_and_duplicates_kept():
    def handler(request):
        return httpx.Response(200, content=b"hi", headers=[
            ("set-cookie", "a=1"), ("set-cookie", "b=2"),
            ("connection", "keep-alive, x-hop"), ("x-hop", "1"),
            ("keep-alive", "timeout=5"), ("x-custom", "yes"),
        ])

    async with proxy_client(handler) as c:
        r = await c.get("/")
    assert r.headers.get_list("set-cookie") == ["a=1", "b=2"]
    assert "x-hop" not in r.headers
    assert "keep-alive" not in r.headers
    assert r.headers["x-custom"] == "yes"
    assert r.headers["content-length"] == "2"


async def test_compressed_upstream_body_is_returned_decoded_with_correct_length():
    import gzip

    payload = b"x" * 1000

    def handler(request):
        return httpx.Response(200, content=gzip.compress(payload),
                              headers={"content-encoding": "gzip"})

    async with proxy_client(handler) as c:
        r = await c.get("/")
    assert r.content == payload
    assert "content-encoding" not in r.headers
    assert r.headers["content-length"] == "1000"


async def test_no_healthy_backends_returns_503():
    async with proxy_client() as c:
        for b in c.app.state.pool:
            b.healthy = False
        r = await c.get("/")
    assert r.status_code == 503
    assert r.json()["error"] == "no healthy backends available"
    assert r.headers["retry-after"] == "2"


async def test_connect_error_retries_next_backend():
    def handler(request):
        if request.url.port == 9001:
            raise httpx.ConnectError("refused", request=request)
        return default_handler(request)

    async with proxy_client(handler) as c:
        r = await c.post("/echo", json={"a": 1})
        metrics = (await c.get("/_admin/metrics")).json()
    assert r.status_code == 200
    assert r.json()["server"] == "B"
    assert metrics["connect_retries"] == 1


async def test_connect_error_without_retry_returns_502():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    async with proxy_client(handler, retry_on_connect_error=False) as c:
        r = await c.get("/")
    assert r.status_code == 502
    assert r.json()["backend"] == "A"


async def test_all_backends_refuse_returns_502_after_trying_each_once():
    attempts = []

    def handler(request):
        attempts.append(request.url.port)
        raise httpx.ConnectError("refused", request=request)

    async with proxy_client(handler) as c:
        r = await c.get("/")
    assert r.status_code == 502
    assert sorted(attempts) == [9001, 9002, 9003]


async def test_read_timeout_returns_504_and_post_is_not_replayed():
    attempts = []

    def handler(request):
        attempts.append(request.url.port)
        raise httpx.ReadTimeout("slow", request=request)

    async with proxy_client(handler) as c:
        r = await c.post("/echo", json={"charge": 100})
    assert r.status_code == 504
    assert attempts == [9001]  # exactly one attempt


async def test_other_transport_error_returns_502_without_retry():
    attempts = []

    def handler(request):
        attempts.append(request.url.port)
        raise httpx.RemoteProtocolError("server hung up", request=request)

    async with proxy_client(handler) as c:
        r = await c.get("/")
    assert r.status_code == 502
    assert attempts == [9001]


async def test_admin_endpoints_are_not_proxied():
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return default_handler(request)

    async with proxy_client(handler) as c:
        backends = (await c.get("/_admin/backends")).json()
        assert (await c.get("/_admin/nope")).status_code == 404
        await c.get("/")
        metrics = (await c.get("/_admin/metrics")).json()
    assert seen == ["/"]
    assert [b["name"] for b in backends["backends"]] == ["A", "B", "C"]
    assert backends["healthy_count"] == 3
    assert metrics["total_requests"] == 1
    assert metrics["assignments"] == {"A": 1}


async def test_metrics_distinguish_5xx_from_network_failures():
    def handler(request):
        if request.url.path == "/boom":
            return httpx.Response(500)
        if request.url.path == "/dead":
            raise httpx.ReadTimeout("x", request=request)
        return httpx.Response(200)

    async with proxy_client(handler) as c:
        await c.get("/ok")
        await c.get("/boom")
        await c.get("/dead")
        m = (await c.get("/_admin/metrics")).json()
        backends = (await c.get("/_admin/backends")).json()["backends"]
    assert m["successes"] == 1
    assert m["upstream_http_errors"] == 1
    assert m["upstream_failures"] == 1
    assert m["status_codes"] == {"200": 1, "500": 1, "504": 1}
    assert backends[1]["total_5xx"] == 1          # B served /boom
    assert backends[2]["total_upstream_errors"] == 1  # C timed out


async def test_active_requests_return_to_zero():
    async with proxy_client() as c:
        for _ in range(6):
            await c.get("/")
        assert all(b.active_requests == 0 for b in c.app.state.pool)
