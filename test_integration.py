"""End-to-end tests: real uvicorn processes for three backends and the balancer.

Health checks run fast here (0.2s interval) so failover tests finish quickly;
thresholds stay at the roadmap's 2 failures / 2 successes.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parent.parent
HEALTH_INTERVAL = 0.2


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(predicate, timeout=10.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except httpx.HTTPError:
            pass
        time.sleep(interval)
    raise AssertionError("condition not met before timeout")


class Server:
    def __init__(self, module: str, port: int, env: dict[str, str]):
        self.module, self.port, self.env = module, port, env
        self.proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, ready_path: str = "/health") -> "Server":
        env = {**os.environ, **self.env, "NO_PROXY": "*", "no_proxy": "*"}
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", self.module, "--host", "127.0.0.1",
             "--port", str(self.port), "--log-level", "warning"],
            cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        with httpx.Client(trust_env=False, timeout=0.5) as c:
            wait_until(lambda: c.get(self.url + ready_path).status_code < 500)
        return self

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None


def start_balancer(backend_urls: list[str], **env: str) -> Server:
    settings = {
        "LB_BACKENDS": ",".join(backend_urls),
        "LB_HEALTH_INTERVAL": str(HEALTH_INTERVAL),
        "LB_HEALTH_TIMEOUT": "0.5",
        **env,
    }
    return Server("balancer.app:app", free_port(), settings).start("/_admin/backends")


@pytest.fixture
def backends():
    servers = [Server("backend.app:app", free_port(), {"SERVER_ID": sid}).start()
               for sid in "ABC"]
    yield servers
    for s in servers:
        s.stop()


@pytest.fixture
def lb(backends):
    server = start_balancer([b.url for b in backends], LB_READ_TIMEOUT="1.0")
    yield server
    server.stop()


@pytest.fixture
def client():
    with httpx.Client(trust_env=False, timeout=10) as c:
        yield c


def healthy_names(client, lb):
    data = client.get(lb.url + "/_admin/backends").json()
    return {b["name"] for b in data["backends"] if b["healthy"]}


# ---------- acceptance criteria ----------

def test_nine_requests_rotate_a_b_c(client, lb):
    servers = [client.get(lb.url + "/").json()["server"] for _ in range(9)]
    assert servers == list("ABCABCABC")


def test_post_echo_preserves_body_and_method(client, lb):
    r = client.post(lb.url + "/echo", json={"order": 42, "items": ["x", "y"]})
    assert r.status_code == 200
    assert r.json()["method"] == "POST"
    assert r.json()["received"] == {"order": 42, "items": ["x", "y"]}


def test_path_and_query_reach_backend(client, lb):
    r = client.get(lb.url + "/inspect/deep/path?a=1&b=two")
    data = r.json()
    assert data["path"] == "/inspect/deep/path"
    assert data["query"] == "a=1&b=two"


@pytest.mark.parametrize("code", [404, 500])
def test_backend_error_status_is_preserved(client, lb, code):
    r = client.get(f"{lb.url}/status/{code}")
    assert r.status_code == code
    assert r.json()["status"] == code


def test_slow_backend_times_out_with_504(client, lb):
    started = time.monotonic()
    r = client.get(lb.url + "/slow?delay=3")
    assert r.status_code == 504
    assert time.monotonic() - started < 2.5


def test_stopped_backend_is_excluded_then_restored(client, lb, backends):
    b = backends[1]
    b.stop()
    wait_until(lambda: healthy_names(client, lb) == {"A", "C"}, timeout=5)

    served = Counter(client.get(lb.url + "/").json()["server"] for _ in range(10))
    assert set(served) == {"A", "C"}
    assert served["A"] == served["C"] == 5

    b.start()
    wait_until(lambda: healthy_names(client, lb) == {"A", "B", "C"}, timeout=5)
    served = Counter(client.get(lb.url + "/").json()["server"] for _ in range(9))
    assert served == {"A": 3, "B": 3, "C": 3}


def test_requests_keep_succeeding_during_detection_window(client, lb, backends):
    """Connection refused is retried on another backend, so no client errors
    even before the health checker has noticed B is gone."""
    backends[1].stop()
    statuses = [client.get(lb.url + "/").status_code for _ in range(12)]
    assert statuses == [200] * 12


def test_backend_reporting_unhealthy_is_excluded(client, lb, backends):
    client.post(backends[0].url + "/_control/health/down")
    wait_until(lambda: "A" not in healthy_names(client, lb), timeout=5)
    servers = {client.get(lb.url + "/").json()["server"] for _ in range(6)}
    assert servers == {"B", "C"}
    client.post(backends[0].url + "/_control/health/up")
    wait_until(lambda: "A" in healthy_names(client, lb), timeout=5)


def test_all_backends_down_returns_503_quickly(client, lb, backends):
    for b in backends:
        b.stop()
    wait_until(lambda: healthy_names(client, lb) == set(), timeout=5)
    started = time.monotonic()
    r = client.get(lb.url + "/")
    assert r.status_code == 503
    assert time.monotonic() - started < 0.5


def test_100_concurrent_requests_counts_add_up(lb):
    def one(_):
        with httpx.Client(trust_env=False, timeout=10) as c:
            return c.get(lb.url + "/").json()["server"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
        served = Counter(pool.map(one, range(100)))

    assert sum(served.values()) == 100
    assert max(served.values()) - min(served.values()) <= 1

    with httpx.Client(trust_env=False) as c:
        metrics = c.get(lb.url + "/_admin/metrics").json()
        backends = c.get(lb.url + "/_admin/backends").json()["backends"]
    assert metrics["total_requests"] == 100
    assert metrics["successes"] == 100
    assert metrics["assignments"] == dict(sorted(served.items()))
    assert all(b["active_requests"] == 0 for b in backends)


# ---------- configurations without retry / with a dead backend ----------

def test_connection_refused_without_retry_returns_502(client):
    live = Server("backend.app:app", free_port(), {"SERVER_ID": "A"}).start()
    dead_url = f"http://127.0.0.1:{free_port()}"  # nothing listens here
    lb = start_balancer([live.url, dead_url],
                        LB_HEALTH_ENABLED="false", LB_RETRY_ON_CONNECT_ERROR="false")
    try:
        statuses = [client.get(lb.url + "/").status_code for _ in range(4)]
        assert statuses == [200, 502, 200, 502]
    finally:
        lb.stop()
        live.stop()


def test_dead_backend_at_startup_is_excluded(client):
    live = Server("backend.app:app", free_port(), {"SERVER_ID": "A"}).start()
    dead_url = f"http://127.0.0.1:{free_port()}"
    lb = start_balancer([live.url, dead_url])
    try:
        wait_until(lambda: healthy_names(client, lb) == {"A"}, timeout=5)
        assert {client.get(lb.url + "/").json()["server"] for _ in range(5)} == {"A"}
    finally:
        lb.stop()
        live.stop()
