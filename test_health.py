import httpx

from balancer.health import HealthMonitor, apply_probe_result
from balancer.state import Backend, BackendPool

URLS = ["http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"]


def probe(b, ok, **kw):
    return apply_probe_result(b, ok, unhealthy_threshold=kw.get("down", 2),
                              healthy_threshold=kw.get("up", 2), error=None if ok else "boom")


def test_single_failure_does_not_remove_backend():
    b = Backend(url="u", name="A")
    assert probe(b, False) is None
    assert b.healthy and b.consecutive_failures == 1


def test_two_consecutive_failures_mark_unhealthy():
    b = Backend(url="u", name="A")
    probe(b, False)
    assert probe(b, False) == "down"
    assert not b.healthy
    assert b.last_error == "boom"


def test_success_resets_failure_streak():
    b = Backend(url="u", name="A")
    probe(b, False)
    probe(b, True)
    assert probe(b, False) is None
    assert b.healthy


def test_recovery_needs_two_consecutive_successes():
    b = Backend(url="u", name="A", healthy=False)
    assert probe(b, True) is None
    assert not b.healthy
    assert probe(b, True) == "up"
    assert b.healthy and b.last_error is None


def test_flapping_backend_stays_down():
    b = Backend(url="u", name="A", healthy=False)
    for _ in range(5):
        probe(b, True)
        probe(b, False)
    assert not b.healthy


def test_custom_thresholds():
    b = Backend(url="u", name="A")
    for _ in range(2):
        assert probe(b, False, down=3) is None
    assert probe(b, False, down=3) == "down"


def test_already_healthy_backend_reports_no_transition():
    b = Backend(url="u", name="A")
    for _ in range(5):
        assert probe(b, True) is None


async def test_monitor_marks_failing_backend_down_and_back_up():
    state = {"B_up": True}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9002 and not state["B_up"]:
            raise httpx.ConnectError("refused", request=request)
        if request.url.port == 9003:
            return httpx.Response(500)
        return httpx.Response(200, json={"status": "ok"})

    pool = BackendPool(URLS)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        mon = HealthMonitor(pool, client, interval=0.01)

        await mon.check_all()
        assert [b.healthy for b in pool] == [True, True, True]  # C: 1 failure so far
        await mon.check_all()
        assert [b.healthy for b in pool] == [True, True, False]  # non-200 counts as failure

        state["B_up"] = False
        await mon.check_all()
        await mon.check_all()
        assert not pool.get("B").healthy
        assert "ConnectError" in pool.get("B").last_error

        state["B_up"] = True
        await mon.check_all()
        assert not pool.get("B").healthy
        await mon.check_all()
        assert pool.get("B").healthy


async def test_monitor_treats_timeout_as_failure():
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    pool = BackendPool(URLS[:1])
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        mon = HealthMonitor(pool, client)
        await mon.check_all()
        await mon.check_all()
    assert not pool.get("A").healthy
    assert pool.get("A").last_error == "health probe timed out"


async def test_monitor_start_and_stop_cleanly():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200)

    import asyncio

    pool = BackendPool(URLS)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        mon = HealthMonitor(pool, client, interval=0.01)
        mon.start()
        await asyncio.sleep(0.08)
        await mon.stop()
        after_stop = calls["n"]
        await asyncio.sleep(0.05)
    assert after_stop >= 3
    assert calls["n"] == after_stop  # no probes after stop()
