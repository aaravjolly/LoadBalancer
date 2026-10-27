# MiniLoadBalancer

A small Layer 7 HTTP reverse proxy for localhost. It spreads requests across three backend servers with round robin, health-checks them in the background, takes a dead server out of rotation, and puts it back when it recovers.

Python 3.11+, FastAPI, Uvicorn, httpx, asyncio. No Docker or cloud services.

```text
                  curl / browser / load test
                            |
                            v
                  FastAPI proxy :8000
                  /      |       \
          scheduler   health     metrics/logging
                  \      |       /
                  shared backend state  (asyncio.Lock)
                            |
                    httpx.AsyncClient   (one, long-lived, pooled)
                      /     |     \
                     v      v      v
                  :9001   :9002   :9003
                   A        B       C
```

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/demo.sh          # starts everything, runs the failover demo, cleans up
```

Or run the pieces by hand, each in its own terminal:

```bash
SERVER_ID=A uvicorn backend.app:app --port 9001
SERVER_ID=B uvicorn backend.app:app --port 9002
SERVER_ID=C uvicorn backend.app:app --port 9003
uvicorn balancer.app:app --port 8000          # single worker: state lives in this process
```

Then:

```bash
for i in {1..9}; do curl -s http://127.0.0.1:8000/; echo; done
curl -s -X POST http://127.0.0.1:8000/echo -H 'content-type: application/json' -d '{"hello":"world"}'
python scripts/lbctl.py status                # health table
curl -s http://127.0.0.1:8000/_admin/metrics
```

To see failover, stop the B terminal (Ctrl-C), run `python scripts/lbctl.py wait B unhealthy`, send the nine requests again (only A and C answer), restart B, and `python scripts/lbctl.py wait B healthy`.

> If you have `HTTP_PROXY`/`HTTPS_PROXY` set in your shell, use `curl --noproxy '*'` or `export NO_PROXY=127.0.0.1,localhost`. The balancer itself ignores those variables for backend traffic.

## Sample demo output

Captured from `./scripts/demo.sh` (trimmed):

```text
== 1. Round robin: 9 requests ==
{"server":"A","message":"hello"}
{"server":"B","message":"hello"}
{"server":"C","message":"hello"}
... A, B, C, A, B, C

== 2. Stopping backend B (port 9002) ==
  B is unhealthy after 3.6s
  NAME URL                      STATE       ASSIGNED  LAST ERROR
  A    http://127.0.0.1:9001    healthy            3  -
  B    http://127.0.0.1:9002    UNHEALTHY          3  health probe failed: ConnectError
  C    http://127.0.0.1:9003    healthy            3  -

== 3. Requests while B is down ==
{"server":"A","message":"hello"}
{"server":"C","message":"hello"}
... A, C, A, C, ...

== 4. Restarting backend B ==
  B is healthy after 3.4s

== 5. Requests after recovery ==
B, C, A, B, C, A, B, C, A

== 6. Load test: 600 requests, concurrency 30 ==
Succeeded     600
Errors        0
Throughput    399.8 req/s
Latency ms    p50 56.8   p95 168.2   p99 228.9
Per backend   A: 200 (33.3%), B: 200 (33.3%), C: 200 (33.3%)

== Health transitions from the balancer log ==
... event=backend_down backend=B url=http://127.0.0.1:9002 reason='health probe failed: ConnectError'
... event=backend_up backend=B url=http://127.0.0.1:9002
```

Detection takes about 2–4 seconds (two failed probes on a 2 s interval, depending on where in the cycle B died); recovery likewise.

## How it works

**Request path** (`balancer/proxy.py`): pick a healthy backend → rebuild the request (same method, raw path, query string and body) → forward with the shared `httpx.AsyncClient` → copy back status, body and end-to-end headers → record metrics and an access log line.

**Health path** (`balancer/health.py`): one background task probes every backend's `/health` concurrently every 2 s with a 1 s timeout. Two consecutive failures (connection error, timeout, or any non-200) mark a backend unhealthy; two consecutive successes restore it. The scheduler only ever picks healthy backends.

**Scheduling** (`balancer/scheduler.py`): round robin walks the full backend list and skips unhealthy entries, so survivors keep their order (A, C, A, C) and a recovered backend slots back in. Least-connections (stretch goal A) is also implemented; enable it with `LB_SCHEDULER=least_connections`.

### Errors

| Situation | Client sees |
|---|---|
| No healthy backend | `503` with `Retry-After: 2`, immediately |
| Backend refuses the connection | retried on the next healthy backend; `502` only if every one refuses |
| Connect timeout | retried the same way; `504` if all fail |
| Read/write timeout (default read timeout 5 s) | `504`, **not** retried |
| Other transport failure (backend hung up mid-response) | `502`, not retried |
| Backend returns 404, 500, etc. | that exact status and body, passed through |

Error bodies are JSON: `{"error": ..., "status": ..., "request_id": ..., "backend": ...}`.

### Headers

* Request: hop-by-hop headers (`Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Upgrade`, `Proxy-*`, `Trailer`) and any header named in `Connection` are dropped. `Host` and `Content-Length` are set by httpx for the upstream. `X-Forwarded-For` (appended), `X-Forwarded-Host`, `X-Forwarded-Proto` and `X-Request-ID` are added.
* Response: hop-by-hop headers are dropped, `Content-Length` is recalculated, repeated headers such as `Set-Cookie` are preserved, and `X-Backend` and `X-Request-ID` are added so you can see who served each request.
* httpx decodes gzip/deflate upstream bodies, so the proxy returns them uncompressed and drops `Content-Encoding`.

## Admin endpoints

Served by the balancer itself; paths under `/_admin/` are never forwarded.

| Endpoint | Returns |
|---|---|
| `GET /_admin/backends` | health, consecutive probe results, in-flight count, assignments, 5xx and network-error counts, average latency and last error per backend |
| `GET /_admin/metrics` | total requests, successes, upstream 5xx, network failures, 503s, connect retries, status-code histogram, per-backend assignments, avg / p50 / p95 latency |

Every request also produces a log line:

```text
INFO balancer.access request_id=9f1e16adcae8 backend=B method=GET path=/inspect/a/b status=200 elapsed_ms=1.9
```

## Demo backend

`backend/app.py`, identity from `SERVER_ID`:

| Route | Purpose |
|---|---|
| `GET /` | `{"server": "A", "message": "hello"}` |
| `GET /health` | 200 when healthy, 503 when switched off |
| `POST /_control/health/down` · `/up` | flip health without killing the process |
| `POST /echo` | returns the submitted JSON and method |
| `GET /slow?delay=N` | sleeps N seconds (capped at 10) |
| `GET /status/{code}` | responds with that status code |
| `ANY /inspect/{path}` | reflects method, path, query, headers and body as received |

## Configuration

Defaults match the roadmap; every setting can be overridden with an environment variable on the balancer process.

| Variable | Default | Meaning |
|---|---|---|
| `LB_BACKENDS` | `http://127.0.0.1:9001,…:9002,…:9003` | comma-separated, fixed list of upstreams |
| `LB_SCHEDULER` | `round_robin` | or `least_connections` |
| `LB_CONNECT_TIMEOUT` | `1.0` | seconds |
| `LB_READ_TIMEOUT` | `5.0` | seconds; exceeding it returns 504 |
| `LB_WRITE_TIMEOUT` | `5.0` | seconds |
| `LB_POOL_TIMEOUT` | `2.0` | seconds waiting for a free pooled connection |
| `LB_HEALTH_ENABLED` | `true` | turn the health monitor off |
| `LB_HEALTH_PATH` | `/health` | |
| `LB_HEALTH_INTERVAL` | `2.0` | seconds between probe rounds |
| `LB_HEALTH_TIMEOUT` | `1.0` | seconds per probe |
| `LB_UNHEALTHY_THRESHOLD` | `2` | consecutive failures to remove |
| `LB_HEALTHY_THRESHOLD` | `2` | consecutive successes to restore |
| `LB_RETRY_ON_CONNECT_ERROR` | `true` | retry refused connections on another backend |
| `LB_MAX_CONNECTIONS` | `200` | upstream connection-pool size |

Backends are never taken from the request: clients can only reach the URLs in this list.

## Tests

```bash
pytest -q                     # 60 tests, ~35 s
pytest -q -m "not integration"   # unit tests only, < 1 s
```

* `test_scheduler.py`: empty pool, single backend, A/B/C rotation, skipping and re-adding unhealthy backends, 300 concurrent selections, least-connections.
* `test_health.py`: threshold transitions, flapping, timeouts and non-200 probes, monitor start/stop.
* `test_proxy.py`: forwarding against a mocked upstream: path/query/body, header filtering, `X-Forwarded-*`, request IDs, status passthrough, `Set-Cookie` duplicates, gzip, 502/503/504 mapping, no POST replay after a timeout, metrics accounting.
* `test_integration.py`: real uvicorn processes on random ports: every acceptance criterion below, plus 100 concurrent requests, a backend reporting itself unhealthy, a dead backend at startup, and refused connections with retry disabled.

### Acceptance criteria

| # | Criterion | Covered by |
|---|---|---|
| 1 | 9 requests go A→B→C three times | `test_nine_requests_rotate_a_b_c` |
| 2 | Stopped backend excluded after threshold; others keep serving | `test_stopped_backend_is_excluded_then_restored` |
| 3 | Restarted backend re-enabled after threshold | same test |
| 4 | All backends down → 503, no hang | `test_all_backends_down_returns_503_quickly` |
| 5 | Status and body reach the client intact | `test_backend_error_status_is_preserved`, `test_post_echo_preserves_body_and_method` |
| 6 | Tests pass; demo runs from this README | `pytest`, `./scripts/demo.sh` |

## Load testing

```bash
python scripts/load_test.py --requests 1000 --concurrency 50
python scripts/load_test.py --path "/slow?delay=0.05" --requests 300 --concurrency 30
```

Reports success/error counts, throughput, p50/p95/p99 latency and the share each backend served. Numbers above came from a shared cloud VM with the load generator, the proxy and all three backends on the same machine and full access logging on; expect different figures on your laptop.

## Design decisions

1. **Layer 7, not Layer 4.** The proxy understands HTTP, so it can preserve paths and methods, translate failures into proper status codes, add `X-Forwarded-*`, and expose admin routes. A TCP distributor can do none of that.
2. **Async I/O.** A proxy mostly waits on the network. One event loop handles many in-flight requests without a thread each, and one pooled `httpx.AsyncClient` reuses keep-alive connections to backends.
3. **Active health checks with thresholds.** Round robin alone keeps sending traffic to a dead server. Requiring two failures in a row avoids ejecting a backend over a single blip; requiring two successes avoids re-adding one that is flapping.
4. **Retry only when nothing was sent.** A refused or timed-out *connection* means the backend never saw the request, so trying another backend is safe even for a POST. Once a request may have been delivered (read timeout, connection dropped mid-response), it is never replayed: the first attempt may have completed and its side effects (a charge, an order) would be duplicated. This is why requests keep succeeding in the seconds before the health checker notices a backend is gone.
5. **`asyncio.Lock` around shared state.** Selection, health transitions and counters are mutated from interleaved coroutines; the lock keeps each update atomic. It does nothing across processes, so the balancer must run with one Uvicorn worker.
6. **Fixed upstream list.** The destination is always one of the configured URLs, never anything derived from the request, which rules out using the proxy as an open relay (SSRF).

## Known limitations

* **In-flight requests on a dying backend fail.** If a backend dies while handling a request, that client gets a 502 or 504. Health checks only protect *subsequent* requests.
* **Detection window.** Between a backend dying and the second failed probe (~2–4 s), new requests to it are rescued by the connect-error retry, but each pays a failed connection attempt first. A backend that accepts connections but hangs is not rescued until health checks remove it; those requests get 504 after the read timeout.
* **Bodies are buffered** in memory in both directions; no streaming, so large uploads/downloads and server-sent events are out of scope.
* **No WebSockets, HTTP/2, or TLS termination.** Run it on localhost only.
* **Single process.** State is in memory; multiple Uvicorn workers would each have their own view of backend health and rotation.
* **Static backends.** No service discovery; change `LB_BACKENDS` and restart.
* **Admin endpoints are unauthenticated.** Fine on localhost, not for anything public.
* No sticky sessions, authentication, rate limiting, or graceful connection draining.
* `HEAD` and `OPTIONS` are forwarded as-is; the demo backend only answers `GET` on `/`, so `HEAD /` returns its 405.

## Repository layout

```text
balancer/
  app.py        FastAPI app, lifespan (client + health task), admin and catch-all routes
  config.py     settings, backend list, hop-by-hop header set
  state.py      Backend model and lock-protected pool
  scheduler.py  round robin and least-connections
  health.py     probe loop and threshold state machine
  proxy.py      forwarding, header filtering, error mapping, retry rule
  metrics.py    counters and latency percentiles
backend/app.py  demo server
scripts/
  demo.sh       end-to-end failover demo
  load_test.py  async load generator
  lbctl.py      health table / wait-for-state helper
tests/          unit + integration tests
```
