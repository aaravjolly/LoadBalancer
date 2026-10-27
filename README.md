# MiniLoadBalancer

A small Layer 7 HTTP reverse proxy for localhost. It spreads requests across three backend servers with round robin, health-checks them in the background, takes a dead server out of rotation, and puts it back when it recovers.


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


## Load testing

```bash
python scripts/load_test.py --requests 1000 --concurrency 50
python scripts/load_test.py --path "/slow?delay=0.05" --requests 300 --concurrency 30
```

Reports success/error counts, throughput, p50/p95/p99 latency and the share each backend served. Numbers above came from a shared cloud VM with the load generator, the proxy and all three backends on the same machine and full access logging on; expect different figures on your laptop.


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
