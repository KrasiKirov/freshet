# M3: Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Live Prometheus metrics from both pipeline workers plus a provisioned Grafana dashboard showing freshness percentiles, throughput, consumer lag, and invalid-event count while the slice runs.

**Architecture:** A shared `freshet/pipeline/metrics.py` module defines counters/histograms and an opt-in HTTP exporter; the normalizer and embedder record one observation per event through small `observe_*` helpers (unit-testable without a broker). Prometheus and Grafana run as profile-gated compose services (`obs` profile) so the lean stack is untouched; Prometheus scrapes the host-side workers via `host.docker.internal` and Redpanda's built-in `/public_metrics`; Grafana auto-provisions one committed dashboard JSON.

**Tech Stack:** prometheus-client (new core dep), prom/prometheus + grafana/grafana containers, PromQL, existing pipeline modules.

**Spec:** `docs/superpowers/specs/2026-06-10-m1-m7-roadmap-design.md` (M3 section). M2 is merged; the slice works end to end.

**Conventions:** commit messages are a single imperative title — no body, no co-authors. Makefile recipes use TABS. Venv at `.venv`.

## Verified facts (probed on the live Redpanda v24.2.7 — do not re-derive)

- `/public_metrics` on the admin port (9644, container-internal) exposes, with NO config flag needed:
  - `redpanda_kafka_consumer_group_committed_offset{redpanda_group=,redpanda_topic=,redpanda_partition=}` (gauge)
  - `redpanda_kafka_max_offset{redpanda_namespace=,redpanda_topic=,redpanda_partition=}` (gauge; high watermark; our topics live in `redpanda_namespace="kafka"`)
- There is no native lag metric in this version (and no `enable_consumer_group_metrics` property), so lag is computed in PromQL:

```promql
sum by (redpanda_group) (
  redpanda_kafka_max_offset{redpanda_namespace="kafka",redpanda_topic=~"raw.events|normalized.events"}
  - on(redpanda_topic, redpanda_partition) group_right()
  redpanda_kafka_consumer_group_committed_offset{redpanda_group=~"normalizer|embedder"}
)
```

## Target file structure

```
observability/
  prometheus.yml                                  # NEW — scrape config
  grafana/
    provisioning/
      datasources/prometheus.yml                  # NEW
      dashboards/freshet.yml                      # NEW — file provider
    dashboards/
      freshet-pipeline.json                       # NEW — the committed dashboard
freshet/pipeline/metrics.py                       # NEW — metric definitions + exporter
freshet/pipeline/normalizer.py                    # MODIFIED — observe + --metrics-port
freshet/pipeline/embedder.py                      # MODIFIED — observe + --metrics-port
tests/test_metrics.py                             # NEW
docker-compose.yml                                # MODIFIED — prometheus + grafana (profile obs)
Makefile                                          # MODIFIED — up-obs; down covers profiles
pyproject.toml                                    # MODIFIED — prometheus-client dep
README.md                                         # MODIFIED — Observability section
```

---

### Task 1: Metrics module

**Files:**
- Create: `freshet/pipeline/metrics.py`
- Modify: `pyproject.toml` (add dependency)
- Test: `tests/test_metrics.py`

- [ ] **Step 1: Add the dependency**

In `pyproject.toml` `[project] dependencies`, append after `"uvicorn>=0.29",`:

```toml
    "prometheus-client>=0.20",
```

Then `source .venv/bin/activate && pip install -e ".[test]" -q`.

- [ ] **Step 2: Write the failing tests `tests/test_metrics.py`**

```python
from prometheus_client import REGISTRY

from freshet.pipeline.metrics import (
    FRESHNESS,
    INDEXED_EVENTS,
    INGEST_LAG,
    INVALID_EVENTS,
    NORMALIZED_EVENTS,
    start_metrics_server,
)


def _value(name: str, labels=None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def test_counters_increment():
    before = _value("freshet_normalizer_events_total")
    NORMALIZED_EVENTS.inc()
    assert _value("freshet_normalizer_events_total") == before + 1

    before = _value("freshet_normalizer_invalid_total")
    INVALID_EVENTS.inc()
    assert _value("freshet_normalizer_invalid_total") == before + 1

    before = _value("freshet_embedder_events_total")
    INDEXED_EVENTS.inc()
    assert _value("freshet_embedder_events_total") == before + 1


def test_histograms_observe_into_buckets():
    before = _value("freshet_freshness_seconds_bucket", {"le": "5.0"})
    FRESHNESS.observe(2.5)
    assert _value("freshet_freshness_seconds_bucket", {"le": "5.0"}) == before + 1

    before = _value("freshet_ingest_lag_seconds_count")
    INGEST_LAG.observe(0.3)
    assert _value("freshet_ingest_lag_seconds_count") == before + 1


def test_metrics_server_port_zero_is_disabled():
    # must be a no-op, not an error — unit tests and library callers use 0
    start_metrics_server(0)
```

- [ ] **Step 3: Run to verify failure**

Run: `pytest -q tests/test_metrics.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.pipeline.metrics'`

- [ ] **Step 4: Implement `freshet/pipeline/metrics.py`**

```python
"""Prometheus metrics shared by the pipeline workers.

Defined at module level on the default registry so the normalizer and embedder
each expose their own metrics when run as separate processes, and unit tests
can read observations without any HTTP server. Freshness buckets are sized for
the project's SLO story: the interesting range is sub-second to a few minutes.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

LATENCY_BUCKETS = (0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)

NORMALIZED_EVENTS = Counter(
    "freshet_normalizer_events_total",
    "Events validated and republished by the normalizer",
)
INVALID_EVENTS = Counter(
    "freshet_normalizer_invalid_total",
    "Payloads that failed validation (dead-letter topic arrives in M4)",
)
INGEST_LAG = Histogram(
    "freshet_ingest_lag_seconds",
    "Seconds from event time (ts) to pipeline receipt (ingested_at)",
    buckets=LATENCY_BUCKETS,
)

INDEXED_EVENTS = Counter(
    "freshet_embedder_events_total",
    "Events embedded and upserted into pgvector",
)
FRESHNESS = Histogram(
    "freshet_freshness_seconds",
    "Event->queryable freshness: seconds from ts to indexed_at",
    buckets=LATENCY_BUCKETS,
)


def start_metrics_server(port: int) -> None:
    """Expose /metrics on the given port; 0 disables (tests, library callers)."""
    if port:
        start_http_server(port)
```

- [ ] **Step 5: Run tests**

Run: `pytest -q tests/test_metrics.py` — expected: `3 passed`.
Then full suite: `pytest -q` — expected: `27 passed, 2 deselected`.

- [ ] **Step 6: Commit**

```bash
git add freshet/pipeline/metrics.py tests/test_metrics.py pyproject.toml
git commit -m "Add Prometheus metrics module for pipeline workers"
```

### Task 2: Instrument the normalizer

**Files:**
- Modify: `freshet/pipeline/normalizer.py`
- Test: append to `tests/test_normalizer.py`

- [ ] **Step 1: Write the failing test (append to `tests/test_normalizer.py`)**

```python
def test_observe_normalized_records_metrics():
    from prometheus_client import REGISTRY

    from freshet.pipeline.normalizer import observe_normalized

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    ev.ingested_at = ev.ts + timedelta(seconds=0.7)

    events_before = REGISTRY.get_sample_value("freshet_normalizer_events_total") or 0
    lag_count_before = REGISTRY.get_sample_value("freshet_ingest_lag_seconds_count") or 0
    lag_sum_before = REGISTRY.get_sample_value("freshet_ingest_lag_seconds_sum") or 0

    observe_normalized(ev)

    assert REGISTRY.get_sample_value("freshet_normalizer_events_total") == events_before + 1
    assert REGISTRY.get_sample_value("freshet_ingest_lag_seconds_count") == lag_count_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_ingest_lag_seconds_sum") - lag_sum_before - 0.7) < 1e-6
```

Also add `timedelta` to the datetime import at the top of the file:

```python
from datetime import datetime, timedelta, timezone
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_normalizer.py`
Expected: FAIL with `ImportError: cannot import name 'observe_normalized'`

- [ ] **Step 3: Instrument `freshet/pipeline/normalizer.py`**

Add to the imports (after the `from freshet.common.schemas import Event` line):

```python
from freshet.pipeline.metrics import (
    INGEST_LAG,
    INVALID_EVENTS,
    NORMALIZED_EVENTS,
    start_metrics_server,
)
```

Add after `normalize()`:

```python
def observe_normalized(ev: Event) -> None:
    """Record metrics for one validated, ingested-stamped event."""
    NORMALIZED_EVENTS.inc()
    if ev.ingested_at is not None:
        INGEST_LAG.observe((ev.ingested_at - ev.ts).total_seconds())
```

Change `run()`'s signature to:

```python
def run(
    brokers: str,
    group: str = "normalizer",
    max_messages: Optional[int] = None,
    raw_topic: str = RAW_TOPIC,
    normalized_topic: str = NORMALIZED_TOPIC,
    metrics_port: int = 0,
) -> int:
```

Add as the first line of `run()`'s body:

```python
    start_metrics_server(metrics_port)
```

In `handle()`, add `INVALID_EVENTS.inc()` on the invalid path (before the print), and `observe_normalized(ev)` after `producer.poll(0)`:

```python
    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            INVALID_EVENTS.inc()
            print(f"[normalizer] skipped invalid payload ({skipped} so far)")
            return
        # key by service to preserve per-service ordering downstream
        producer.produce(normalized_topic, key=ev.service, value=ev.model_dump_json())
        producer.poll(0)
        observe_normalized(ev)
```

In `main()`, add the argument and pass it through:

```python
    p.add_argument("--metrics-port", type=int, default=8001, help="Prometheus /metrics port (0 disables)")
```

```python
    n = run(a.brokers, group=a.group, max_messages=a.max, metrics_port=a.metrics_port)
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_normalizer.py` — expected: `3 passed`. Full suite: `pytest -q` — expected: `28 passed, 2 deselected`.

- [ ] **Step 5: Commit**

```bash
git add freshet/pipeline/normalizer.py tests/test_normalizer.py
git commit -m "Instrument normalizer with Prometheus metrics"
```

### Task 3: Instrument the embedder

**Files:**
- Modify: `freshet/pipeline/embedder.py`
- Test: append to `tests/test_embedder.py`

- [ ] **Step 1: Write the failing test (append to `tests/test_embedder.py`)**

```python
def test_observe_indexed_records_freshness():
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    now = ev.ts + timedelta(seconds=2.5)
    rec = to_vector_record(ev, now=now)

    events_before = REGISTRY.get_sample_value("freshet_embedder_events_total") or 0
    sum_before = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") or 0

    observe_indexed(rec)

    assert REGISTRY.get_sample_value("freshet_embedder_events_total") == events_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_freshness_seconds_sum") - sum_before - 2.5) < 1e-6
```

Also extend the datetime import at the top of the file:

```python
from datetime import datetime, timedelta, timezone
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_embedder.py`
Expected: FAIL with `ImportError: cannot import name 'observe_indexed'`

- [ ] **Step 3: Instrument `freshet/pipeline/embedder.py`**

Add to the imports (after the embedding import):

```python
from freshet.pipeline.metrics import FRESHNESS, INDEXED_EVENTS, start_metrics_server
```

Add after `upsert_record()`:

```python
def observe_indexed(rec: VectorRecord) -> None:
    """Record metrics for one indexed (queryable) record."""
    INDEXED_EVENTS.inc()
    FRESHNESS.observe((rec.indexed_at - rec.ts).total_seconds())
```

Change `run()`'s signature to:

```python
def run(
    brokers: str,
    group: str = "embedder",
    max_messages: Optional[int] = None,
    topic: str = NORMALIZED_TOPIC,
    embedder: Optional[Embedder] = None,
    dsn: Optional[str] = None,
    metrics_port: int = 0,
) -> int:
```

Add `start_metrics_server(metrics_port)` as the first line of `run()`'s body, and call `observe_indexed(rec)` in `handle()` immediately after `upsert_record(conn, rec, vector)`.

In `main()`:

```python
    p.add_argument("--metrics-port", type=int, default=8002, help="Prometheus /metrics port (0 disables)")
```

```python
    n = run(a.brokers, group=a.group, max_messages=a.max, embedder=make_embedder(a.embedder), dsn=a.dsn, metrics_port=a.metrics_port)
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_embedder.py` — expected: `3 passed`. Full suite: `pytest -q` — expected: `29 passed, 2 deselected`.

- [ ] **Step 5: Commit**

```bash
git add freshet/pipeline/embedder.py tests/test_embedder.py
git commit -m "Instrument embedder with freshness metrics"
```

### Task 4: Prometheus + Grafana compose services

**Files:**
- Create: `observability/prometheus.yml`
- Modify: `docker-compose.yml`, `Makefile`

- [ ] **Step 1: Create `observability/prometheus.yml`**

```yaml
# Scrapes the host-side workers (they run outside compose) and Redpanda's
# built-in /public_metrics. 5s interval keeps dashboard latency demo-friendly.
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: redpanda
    metrics_path: /public_metrics
    static_configs:
      - targets: ["redpanda:9644"]

  - job_name: freshet-workers
    static_configs:
      - targets:
          - "host.docker.internal:8001"   # normalizer
          - "host.docker.internal:8002"   # embedder
```

- [ ] **Step 2: Add the two services to `docker-compose.yml`** (append after the `postgres` service, before `volumes:`; both profile-gated so `make up` stays lean)

```yaml
  prometheus:
    image: prom/prometheus:v2.53.0
    container_name: freshet-prometheus
    profiles: [obs]
    ports:
      - "9090:9090"
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"   # workers run on the host

  grafana:
    image: grafana/grafana:11.1.0
    container_name: freshet-grafana
    profiles: [obs]
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards:ro
```

- [ ] **Step 3: Makefile — add `up-obs`, make `down` profile-aware**

Add `up-obs` to `.PHONY`. Add after the `up` target:

```make
# Bring up the stack plus Prometheus (:9090) and Grafana (:3000).
up-obs:
	COMPOSE_PROFILES=obs $(MAKE) up
```

Change the `down` recipe so profile-gated services are also removed:

```make
down:
	COMPOSE_PROFILES=obs $(COMPOSE) down -v
```

- [ ] **Step 4: Verify what you can without Docker**

```bash
docker compose config --quiet 2>/dev/null || true   # if docker available: validates YAML
make -n up-obs    # prints: COMPOSE_PROFILES=obs make up
make -n down      # prints: COMPOSE_PROFILES=obs docker compose down -v
pytest -q         # 29 passed, 2 deselected
```

If Docker is reachable, also: `make up-obs` then `curl -s localhost:9090/-/ready` → `Prometheus Server is Ready.` If not, report as pending controller verification.

- [ ] **Step 5: Commit**

```bash
git add observability/prometheus.yml docker-compose.yml Makefile
git commit -m "Add profile-gated Prometheus and Grafana services"
```

### Task 5: Grafana provisioning + the committed dashboard

**Files:**
- Create: `observability/grafana/provisioning/datasources/prometheus.yml`
- Create: `observability/grafana/provisioning/dashboards/freshet.yml`
- Create: `observability/grafana/dashboards/freshet-pipeline.json`

- [ ] **Step 1: Create `observability/grafana/provisioning/datasources/prometheus.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 2: Create `observability/grafana/provisioning/dashboards/freshet.yml`**

```yaml
apiVersion: 1
providers:
  - name: freshet
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 3: Create `observability/grafana/dashboards/freshet-pipeline.json`**

```json
{
  "uid": "freshet-pipeline",
  "title": "Freshet pipeline",
  "schemaVersion": 39,
  "version": 1,
  "editable": true,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "type": "stat",
      "title": "Freshness p50",
      "gridPos": { "x": 0, "y": 0, "w": 6, "h": 5 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))",
          "refId": "A"
        }
      ]
    },
    {
      "id": 2,
      "type": "stat",
      "title": "Freshness p95",
      "gridPos": { "x": 6, "y": 0, "w": 6, "h": 5 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))",
          "refId": "A"
        }
      ]
    },
    {
      "id": 3,
      "type": "stat",
      "title": "Invalid events (dead-letter candidates)",
      "gridPos": { "x": 12, "y": 0, "w": 6, "h": 5 },
      "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
      "targets": [
        { "expr": "freshet_normalizer_invalid_total", "refId": "A" }
      ]
    },
    {
      "id": 4,
      "type": "stat",
      "title": "Events indexed",
      "gridPos": { "x": 18, "y": 0, "w": 6, "h": 5 },
      "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
      "targets": [
        { "expr": "freshet_embedder_events_total", "refId": "A" }
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Event -> queryable freshness (percentiles)",
      "gridPos": { "x": 0, "y": 5, "w": 12, "h": 9 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))",
          "legendFormat": "p50",
          "refId": "A"
        },
        {
          "expr": "histogram_quantile(0.95, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))",
          "legendFormat": "p95",
          "refId": "B"
        },
        {
          "expr": "histogram_quantile(0.99, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))",
          "legendFormat": "p99",
          "refId": "C"
        }
      ]
    },
    {
      "id": 6,
      "type": "timeseries",
      "title": "Throughput (events/s)",
      "gridPos": { "x": 12, "y": 5, "w": 12, "h": 9 },
      "fieldConfig": { "defaults": { "unit": "ops" }, "overrides": [] },
      "targets": [
        {
          "expr": "rate(freshet_normalizer_events_total[1m])",
          "legendFormat": "normalizer",
          "refId": "A"
        },
        {
          "expr": "rate(freshet_embedder_events_total[1m])",
          "legendFormat": "embedder",
          "refId": "B"
        }
      ]
    },
    {
      "id": 7,
      "type": "timeseries",
      "title": "Consumer lag (messages)",
      "gridPos": { "x": 0, "y": 14, "w": 24, "h": 9 },
      "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
      "targets": [
        {
          "expr": "clamp_min(sum by (redpanda_group) (redpanda_kafka_max_offset{redpanda_namespace=\"kafka\",redpanda_topic=~\"raw.events|normalized.events\"} - on(redpanda_topic, redpanda_partition) group_right() redpanda_kafka_consumer_group_committed_offset{redpanda_group=~\"normalizer|embedder\"}), 0)",
          "legendFormat": "{{redpanda_group}}",
          "refId": "A"
        }
      ]
    }
  ]
}
```

- [ ] **Step 4: Verify JSON validity**

Run: `python3 -m json.tool observability/grafana/dashboards/freshet-pipeline.json > /dev/null && echo valid`
Expected: `valid`

- [ ] **Step 5: Commit**

```bash
git add observability/grafana
git commit -m "Add provisioned Grafana dashboard for pipeline metrics"
```

### Task 6: Live verification + README

**Files:**
- Modify: `README.md`

This task is mostly stack verification (controller runs the Docker parts if the implementer's sandbox can't).

- [ ] **Step 1: Bring up the observability stack and generate load**

```bash
make down && make up-obs
source .venv/bin/activate
make slice          # or EMBEDDER=stub make slice
```

- [ ] **Step 2: Verify Prometheus sees all targets**

```bash
curl -s localhost:9090/api/v1/targets | python3 -c "import json,sys; [print(t['labels']['job'], t['scrapeUrl'], t['health']) for t in json.load(sys.stdin)['data']['activeTargets']]"
```

Expected: three targets (`redpanda`, and both `freshet-workers` endpoints) — redpanda `up`; the worker targets are `up` while `make slice` is running (between runs they are `down`, which is expected for on-demand workers).

- [ ] **Step 3: Verify the dashboard answers**

```bash
curl -s "localhost:9090/api/v1/query" --data-urlencode "query=histogram_quantile(0.95, sum(rate(freshet_freshness_seconds_bucket[1m])) by (le))" | python3 -m json.tool | head
curl -s "localhost:3000/api/dashboards/uid/freshet-pipeline" | python3 -c "import json,sys; d=json.load(sys.stdin)['dashboard']; print(d['title'], len(d['panels']), 'panels')"
```

Expected: the first query returns a numeric value (low single digits during/after a slice run); the second prints `Freshet pipeline 7 panels`. Open http://localhost:3000/d/freshet-pipeline during a slice run and confirm freshness/throughput/lag panels move — this is the M3 done-criterion.

- [ ] **Step 4: Add an Observability section to `README.md`** (after "What the demo shows", before "Other commands")

```markdown
## Observability

    make up-obs   # stack + Prometheus (:9090) + Grafana (:3000, anonymous admin)
    make slice    # generate load, then watch the dashboard

Grafana auto-provisions a "Freshet pipeline" dashboard at
http://localhost:3000/d/freshet-pipeline: freshness percentiles (p50/p95/p99),
pipeline throughput, per-group consumer lag, and invalid-event count. The
workers expose Prometheus metrics on :8001 (normalizer) and :8002 (embedder);
consumer lag comes from Redpanda's built-in metrics endpoint.
```

- [ ] **Step 5: Full suites one more time**

```bash
pytest -q                # 29 passed, 2 deselected
make test-integration    # 2 passed
```

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "Document the observability stack in the README"
```

---

## Definition of done (M3, from the spec)

- [ ] Workers export events-processed counters, ingest/index latency histograms, and the invalid/dead-letter placeholder counter
- [ ] Prometheus scrapes the workers and Redpanda's built-in metrics endpoint (consumer lag without an extra exporter)
- [ ] One provisioned Grafana dashboard, committed as JSON: freshness percentiles, consumer lag per group, throughput, invalid count
- [ ] Compose services are profile-gated; `make up` stays lean
- [ ] During a generator run the dashboard shows freshness and lag moving live
