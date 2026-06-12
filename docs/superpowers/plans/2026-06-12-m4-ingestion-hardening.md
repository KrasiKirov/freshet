# M4: Ingestion Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the brief's Phase 1: incident correlation persisted to Postgres, a dead-letter topic in both workers, chunking for long texts, graceful shutdown, topic replay, and a recorded 1→3 embedder scaling demonstration.

**Architecture:** A rule-based correlator (`freshet/pipeline/incidents.py`) gives the normalizer its first Postgres writes — idempotent SQL so at-least-once redelivery can't duplicate state. A shared dead-letter envelope (`freshet/pipeline/deadletter.py`) plus one `freshet_deadletter_total` metric replaces silent skips in both workers. `consume_loop` gains a stop event (SIGTERM/SIGINT → clean group leave, curing the documented re-run rebalance stall), an idle timeout (powers `make replay`), and synchronous offset commits. The embedder embeds one record per text chunk with deterministic `chk_<event_id>_<i>` ids. Topics get 3 partitions at `make up` so a 3-instance embedder group actually parallelizes; the scaling demo records throughput into a new root `RESULTS.md`.

**Tech Stack:** existing modules + psycopg array SQL; no new dependencies.

**Spec:** `docs/superpowers/specs/2026-06-10-m1-m7-roadmap-design.md` (M4 section). Carried-forward review items addressed here: per-message produce flush before offset commit (closes the normalizer's documented loss window), synchronous commits, per-instance `--metrics-port` for scaled workers.

**Conventions:** commit messages are a single imperative title — no body, no co-authors. Makefile recipes use TABS. Venv at `.venv`. Current suite: 29 unit passed, 2 integration deselected; expected at the end: **38 passed, 5 deselected**.

## Target file structure

```
db/init.sql                          # MODIFIED — incidents table
freshet/pipeline/incidents.py        # NEW — correlation rules + idempotent SQL
freshet/pipeline/deadletter.py       # NEW — topic name + JSON envelope
freshet/pipeline/chunking.py         # NEW — greedy word-packing chunker
freshet/pipeline/metrics.py          # MODIFIED — INVALID_EVENTS -> DEADLETTER_EVENTS
freshet/pipeline/normalizer.py       # MODIFIED — correlate, dead-letter, flush-before-commit, stop
freshet/pipeline/embedder.py         # MODIFIED — dead-letter, chunked records, stop, idle timeout
freshet/common/kafka_io.py           # MODIFIED — stop event, idle timeout, sync commit
observability/prometheus.yml         # MODIFIED — embedder ports 8003/8004
observability/grafana/dashboards/freshet-pipeline.json  # MODIFIED — dead-letter panel
Makefile                             # MODIFIED — topic creation in up; replay target
scripts/run_slice.sh                 # MODIFIED — stale rebalance-stall comment removed
scripts/run_scaling_demo.sh          # NEW
RESULTS.md                           # NEW — recorded scaling numbers
tests/test_incidents.py              # NEW
tests/test_deadletter.py             # NEW
tests/test_chunking.py               # NEW
tests/test_metrics.py                # MODIFIED — renamed counter
tests/test_embedder.py               # MODIFIED — records_for_event
tests/integration/test_db.py         # MODIFIED — incidents table check
tests/integration/test_hardening.py  # NEW — poison, incidents e2e, chunked replay
README.md                            # MODIFIED
```

---

### Task 1: Incident schema + correlation module

**Files:**
- Modify: `db/init.sql`
- Create: `freshet/pipeline/incidents.py`
- Test: `tests/test_incidents.py`

- [ ] **Step 1: Extend `db/init.sql`** — append after the existing index:

```sql
CREATE TABLE IF NOT EXISTS incidents (
    incident_id        text PRIMARY KEY,
    title              text NOT NULL DEFAULT '',
    services           text[] NOT NULL DEFAULT '{}',
    opened_at          timestamptz NOT NULL,
    resolved_at        timestamptz,
    resolution_summary text,
    event_ids          text[] NOT NULL DEFAULT '{}'
);
```

- [ ] **Step 2: Write failing tests `tests/test_incidents.py`**

```python
from freshet.common.schemas import Event, EventSource, Severity
from freshet.pipeline.incidents import incident_title, is_severe


def _ev(**kw) -> Event:
    base = dict(service="scheduler-api", source=EventSource.ALERT, type="error_spike", text="x")
    base.update(kw)
    return Event(**base)


def test_is_severe_by_type():
    assert is_severe(_ev(type="error_spike"))
    assert is_severe(_ev(type="latency_spike", source=EventSource.METRIC))
    assert is_severe(_ev(type="rollback", source=EventSource.DEPLOY))


def test_is_severe_by_severity():
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV1))
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV2))


def test_noise_is_not_severe():
    assert not is_severe(_ev(type="metric_sample", source=EventSource.METRIC))
    assert not is_severe(_ev(type="healthy", source=EventSource.ALERT))
    assert not is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV4))


def test_incident_title():
    assert incident_title(_ev(type="error_spike")) == "scheduler-api: error_spike"
```

- [ ] **Step 3: Run to verify failure**

Run: `pytest -q tests/test_incidents.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.pipeline.incidents'`

- [ ] **Step 4: Implement `freshet/pipeline/incidents.py`**

```python
"""Incident correlation: attach events to incidents and persist state.

M4 scope: rule-based correlation suited to the synthetic stream. An event with
an explicit incident_id is recorded against it. A severe event (error/latency
spike, rollback, or SEV1/SEV2) without one joins the open incident on its
service, or opens a new one. A healthy event resolves its incident; an RCA
event records the resolution summary. All writes are idempotent (guarded array
appends, COALESCE on resolution fields) so at-least-once redelivery cannot
duplicate or regress state.
"""

from __future__ import annotations

import uuid
from typing import Optional

from freshet.common.schemas import Event, EventType, Severity

SEVERE_TYPES = {
    EventType.ERROR_SPIKE.value,
    EventType.LATENCY_SPIKE.value,
    EventType.ROLLBACK.value,
}


def is_severe(ev: Event) -> bool:
    return ev.type in SEVERE_TYPES or ev.severity in (Severity.SEV1, Severity.SEV2)


def incident_title(ev: Event) -> str:
    return f"{ev.service}: {ev.type}"


def _new_incident_id() -> str:
    return f"INC_{uuid.uuid4().hex[:12]}"


UPSERT_INCIDENT_SQL = """
INSERT INTO incidents (incident_id, title, services, opened_at, event_ids)
VALUES (%(id)s, %(title)s, ARRAY[%(service)s], %(ts)s, ARRAY[%(event_id)s])
ON CONFLICT (incident_id) DO UPDATE SET
    services = CASE WHEN %(service)s = ANY(incidents.services)
                    THEN incidents.services
                    ELSE array_append(incidents.services, %(service)s) END,
    event_ids = CASE WHEN %(event_id)s = ANY(incidents.event_ids)
                     THEN incidents.event_ids
                     ELSE array_append(incidents.event_ids, %(event_id)s) END,
    opened_at = LEAST(incidents.opened_at, EXCLUDED.opened_at)
"""

RESOLVE_SQL = (
    "UPDATE incidents SET resolved_at = COALESCE(resolved_at, %(ts)s)"
    " WHERE incident_id = %(id)s"
)
SUMMARY_SQL = (
    "UPDATE incidents SET resolution_summary = COALESCE(resolution_summary, %(text)s)"
    " WHERE incident_id = %(id)s"
)
FIND_OPEN_SQL = (
    "SELECT incident_id FROM incidents"
    " WHERE resolved_at IS NULL AND %(service)s = ANY(services)"
    " ORDER BY opened_at LIMIT 1"
)


def correlate(conn, ev: Event) -> Optional[str]:
    """Record ev against its incident; return the incident_id or None.

    Resolution rules apply only to events explicitly carrying an incident_id —
    a routine 'healthy' noise event must not close an open incident.
    """
    incident_id = ev.incident_id
    if incident_id is None and is_severe(ev):
        row = conn.execute(FIND_OPEN_SQL, {"service": ev.service}).fetchone()
        incident_id = row[0] if row else _new_incident_id()
    if incident_id is None:
        return None
    conn.execute(
        UPSERT_INCIDENT_SQL,
        {
            "id": incident_id,
            "title": incident_title(ev),
            "service": ev.service,
            "ts": ev.ts,
            "event_id": ev.event_id,
        },
    )
    if ev.incident_id is not None and ev.type == EventType.HEALTHY.value:
        conn.execute(RESOLVE_SQL, {"ts": ev.ts, "id": incident_id})
    elif ev.incident_id is not None and ev.type == EventType.RCA.value:
        conn.execute(SUMMARY_SQL, {"text": ev.text, "id": incident_id})
    return incident_id
```

- [ ] **Step 5: Run tests**

Run: `pytest -q tests/test_incidents.py` — expected `4 passed`. Full suite: `pytest -q` — expected `33 passed, 2 deselected`.

- [ ] **Step 6: Commit**

```bash
git add db/init.sql freshet/pipeline/incidents.py tests/test_incidents.py
git commit -m "Add incidents schema and rule-based correlation module"
```

### Task 2: Wire correlation into the normalizer

**Files:**
- Modify: `freshet/pipeline/normalizer.py`

The normalizer gains its first Postgres connection. DB behavior is covered by the Task 8 integration test; unit tests of `normalize()` are unaffected.

- [ ] **Step 1: Add imports** (after the metrics import block):

```python
from freshet.pipeline.incidents import correlate
```

- [ ] **Step 2: Extend `run()`** — new signature (note the new `dsn`):

```python
def run(
    brokers: str,
    group: str = "normalizer",
    max_messages: Optional[int] = None,
    raw_topic: str = RAW_TOPIC,
    normalized_topic: str = NORMALIZED_TOPIC,
    metrics_port: int = 0,
    dsn: Optional[str] = None,
) -> int:
```

Body changes: after `start_metrics_server(metrics_port)` add the connection (lazy import next to the kafka one):

```python
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop, make_producer

    conn = connect(dsn)
    producer = make_producer(brokers)
```

In `handle()`, between `normalize` and the produce, attach the incident:

```python
        assigned = correlate(conn, ev)
        if assigned is not None:
            ev.incident_id = assigned
```

Wrap the loop so the connection always closes:

```python
    try:
        n = consume_loop(brokers, group, [raw_topic], handle, max_messages, auto_commit=False)
    finally:
        producer.flush()
        conn.close()
    return n
```

(The old trailing `producer.flush()` line moves into the `finally`.)

In `main()`, add `--dsn` and thread it:

```python
    p.add_argument("--dsn", default=None)
```

```python
    n = run(a.brokers, group=a.group, max_messages=a.max, metrics_port=a.metrics_port, dsn=a.dsn)
```

Update the module docstring's first paragraph to say correlation is now in scope (replace "Incident correlation and the dead-letter topic arrive in M4" appropriately — dead-letter lands in Task 3, so after Task 3 both clauses go).

- [ ] **Step 3: Run tests**

Run: `pytest -q` — expected `33 passed, 2 deselected` (normalizer unit tests don't touch `run()`).

- [ ] **Step 4: Commit**

```bash
git add freshet/pipeline/normalizer.py
git commit -m "Correlate events to incidents in the normalizer"
```

### Task 3: Dead-letter topic in both workers

**Files:**
- Create: `freshet/pipeline/deadletter.py`
- Modify: `freshet/pipeline/metrics.py`, `freshet/pipeline/normalizer.py`, `freshet/pipeline/embedder.py`, `observability/grafana/dashboards/freshet-pipeline.json`
- Test: `tests/test_deadletter.py`, modify `tests/test_metrics.py`

- [ ] **Step 1: Write failing test `tests/test_deadletter.py`**

```python
import json

from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter


def test_envelope_preserves_payload_and_context():
    out = json.loads(build_deadletter("validation failed", '{"bad": true}', "raw.events"))
    assert out["error"] == "validation failed"
    assert out["payload"] == '{"bad": true}'
    assert out["source_topic"] == "raw.events"
    assert "dead_lettered_at" in out
    assert DEADLETTER_TOPIC == "deadletter.events"
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_deadletter.py` — expected `ModuleNotFoundError`.

- [ ] **Step 3: Implement `freshet/pipeline/deadletter.py`**

```python
"""Dead-letter support: unprocessable messages are recorded, never dropped.

The envelope keeps the original payload byte-for-byte so a fixed consumer (or
a human) can replay it later, plus enough context to know what failed where.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

DEADLETTER_TOPIC = "deadletter.events"


def build_deadletter(error: str, payload: str, source_topic: str) -> str:
    return json.dumps(
        {
            "error": error,
            "source_topic": source_topic,
            "payload": payload,
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
        }
    )
```

- [ ] **Step 4: Rename the metric in `freshet/pipeline/metrics.py`**

Replace the `INVALID_EVENTS` definition with:

```python
DEADLETTER_EVENTS = Counter(
    "freshet_deadletter_total",
    "Messages routed to the dead-letter topic (normalizer + embedder)",
)
```

In `tests/test_metrics.py`, update the import (`INVALID_EVENTS` → `DEADLETTER_EVENTS`) and the middle block of `test_counters_increment` to:

```python
    before = _value("freshet_deadletter_total")
    DEADLETTER_EVENTS.inc()
    assert _value("freshet_deadletter_total") == before + 1
```

- [ ] **Step 5: Normalizer — dead-letter the invalid path and flush-before-commit**

In `freshet/pipeline/normalizer.py`: update the metrics import (`INVALID_EVENTS` → `DEADLETTER_EVENTS`), add `from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter`, give `run()` a `deadletter_topic: str = DEADLETTER_TOPIC` parameter (after `normalized_topic`), and replace `handle()` with:

```python
    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            DEADLETTER_EVENTS.inc()
            producer.produce(deadletter_topic, value=build_deadletter("validation failed", value, raw_topic))
            producer.flush()
            print(f"[normalizer] dead-lettered invalid payload ({skipped} so far)")
            return
        assigned = correlate(conn, ev)
        if assigned is not None:
            ev.incident_id = assigned
        # key by service to preserve per-service ordering downstream
        producer.produce(normalized_topic, key=ev.service, value=ev.model_dump_json())
        # flush before the loop commits this offset: closes the documented
        # produce-before-commit loss window (per-message flush is fine at demo rate)
        producer.flush()
        observe_normalized(ev)
```

Update the module docstring: correlation and dead-lettering are now implemented; remove the loss-window caveat sentence entirely (it is cured by the flush) — the docstring should now read:

```python
"""Normalizer worker: raw.events -> validate -> correlate -> normalized.events.

Validates each payload against the canonical Event schema, stamps ingested_at,
attaches the event to an incident (state in Postgres), and republishes keyed
by service. Invalid payloads go to the dead-letter topic, never silently
dropped. The produce is flushed before the consumer offset commits, so a crash
can only cause redelivery (at-least-once), not loss.

Run (stack up first):
    python -m freshet.pipeline.normalizer --brokers localhost:9092
"""
```

- [ ] **Step 6: Embedder — dead-letter unparseable messages**

In `freshet/pipeline/embedder.py`: add imports `from freshet.common.kafka_io import consume_loop, make_producer` is already lazy inside `run()` — extend that lazy import to include `make_producer`; add top-level `from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter` and extend the metrics import with `DEADLETTER_EVENTS`. Give `run()` a `deadletter_topic: str = DEADLETTER_TOPIC` parameter (after `dsn`). In `run()`, create `producer = make_producer(brokers)` next to `conn = connect(dsn)`, and replace the start of `handle()`:

```python
    def handle(value: str) -> None:
        try:
            ev = Event.model_validate_json(value)
        except Exception as e:
            DEADLETTER_EVENTS.inc()
            producer.produce(deadletter_topic, value=build_deadletter(str(e), value, topic))
            producer.flush()
            return
        if not ev.text.strip():
            return
```

(the rest of `handle()` is unchanged). Add `producer.flush()` next to `conn.close()` in the `finally`.

- [ ] **Step 7: Update the dashboard panel** in `observability/grafana/dashboards/freshet-pipeline.json` — panel id 3: title becomes `"Dead-lettered messages"`, expr becomes `"sum(freshet_deadletter_total)"`.

- [ ] **Step 8: Run tests**

`python3 -m json.tool observability/grafana/dashboards/freshet-pipeline.json > /dev/null && echo valid`, then `pytest -q` — expected `34 passed, 2 deselected`.

- [ ] **Step 9: Commit**

```bash
git add freshet/pipeline/deadletter.py freshet/pipeline/metrics.py freshet/pipeline/normalizer.py freshet/pipeline/embedder.py observability/grafana/dashboards/freshet-pipeline.json tests/test_deadletter.py tests/test_metrics.py
git commit -m "Route unprocessable messages to a dead-letter topic"
```

### Task 4: Graceful shutdown, idle timeout, synchronous commits

**Files:**
- Modify: `freshet/common/kafka_io.py`, `freshet/pipeline/normalizer.py`, `freshet/pipeline/embedder.py`, `scripts/run_slice.sh`

- [ ] **Step 1: Extend `consume_loop`** — full new version of the function (note `import time` and `import threading` go at the top of the module under `from __future__ ...`):

```python
import threading
import time
```

```python
def consume_loop(
    brokers: str,
    group_id: str,
    topics: list[str],
    handler: Callable[[str], None],
    max_messages: Optional[int] = None,
    auto_commit: bool = True,
    stop: Optional[threading.Event] = None,
    idle_timeout_s: Optional[float] = None,
) -> int:
    """Run a simple consume loop, calling handler(value_str) per message.

    With auto_commit=False the offset is committed synchronously after the
    handler returns, so an unprocessed message is redelivered after a crash
    (at-least-once). `stop` lets a signal handler end the loop cleanly — the
    consumer then leaves its group on close(), so a restart is not stalled by
    a session timeout. `idle_timeout_s` ends the loop after that many seconds
    without a message (used by replay). Returns the number of messages
    processed; `max_messages` bounds the loop for tests.
    """
    c = make_consumer(brokers, group_id, topics, auto_commit=auto_commit)
    n = 0
    last_msg = time.monotonic()
    try:
        while max_messages is None or n < max_messages:
            if stop is not None and stop.is_set():
                break
            msg = c.poll(1.0)
            if msg is None:
                if idle_timeout_s is not None and time.monotonic() - last_msg >= idle_timeout_s:
                    break
                continue
            if msg.error():
                # consumer events (not records): nothing to commit or dead-letter
                print(f"[consume error] {msg.error()}")
                continue
            last_msg = time.monotonic()
            handler(msg.value().decode("utf-8"))
            if not auto_commit:
                c.commit(message=msg, asynchronous=False)
            n += 1
    finally:
        c.close()
    return n
```

- [ ] **Step 2: Thread `stop` through both workers**

Normalizer `run()` gains a final parameter `stop: Optional["threading.Event"] = None` (add `import threading` to its imports) and passes it: `consume_loop(..., auto_commit=False, stop=stop)`. Embedder `run()` gains two final parameters `stop: Optional["threading.Event"] = None, idle_timeout_s: Optional[float] = None` (add `import threading`) and passes both to its `consume_loop` call.

- [ ] **Step 3: Install signal handlers in both `main()`s**

In both workers, add `import signal` and `import threading` (top of file), and in `main()` before calling `run()`:

```python
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
```

then pass `stop=stop` to `run(...)`.

- [ ] **Step 4: Remove the stale rebalance-stall comment from `scripts/run_slice.sh`**

Delete these three lines from the header (the stall is cured — SIGTERM now leaves the group cleanly):

```
# Re-runs also stall ~30s while the broker times out the previous run's
# killed workers before reassigning partitions (graceful shutdown lands in M4),
# which inflates that run's freshness numbers.
```

- [ ] **Step 5: Run tests**

Run: `pytest -q` — expected `34 passed, 2 deselected`. (Loop changes are exercised live in Tasks 8–9.)

- [ ] **Step 6: Commit**

```bash
git add freshet/common/kafka_io.py freshet/pipeline/normalizer.py freshet/pipeline/embedder.py scripts/run_slice.sh
git commit -m "Add graceful shutdown, idle timeout, and synchronous commits"
```

### Task 5: Chunking for long texts

**Files:**
- Create: `freshet/pipeline/chunking.py`
- Modify: `freshet/pipeline/embedder.py`
- Test: `tests/test_chunking.py`, rewrite `tests/test_embedder.py`

- [ ] **Step 1: Write failing tests `tests/test_chunking.py`**

```python
from freshet.pipeline.chunking import chunk_text


def test_short_text_is_one_chunk():
    assert chunk_text("error spike on scheduler-api") == ["error spike on scheduler-api"]


def test_long_text_packs_words_under_limit():
    text = " ".join(f"word{i}" for i in range(200))
    chunks = chunk_text(text, max_chars=100)
    assert len(chunks) > 1
    assert all(len(c) <= 100 for c in chunks)
    assert " ".join(chunks) == text


def test_blank_text_is_empty():
    assert chunk_text("") == []
    assert chunk_text("   ") == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_chunking.py` — expected `ModuleNotFoundError`.

- [ ] **Step 3: Implement `freshet/pipeline/chunking.py`**

```python
"""Greedy word-packing chunker for long event texts (e.g. postmortems).

Words are never split; each chunk stays under max_chars. 400 chars keeps a
chunk comfortably inside the embedding model's input window while leaving
retrieval granularity per-paragraph-ish.
"""

from __future__ import annotations

DEFAULT_MAX_CHARS = 400


def chunk_text(text: str, max_chars: int = DEFAULT_MAX_CHARS) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) <= max_chars:
            current += " " + word
        else:
            chunks.append(current)
            current = word
    chunks.append(current)
    return chunks
```

- [ ] **Step 4: Replace `to_vector_record` with `records_for_event` in `freshet/pipeline/embedder.py`**

Add `from freshet.pipeline.chunking import chunk_text` to the imports. Replace the `to_vector_record` function with:

```python
def records_for_event(ev: Event, now: Optional[datetime] = None) -> list[VectorRecord]:
    """One record per text chunk. chunk_id derives from event_id + index, so
    redelivery and replay overwrite the same rows (idempotent). Blank text
    yields no records."""
    stamp = now or datetime.now(timezone.utc)
    return [
        VectorRecord(
            chunk_id=f"chk_{ev.event_id}_{i}",
            event_id=ev.event_id,
            incident_id=ev.incident_id,
            service=ev.service,
            ts=ev.ts,
            indexed_at=stamp,
            text=chunk,
            source=ev.source,
        )
        for i, chunk in enumerate(chunk_text(ev.text))
    ]
```

Replace the body of `handle()` after the dead-letter guard with:

```python
        records = records_for_event(ev)
        if not records:
            return
        vectors = emb.encode([r.text for r in records])
        for rec, vector in zip(records, vectors):
            upsert_record(conn, rec, vector)
            observe_indexed(rec)
```

(the explicit blank-text `if not ev.text.strip(): return` guard is removed — `records_for_event` returning `[]` covers it). Update the module docstring's "One chunk per event at M2; chunking ... arrives in M4" sentence to state chunking is implemented.

- [ ] **Step 5: Rewrite `tests/test_embedder.py`** — full new content:

```python
from datetime import datetime, timedelta, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import records_for_event


def test_records_have_deterministic_chunk_ids():
    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="5xx spike", incident_id="INC-1")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    [a] = records_for_event(ev, now=now)
    [b] = records_for_event(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_long_text_yields_multiple_records():
    text = " ".join(f"word{i}" for i in range(300))
    ev = Event(service="s", source=EventSource.POSTMORTEM, type="rca", text=text)
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    records = records_for_event(ev, now=now)
    assert len(records) > 1
    assert [r.chunk_id for r in records] == [f"chk_{ev.event_id}_{i}" for i in range(len(records))]
    assert all(r.indexed_at == now for r in records)
    assert " ".join(r.text for r in records) == text


def test_records_copy_fields_and_blank_text_is_empty():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    [rec] = records_for_event(ev, now=now)
    assert rec.event_id == ev.event_id
    assert rec.service == "s"
    assert rec.ts == ev.ts
    assert rec.indexed_at == now
    assert rec.text == "hello"
    assert rec.source is EventSource.CHAT
    assert rec.incident_id is None
    assert records_for_event(Event(service="s", source=EventSource.CHAT, type="message", text="  "), now=now) == []


def test_observe_indexed_records_freshness():
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    now = ev.ts + timedelta(seconds=2.5)
    [rec] = records_for_event(ev, now=now)

    events_before = REGISTRY.get_sample_value("freshet_embedder_events_total") or 0
    sum_before = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") or 0

    observe_indexed(rec)

    assert REGISTRY.get_sample_value("freshet_embedder_events_total") == events_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_freshness_seconds_sum") - sum_before - 2.5) < 1e-6
```

- [ ] **Step 6: Run tests**

Run: `pytest -q` — expected `38 passed, 2 deselected`.

- [ ] **Step 7: Commit**

```bash
git add freshet/pipeline/chunking.py freshet/pipeline/embedder.py tests/test_chunking.py tests/test_embedder.py
git commit -m "Chunk long texts into per-chunk vector records"
```

### Task 6: Topic partitions + scaled scrape targets

**Files:**
- Modify: `Makefile`, `observability/prometheus.yml`

- [ ] **Step 1: Create topics with 3 partitions in `make up`** — append to the `up` recipe (after `@echo "stack healthy."`, TAB-indented):

```make
	@docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events -p 3 >/dev/null 2>&1 || true
	@echo "topics ready (3 partitions)."
```

(`|| true`: already-existing topics make rpk exit nonzero; idempotent. NOTE: topics auto-created with 1 partition by an old volume keep 1 partition — scaling needs a fresh `make down && make up`.)

- [ ] **Step 2: Add scaled embedder scrape targets** in `observability/prometheus.yml` — the freshet-workers job becomes:

```yaml
  - job_name: freshet-workers
    static_configs:
      - targets:
          - "host.docker.internal:8001"   # normalizer
          - "host.docker.internal:8002"   # embedder 1
          - "host.docker.internal:8003"   # embedder 2 (scaling demo)
          - "host.docker.internal:8004"   # embedder 3 (scaling demo)
```

- [ ] **Step 3: Verify**

`make -n up | tail -2` shows the rpk line; `pytest -q` — `38 passed, 2 deselected`.

- [ ] **Step 4: Commit**

```bash
git add Makefile observability/prometheus.yml
git commit -m "Create 3-partition topics and scrape scaled embedders"
```

### Task 7: Replay target

**Files:**
- Modify: `freshet/pipeline/embedder.py`, `Makefile`

- [ ] **Step 1: Add `--idle-timeout` to the embedder `main()`**

```python
    p.add_argument("--idle-timeout", type=float, default=None, help="exit after N seconds without messages (replay)")
```

and pass `idle_timeout_s=a.idle_timeout` to `run(...)`.

- [ ] **Step 2: Add the Makefile target** (add `replay` to `.PHONY`; place after `slice`):

```make
# Re-index the whole corpus under a fresh consumer group (e.g. after a model
# change). Reads normalized.events from the beginning; idempotent upserts
# overwrite rows in place. EMBEDDER=stub skips the model download.
replay:
	$(PYTHON) -m freshet.pipeline.embedder --brokers localhost:9092 --group reindex-$$(date +%s) --embedder $${EMBEDDER:-minilm} --metrics-port 0 --idle-timeout 10
```

- [ ] **Step 3: Verify**

`make -n replay` prints the command; `pytest -q` — `38 passed, 2 deselected`.

- [ ] **Step 4: Commit**

```bash
git add freshet/pipeline/embedder.py Makefile
git commit -m "Add make replay for full corpus re-indexing"
```

### Task 8: Hardening integration tests

**Files:**
- Create: `tests/integration/test_hardening.py`
- Modify: `tests/integration/test_db.py`

- [ ] **Step 1: Extend `tests/integration/test_db.py`** — append inside `test_schema_applied`'s `try:` block (after the vector_records assertions):

```python
        inc_cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'incidents'"
            ).fetchall()
        }
        assert {
            "incident_id", "title", "services", "opened_at",
            "resolved_at", "resolution_summary", "event_ids",
        } <= inc_cols
```

- [ ] **Step 2: Create `tests/integration/test_hardening.py`**

```python
"""M4 hardening tests against the real stack: dead-lettering, incident
correlation, and chunked replay idempotency. Run via: make test-integration."""

import json
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

BROKERS = os.environ.get("FRESHET_BROKERS", "localhost:9092")


@pytest.fixture
def conn():
    from freshet.common.db import connect

    c = connect()
    c.execute("DELETE FROM vector_records")
    c.execute("DELETE FROM incidents")
    yield c
    c.close()


def _drain(topic: str, group: str, n: int) -> list[str]:
    from freshet.common.kafka_io import consume_loop

    out: list[str] = []
    consume_loop(BROKERS, group, [topic], out.append, max_messages=n, idle_timeout_s=15)
    return out


def test_poison_message_is_dead_lettered(conn):
    from freshet.common.kafka_io import make_producer
    from freshet.generator.generator import EventGenerator, live_stream
    from freshet.pipeline import normalizer

    run_id = uuid.uuid4().hex[:8]
    raw, norm, dl = (f"{t}.it{run_id}" for t in ("raw.events", "normalized.events", "deadletter.events"))

    producer = make_producer(BROKERS)
    events = list(live_stream(EventGenerator(seed=5, incident_after=0), count=2, spacing_s=0))
    producer.produce(raw, value=events[0].model_dump_json())
    producer.produce(raw, value=b"this is not json")
    for ev in events[1:]:
        producer.produce(raw, value=ev.model_dump_json())
    producer.flush()
    total = len(events) + 1

    n = normalizer.run(
        BROKERS, group=f"n-{run_id}", max_messages=total,
        raw_topic=raw, normalized_topic=norm, deadletter_topic=dl,
    )
    assert n == total

    dead = _drain(dl, f"dl-{run_id}", 1)
    assert len(dead) == 1
    envelope = json.loads(dead[0])
    assert envelope["payload"] == "this is not json"
    assert envelope["source_topic"] == raw
    assert "error" in envelope

    normalized = _drain(norm, f"nv-{run_id}", len(events))
    assert len(normalized) == len(events)


def test_scenario_incident_is_correlated_and_resolved(conn):
    from freshet.generator.generator import EventGenerator, KafkaSink, live_stream
    from freshet.pipeline import normalizer

    run_id = uuid.uuid4().hex[:8]
    raw, norm = f"raw.events.ic{run_id}", f"normalized.events.ic{run_id}"

    sink = KafkaSink(BROKERS, raw)
    produced = 0
    for ev in live_stream(EventGenerator(seed=7, incident_after=0), count=1, spacing_s=0):
        sink.write(ev)
        produced += 1
    sink.close()
    assert produced == 10  # 1 noise + 9 scripted incident events

    normalizer.run(BROKERS, group=f"n-{run_id}", max_messages=10, raw_topic=raw, normalized_topic=norm)

    row = conn.execute(
        "SELECT services, event_ids, resolved_at, resolution_summary FROM incidents"
        " WHERE incident_id = 'INC-DEMO-0001'"
    ).fetchone()
    assert row is not None
    services, event_ids, resolved_at, summary = row
    assert services == ["scheduler-api"]
    assert len(event_ids) == 9
    assert resolved_at is not None
    assert summary is not None and summary.startswith("Postmortem")

    # idempotency: a fresh group replays everything; state must not change
    normalizer.run(BROKERS, group=f"n2-{run_id}", max_messages=10, raw_topic=raw, normalized_topic=norm)
    event_ids2 = conn.execute(
        "SELECT event_ids FROM incidents WHERE incident_id = 'INC-DEMO-0001'"
    ).fetchone()[0]
    assert len(event_ids2) == 9


def test_long_text_chunks_and_replay_is_idempotent(conn):
    from freshet.common.kafka_io import make_producer
    from freshet.common.schemas import Event, EventSource
    from freshet.pipeline import embedder
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    norm = f"normalized.events.ch{run_id}"

    long_text = " ".join(f"finding{i}" for i in range(300))
    ev = Event(service="scheduler-api", source=EventSource.POSTMORTEM, type="rca", text=long_text)
    producer = make_producer(BROKERS)
    producer.produce(norm, value=ev.model_dump_json())
    producer.flush()

    embedder.run(BROKERS, group=f"e-{run_id}", max_messages=1, topic=norm, embedder=StubEmbedder())
    chunks = conn.execute(
        "SELECT count(*) FROM vector_records WHERE event_id = %s", (ev.event_id,)
    ).fetchone()[0]
    assert chunks > 1

    # replay with a fresh group (the make replay path): row count unchanged
    embedder.run(BROKERS, group=f"e2-{run_id}", max_messages=1, topic=norm, embedder=StubEmbedder())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE event_id = %s", (ev.event_id,)
    ).fetchone()[0] == chunks
```

- [ ] **Step 3: Run against the stack** (controller runs if sandbox lacks Docker)

```bash
make down && make up      # fresh volume: applies incidents schema, 3-partition topics
make test-integration
```

Expected: `5 passed` (2 existing + 3 new). Also `pytest -q` → `38 passed, 5 deselected`.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_hardening.py tests/integration/test_db.py
git commit -m "Add hardening integration tests"
```

### Task 9: Scaling demo + RESULTS.md + README

**Files:**
- Create: `scripts/run_scaling_demo.sh`, `RESULTS.md`
- Modify: `Makefile`, `README.md`

- [ ] **Step 1: Create `scripts/run_scaling_demo.sh`** (mode 755 — `chmod +x` or `git update-index --chmod=+x`)

```bash
#!/usr/bin/env bash
# Consumer-group scaling demo: drain a burst of events with WORKERS embedder
# instances and report throughput. Run twice (WORKERS=1, then WORKERS=3) on a
# fresh stack (make down && make up) — topics need their 3 partitions.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS="${WORKERS:-3}"
COUNT="${COUNT:-300}"           # noise events; total = COUNT + 9 scripted
EMBEDDER="${EMBEDDER:-minilm}"
BROKERS="${BROKERS:-localhost:9092}"
SEED="${SEED:-$(date +%s)}"
TOTAL=$((COUNT + 9))
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

"${PSQL[@]}" -v ON_ERROR_STOP=1 < db/init.sql > /dev/null
BEFORE=$("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records")
TARGET=$((BEFORE + TOTAL))

python3 -m freshet.pipeline.normalizer --brokers "$BROKERS" &
PIDS=($!)
for i in $(seq 1 "$WORKERS"); do
  python3 -m freshet.pipeline.embedder --brokers "$BROKERS" --embedder "$EMBEDDER" --metrics-port $((8001 + i)) &
  PIDS+=($!)
done
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT

sleep 5   # let the group settle and the model load before the clock starts
START=$(date +%s)
python3 -m freshet.generator --sink kafka --brokers "$BROKERS" --count "$COUNT" --seed "$SEED" --live --live-spacing 0.01

i=0
until [ "$("${PSQL[@]}" -tAc 'SELECT count(*) FROM vector_records')" -ge "$TARGET" ]; do
  i=$((i+1))
  if [ "$i" -ge 300 ]; then echo "ERROR: did not drain $TOTAL events within 300s"; exit 1; fi
  sleep 1
done
END=$(date +%s)
ELAPSED=$((END - START))
if [ "$ELAPSED" -eq 0 ]; then ELAPSED=1; fi
echo "drained $TOTAL events with $WORKERS embedder(s) in ${ELAPSED}s ($((TOTAL / ELAPSED)) ev/s)"
```

- [ ] **Step 2: Add a Makefile target** (add `scale-demo` to `.PHONY`, after `replay`):

```make
# Throughput demo: WORKERS=1 make scale-demo, then WORKERS=3 make scale-demo.
scale-demo:
	bash scripts/run_scaling_demo.sh
```

- [ ] **Step 3: Run the demo and record the numbers** (controller runs; fresh stack first):

```bash
make down && make up
WORKERS=1 make scale-demo     # record the ev/s
make down && make up
WORKERS=3 make scale-demo     # record the ev/s
```

- [ ] **Step 4: Create `RESULTS.md`** with the real measured numbers substituted for `<N>`:

```markdown
# Results

Reproducible numbers, newest first. Hardware context: Apple Silicon laptop,
single-node Redpanda + Postgres in Docker, workers on the host.

## M4 — consumer-group scaling (embedder, all-MiniLM-L6-v2)

309 live events burst at ~100 ev/s into 3-partition topics, time measured from
generation start to all events queryable in pgvector (`make scale-demo`):

| embedder instances | drain time | throughput |
|---|---|---|
| 1 | <N>s | <N> ev/s |
| 3 | <N>s | <N> ev/s |

Reproduce: `make down && make up && WORKERS=1 make scale-demo` (then WORKERS=3).

## M2 — event-to-queryable freshness (slice demo, real embedder)

p50 ≈ 2–3 s, p95 ≈ 6 s over 69 live events (`make slice`; printed by
`freshet.eval.freshness`). The full eval harness with committed artifacts is
M6.
```

- [ ] **Step 5: Update `README.md`**: in the intro paragraph, replace "Hybrid retrieval, dashboards, and the evaluation harness are upcoming milestones." with "Ingestion is hardened (incident correlation, dead-letter topic, graceful shutdown, replay, scaled consumers — see `RESULTS.md`); hybrid retrieval and the evaluation harness are upcoming milestones." In "Other commands" add:

```
    make replay           # re-index the corpus under a fresh consumer group
    make scale-demo       # WORKERS=1|3 throughput demonstration
```

- [ ] **Step 6: Final suites + live checks** (controller): `pytest -q` (38 passed, 5 deselected), `make test-integration` (5 passed), `make slice` still green end-to-end, and during a slice run the dashboard's dead-letter panel reads 0 while a hand-produced garbage message (`docker exec freshet-redpanda rpk topic produce raw.events` + junk line) makes it tick to 1.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_scaling_demo.sh Makefile RESULTS.md README.md
git commit -m "Add scaling demo and record M4 results"
```

---

## Definition of done (M4, from the spec)

- [ ] Incident correlation in the normalizer; incidents table populated; scenario incident resolved with summary
- [ ] Dead-letter topic with a poison-message test; nothing silently dropped
- [ ] Chunking for long texts (per-chunk idempotent rows)
- [ ] Graceful shutdown (clean group leave); replay support (`make replay`)
- [ ] 1 → 3 embedder instances raises throughput, recorded in RESULTS.md
- [ ] Dashboard reflects dead-letters; brief Phase 1 done-criteria hold
