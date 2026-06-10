# M1 + M2: Packaging & Vertical Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the repo into an installable package (M1), then build minimal versions of every remaining pipeline stage so events flow generator → Kafka → normalizer → embedder → pgvector → query API, with a printed freshness report (M2).

**Architecture:** Two consumer-group workers (normalizer, embedder) connect the existing generator to Postgres/pgvector via two Kafka topics (`raw.events`, `normalized.events`). A FastAPI endpoint does vector-only top-k. Embeddings are behind a tiny interface: a deterministic stub (tests/CI, no model download) and local MiniLM (real runs). Offsets commit only after successful processing; upserts are idempotent via deterministic chunk ids.

**Tech Stack:** Python 3.12+, pydantic v2, confluent-kafka, psycopg3, pgvector (SQL extension only — no Python lib), sentence-transformers (optional extra), FastAPI, pytest.

**Spec:** `docs/superpowers/specs/2026-06-10-m1-m7-roadmap-design.md` (M1 and M2 sections). M3–M7 get their own plans later.

**Conventions for this repo:** commit messages are a single imperative title — no body, no co-author lines. Existing tests must stay green after every task.

---

## Target file structure

```
repo root/
  pyproject.toml            # NEW — package metadata, deps, pytest config
  LICENSE                   # NEW — MIT
  Makefile                  # MOVED from freshet/Makefile, updated
  docker-compose.yml        # MOVED from freshet/docker-compose.yml, + init.sql mount
  README.md                 # MOVED from freshet/README.md, updated
  BRIEF.md                  # MOVED from freshet/BRIEF.md
  db/init.sql               # NEW — pgvector extension + vector_records table
  scripts/run_slice.sh      # NEW — end-to-end slice demo
  freshet/                  # the Python package (was freshet/{common,generator,pipeline})
    __init__.py             # NEW (empty)
    common/
      __init__.py
      schemas.py            # unchanged content
      kafka_io.py           # + manual-commit support
      db.py                 # NEW — Postgres connect helper
    generator/
      __init__.py
      __main__.py           # import path updated
      generator.py          # imports updated, + live_stream
      scenarios.py          # imports updated
    pipeline/
      __init__.py
      consumer_helloworld.py  # imports updated
      embedding.py          # NEW — Embedder protocol, StubEmbedder, MiniLM, vec_literal
      normalizer.py         # NEW — raw.events -> normalized.events
      embedder.py           # NEW — normalized.events -> pgvector
    api/
      __init__.py           # NEW (empty)
      app.py                # NEW — POST /query
    eval/
      __init__.py           # NEW (empty)
      freshness.py          # NEW — percentile report
  tests/                    # MOVED from freshet/tests
    test_schema.py          # imports updated
    test_generator.py       # imports updated, + live_stream test
    test_embedding.py       # NEW
    test_normalizer.py      # NEW
    test_embedder.py        # NEW
    test_api.py             # NEW
    test_freshness.py       # NEW
    integration/
      test_db.py            # NEW — marked integration
      test_slice.py         # NEW — marked integration
```

Deleted: `freshet/requirements.txt`, `freshet/requirements-test.txt` (replaced by pyproject).

---

## Milestone 1 — Hygiene & packaging

### Task 1: Restructure into an installable package

**Files:**
- Create: `pyproject.toml`, `freshet/__init__.py`
- Move: `freshet/Makefile → Makefile`, `freshet/docker-compose.yml → docker-compose.yml`, `freshet/README.md → README.md`, `freshet/BRIEF.md → BRIEF.md`, `freshet/tests → tests`
- Modify: `freshet/generator/generator.py`, `freshet/generator/scenarios.py`, `freshet/generator/__main__.py`, `freshet/pipeline/consumer_helloworld.py`, `tests/test_schema.py`, `tests/test_generator.py`
- Delete: `freshet/requirements.txt`, `freshet/requirements-test.txt`

- [ ] **Step 1: Move files with git mv**

```bash
cd /Users/krasi/Documents/GitHub/freshet
git mv freshet/Makefile Makefile
git mv freshet/docker-compose.yml docker-compose.yml
git mv freshet/README.md README.md
git mv freshet/BRIEF.md BRIEF.md
git mv freshet/tests tests
git rm freshet/requirements.txt freshet/requirements-test.txt
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "freshet"
version = "0.1.0"
description = "Freshness-first streaming-RAG system for on-call engineers"
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2",
    "confluent-kafka>=2",
    "psycopg[binary]>=3",
    "fastapi>=0.110",
    "uvicorn>=0.29",
]

[project.optional-dependencies]
embed = ["sentence-transformers>=2"]
test = ["pytest>=8", "httpx>=0.27"]

[tool.setuptools.packages.find]
include = ["freshet*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "integration: requires the docker-compose stack (broker + Postgres)",
]
addopts = "-m 'not integration'"
```

Note: the `pgvector` Python package is deliberately NOT a dependency — vectors are written/queried as string literals cast with `::vector` (see Task 8), so only the SQL extension is needed.

- [ ] **Step 3: Create empty `freshet/__init__.py`**

```bash
touch freshet/__init__.py
```

- [ ] **Step 4: Update imports in `freshet/generator/scenarios.py`**

Change line 14:

```python
from freshet.common.schemas import Event, EventSource, EventType, Severity
```

(was `from common.schemas import ...`)

- [ ] **Step 5: Update imports in `freshet/generator/generator.py`**

Change lines 21–22:

```python
from freshet.common.schemas import Event, EventSource, EventType, Severity
from freshet.generator.scenarios import build_scenario
```

And the lazy import inside `KafkaSink.__init__` (line 119):

```python
from freshet.common.kafka_io import make_producer  # lazy import
```

- [ ] **Step 6: Update `freshet/generator/__main__.py`**

Full new content:

```python
from freshet.generator.generator import main

if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Update imports in `freshet/pipeline/consumer_helloworld.py`**

Change lines 17–18:

```python
from freshet.common.kafka_io import consume_loop
from freshet.common.schemas import Event
```

Also update the docstring's run example (line 9) to `python -m freshet.pipeline.consumer_helloworld --brokers localhost:9092`.

- [ ] **Step 8: Update imports in `tests/test_schema.py` and `tests/test_generator.py`**

`tests/test_schema.py` line 3:

```python
from freshet.common.schemas import Event, EventSource, Incident, Severity, VectorRecord
```

`tests/test_generator.py` lines 1–3:

```python
from freshet.common.schemas import Event, EventSource, EventType
from freshet.generator.generator import EventGenerator
from freshet.generator.scenarios import BAD_VERSION, GOOD_VERSION, SERVICE, build_scenario
```

- [ ] **Step 9: Install editable and run the test suite**

```bash
python3 -m venv .venv && source .venv/bin/activate   # or reuse an existing venv
pip install -e ".[test]"
pytest -q
```

Expected: `10 passed`. If imports fail, a stale `freshet/freshet` path or missed import is the cause — fix before proceeding.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "Restructure into installable freshet package with pyproject"
```

### Task 2: Update Makefile, CI, and README for the new layout

**Files:**
- Modify: `Makefile`, `.github/workflows/ci.yml`, `README.md`

- [ ] **Step 1: Rewrite `Makefile`**

Full new content (note: `PYTHONPATH` is gone; recipes use tabs):

```make
COMPOSE := docker compose
PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

.PHONY: up down smoke test

# Bring the stack up and block until both containers report healthy.
up:
	$(COMPOSE) up -d
	@echo "waiting for services to be healthy..."
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' freshet-redpanda 2>/dev/null)" = "healthy" ] \
		&& [ "$$(docker inspect -f '{{.State.Health.Status}}' freshet-postgres 2>/dev/null)" = "healthy" ]; do \
		i=$$((i+1)); \
		if [ $$i -ge 30 ]; then \
			echo "ERROR: stack did not become healthy after 60s"; \
			docker inspect -f '{{.Name}} -> {{.State.Health.Status}}' freshet-redpanda freshet-postgres; \
			exit 1; \
		fi; \
		sleep 2; echo "  ...still waiting ($$i/30)"; \
	done
	@echo "stack healthy."

# Tear down and drop the Postgres volume.
down:
	$(COMPOSE) down -v

# Run the unit tests (no broker needed; integration tests are excluded by pytest addopts).
test:
	$(PYTHON) -m pytest -q

# Produce -> consume -> validate against the real broker, and confirm Postgres.
# --count 60 emits 69 events total (60 noise + 9 scripted incident). A unique
# consumer group makes this re-runnable without tearing the stack down.
smoke:
	$(PYTHON) -m freshet.generator --sink kafka --brokers localhost:9092 --count 60
	$(PYTHON) -m freshet.pipeline.consumer_helloworld --brokers localhost:9092 --max 69 --group smoke-$$(date +%s)
	pg_isready -h localhost -p 5433
```

- [ ] **Step 2: Rewrite `.github/workflows/ci.yml`**

Full new content (the `working-directory: freshet` default is gone):

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install package and test deps
        run: pip install -e ".[test]"
      - name: Run unit tests
        run: pytest -q
```

- [ ] **Step 3: Update `README.md` run instructions**

Replace the "Run (Phase 0)" section's commands with:

```
    python3 -m venv .venv && source .venv/bin/activate
    pip install -e ".[test]"
    pytest -q
```

(no more `cd freshet` / `PYTHONPATH=.`), and the manual commands with:

```
    python -m freshet.generator --sink kafka --brokers localhost:9092 --count 60
    python -m freshet.pipeline.consumer_helloworld --brokers localhost:9092 --max 69
```

Update the Layout section paths (`freshet/common/`, `freshet/generator/`, `freshet/pipeline/`, `tests/`).

- [ ] **Step 4: Verify everything still works**

```bash
pytest -q                      # expected: 10 passed
make up                        # expected: "stack healthy."
make smoke                     # expected: 69 events printed, "consumed 69 events", pg_isready ok
make down
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Update Makefile, CI, and README for package layout"
```

### Task 3: LICENSE + untrack the local brief

**Files:**
- Create: `LICENSE`
- Untrack: `BRIEF_for_Claude_Code.md` (gitignored but still tracked)

- [ ] **Step 1: Create `LICENSE` (MIT)**

```
MIT License

Copyright (c) 2026 Krasimir Kirov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 2: Untrack the gitignored brief**

```bash
git rm --cached BRIEF_for_Claude_Code.md
```

Expected: `rm 'BRIEF_for_Claude_Code.md'` — the file stays on disk, leaves the index.

- [ ] **Step 3: Commit**

```bash
git add LICENSE
git commit -m "Add MIT license and untrack local brief"
```

---

## Milestone 2 — Vertical slice

### Task 4: Postgres schema + db helper

**Files:**
- Create: `db/init.sql`, `freshet/common/db.py`
- Modify: `docker-compose.yml`
- Test: `tests/integration/test_db.py`

- [ ] **Step 1: Create `db/init.sql`**

```sql
-- Freshet M2 schema. Idempotent: safe to apply repeatedly.
-- 384 dims = all-MiniLM-L6-v2 (and the stub embedder matches it).
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS vector_records (
    chunk_id    text PRIMARY KEY,
    event_id    text NOT NULL,
    incident_id text,
    service     text NOT NULL,
    ts          timestamptz NOT NULL,
    indexed_at  timestamptz NOT NULL,
    source      text NOT NULL,
    text        text NOT NULL,
    embedding   vector(384) NOT NULL
);

CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);
```

- [ ] **Step 2: Mount it in `docker-compose.yml`**

Add a `volumes` entry to the `postgres` service (it runs automatically only on a fresh volume; `make down` drops the volume, so `make down && make up` re-applies):

```yaml
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
```

- [ ] **Step 3: Add a `db-init` Makefile target** (applies to an already-running stack; add `db-init` to `.PHONY`)

```make
# Apply the schema to a running stack (idempotent).
db-init:
	docker exec -i freshet-postgres psql -U freshet -d freshet < db/init.sql
```

- [ ] **Step 4: Create `freshet/common/db.py`**

```python
"""Postgres connection helper. The compose stack publishes Postgres on host
port 5433 (5432 is left free for a local Postgres)."""

from __future__ import annotations

import os

import psycopg

DEFAULT_DSN = "postgresql://freshet:freshet@localhost:5433/freshet"


def connect(dsn: str | None = None) -> psycopg.Connection:
    return psycopg.connect(dsn or os.environ.get("FRESHET_DSN", DEFAULT_DSN), autocommit=True)
```

- [ ] **Step 5: Write the integration test `tests/integration/test_db.py`**

```python
import pytest

pytestmark = pytest.mark.integration


def test_schema_applied():
    from freshet.common.db import connect

    conn = connect()
    try:
        ext = conn.execute(
            "SELECT count(*) FROM pg_extension WHERE extname = 'vector'"
        ).fetchone()[0]
        assert ext == 1
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'vector_records'"
            ).fetchall()
        }
        assert {
            "chunk_id", "event_id", "incident_id", "service",
            "ts", "indexed_at", "source", "text", "embedding",
        } <= cols
    finally:
        conn.close()
```

- [ ] **Step 6: Verify**

```bash
pytest -q                          # expected: 10 passed (integration deselected by addopts)
make down && make up               # fresh volume -> init.sql auto-applies
pytest -q -m integration tests/integration/test_db.py
```

Expected: `1 passed`.

- [ ] **Step 7: Commit**

```bash
git add db/init.sql docker-compose.yml Makefile freshet/common/db.py tests/integration/test_db.py
git commit -m "Add pgvector schema, db helper, and db-init target"
```

### Task 5: Embedding interface (stub + MiniLM)

**Files:**
- Create: `freshet/pipeline/embedding.py`
- Test: `tests/test_embedding.py`

- [ ] **Step 1: Write the failing tests `tests/test_embedding.py`**

```python
import pytest

from freshet.pipeline.embedding import (
    EMBEDDING_DIM,
    StubEmbedder,
    make_embedder,
    vec_literal,
)


def test_stub_is_deterministic_and_distinct():
    e = StubEmbedder()
    [a1] = e.encode(["error spike on scheduler-api"])
    [a2] = e.encode(["error spike on scheduler-api"])
    [b] = e.encode(["routine deploy finished"])
    assert a1 == a2
    assert a1 != b
    assert len(a1) == EMBEDDING_DIM


def test_stub_vectors_are_unit_norm():
    [v] = StubEmbedder().encode(["x"])
    assert abs(sum(x * x for x in v) - 1.0) < 1e-6


def test_make_embedder():
    assert isinstance(make_embedder("stub"), StubEmbedder)
    with pytest.raises(ValueError):
        make_embedder("nope")


def test_vec_literal_format():
    assert vec_literal([1.0, -0.5]) == "[1.0,-0.5]"
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_embedding.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.pipeline.embedding'`

- [ ] **Step 3: Implement `freshet/pipeline/embedding.py`**

```python
"""Embedding backends behind one tiny interface.

StubEmbedder is deterministic and dependency-free so unit tests and CI never
download model weights. SentenceTransformerEmbedder is the real local default
(no API key). Both produce EMBEDDING_DIM-dimensional vectors — the
vector_records.embedding column is sized to match.
"""

from __future__ import annotations

import hashlib
import math
import random
from typing import Protocol

EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output size


class Embedder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...


class StubEmbedder:
    """Deterministic fake embeddings: same text -> same unit vector."""

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]

    @staticmethod
    def _vec(text: str) -> list[float]:
        seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
        rng = random.Random(seed)
        v = [rng.uniform(-1.0, 1.0) for _ in range(EMBEDDING_DIM)]
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]


class SentenceTransformerEmbedder:
    """Real local embeddings. Lazy import; first use downloads ~90 MB."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [
            [float(x) for x in row]
            for row in self.model.encode(texts, normalize_embeddings=True)
        ]


def make_embedder(kind: str) -> Embedder:
    if kind == "stub":
        return StubEmbedder()
    if kind == "minilm":
        return SentenceTransformerEmbedder()
    raise ValueError(f"unknown embedder: {kind!r} (expected 'stub' or 'minilm')")


def vec_literal(v: list[float]) -> str:
    """Format a vector as a pgvector text literal for use with %s::vector."""
    return "[" + ",".join(str(x) for x in v) + "]"
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_embedding.py`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add freshet/pipeline/embedding.py tests/test_embedding.py
git commit -m "Add embedding interface with stub and MiniLM backends"
```

### Task 6: Manual offset commits in kafka_io

**Files:**
- Modify: `freshet/common/kafka_io.py`

- [ ] **Step 1: Add `auto_commit` to `make_consumer` and `consume_loop`**

Full new content of `freshet/common/kafka_io.py`:

```python
"""Thin Kafka helpers. Isolated here so the rest of the codebase (and the tests)
don't import a Kafka client unless they actually talk to a broker.

Uses confluent-kafka, the standard Kafka client. The broker is provided by
docker-compose (Redpanda, which speaks the Kafka protocol). Delivery is
at-least-once; downstream upserts must be idempotent (keyed on chunk_id).
"""

from __future__ import annotations

from typing import Callable, Optional


def make_producer(brokers: str):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": brokers, "linger.ms": 5})


def make_consumer(brokers: str, group_id: str, topics: list[str], auto_commit: bool = True):
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": auto_commit,
        }
    )
    c.subscribe(topics)
    return c


def consume_loop(
    brokers: str,
    group_id: str,
    topics: list[str],
    handler: Callable[[str], None],
    max_messages: Optional[int] = None,
    auto_commit: bool = True,
) -> int:
    """Run a simple consume loop, calling handler(value_str) per message.

    With auto_commit=False the offset is committed only after the handler
    returns, so an unprocessed message is redelivered after a crash
    (at-least-once). Returns the number of messages processed. `max_messages`
    lets callers/tests bound the loop; None runs until interrupted.
    """
    c = make_consumer(brokers, group_id, topics, auto_commit=auto_commit)
    n = 0
    try:
        while max_messages is None or n < max_messages:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # in real code: route to dead-letter; here we just print
                print(f"[consume error] {msg.error()}")
                continue
            handler(msg.value().decode("utf-8"))
            if not auto_commit:
                c.commit(message=msg)
            n += 1
    finally:
        c.close()
    return n
```

- [ ] **Step 2: Run the unit suite (no broker logic is unit-testable here; the slice integration test in Task 12 covers it)**

Run: `pytest -q`
Expected: `14 passed` (10 original + 4 embedding)

- [ ] **Step 3: Commit**

```bash
git add freshet/common/kafka_io.py
git commit -m "Support manual offset commits in kafka_io"
```

### Task 7: Normalizer worker

**Files:**
- Create: `freshet/pipeline/normalizer.py`
- Test: `tests/test_normalizer.py`

- [ ] **Step 1: Write the failing tests `tests/test_normalizer.py`**

```python
from datetime import datetime, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.normalizer import normalize


def test_normalize_stamps_ingested_at_and_preserves_event():
    ev = Event(service="scheduler-api", source=EventSource.ALERT, type="error_spike", text="boom")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    out = normalize(ev.model_dump_json(), now=now)
    assert out is not None
    assert out.ingested_at == now
    assert out.event_id == ev.event_id
    assert out.ts == ev.ts
    assert out.text == "boom"


def test_normalize_rejects_invalid_payloads():
    assert normalize("not json at all") is None
    assert normalize('{"service": "s"}') is None  # missing required fields
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_normalizer.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.pipeline.normalizer'`

- [ ] **Step 3: Implement `freshet/pipeline/normalizer.py`**

```python
"""Normalizer worker: raw.events -> validate -> stamp ingested_at -> normalized.events.

M2 scope: validation + timestamping only. Incident correlation and the
dead-letter topic arrive in M4 — until then invalid payloads are skipped with
a warning, never silently dropped without trace.

Run (stack up first):
    python -m freshet.pipeline.normalizer --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event

RAW_TOPIC = "raw.events"
NORMALIZED_TOPIC = "normalized.events"


def normalize(value: str, now: Optional[datetime] = None) -> Optional[Event]:
    """Parse and validate one raw payload; stamp ingested_at. None if invalid."""
    try:
        ev = Event.model_validate_json(value)
    except Exception:
        return None
    ev.ingested_at = now or datetime.now(timezone.utc)
    return ev


def run(
    brokers: str,
    group: str = "normalizer",
    max_messages: Optional[int] = None,
    raw_topic: str = RAW_TOPIC,
    normalized_topic: str = NORMALIZED_TOPIC,
) -> int:
    from freshet.common.kafka_io import consume_loop, make_producer

    producer = make_producer(brokers)
    skipped = 0

    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            print(f"[normalizer] skipped invalid payload ({skipped} so far)")
            return
        # key by service to preserve per-service ordering downstream
        producer.produce(normalized_topic, key=ev.service, value=ev.model_dump_json())
        producer.poll(0)

    n = consume_loop(brokers, group, [raw_topic], handle, max_messages, auto_commit=False)
    producer.flush()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet normalizer (raw.events -> normalized.events)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="normalizer")
    p.add_argument("--max", type=int, default=None)
    a = p.parse_args()
    n = run(a.brokers, group=a.group, max_messages=a.max)
    print(f"[normalizer] processed {n} messages")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_normalizer.py`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add freshet/pipeline/normalizer.py tests/test_normalizer.py
git commit -m "Add normalizer worker"
```

### Task 8: Embedding worker with idempotent upserts

**Files:**
- Create: `freshet/pipeline/embedder.py`
- Test: `tests/test_embedder.py`

- [ ] **Step 1: Write the failing tests `tests/test_embedder.py`**

```python
from datetime import datetime, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import to_vector_record


def test_to_vector_record_has_deterministic_chunk_id():
    ev = Event(
        service="scheduler-api",
        source=EventSource.ALERT,
        type="error_spike",
        text="5xx spike",
        incident_id="INC-1",
    )
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    a = to_vector_record(ev, now=now)
    b = to_vector_record(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_to_vector_record_copies_fields_and_stamps_indexed_at():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    rec = to_vector_record(ev, now=now)
    assert rec.event_id == ev.event_id
    assert rec.service == "s"
    assert rec.ts == ev.ts
    assert rec.indexed_at == now
    assert rec.text == "hello"
    assert rec.source is EventSource.CHAT
    assert rec.incident_id is None
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_embedder.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.pipeline.embedder'`

- [ ] **Step 3: Implement `freshet/pipeline/embedder.py`**

```python
"""Embedding worker: normalized.events -> embed -> idempotent upsert into pgvector.

chunk_id derives from event_id, so redelivered or replayed events overwrite
their own row instead of duplicating (at-least-once + idempotent = effectively
once in the index). One chunk per event at M2; chunking for long texts (e.g.
postmortems) arrives in M4.

Run (stack up first; use --embedder stub to skip the model download):
    python -m freshet.pipeline.embedder --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event, VectorRecord
from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal
from freshet.pipeline.normalizer import NORMALIZED_TOPIC


def to_vector_record(ev: Event, now: Optional[datetime] = None) -> VectorRecord:
    return VectorRecord(
        chunk_id=f"chk_{ev.event_id}_0",
        event_id=ev.event_id,
        incident_id=ev.incident_id,
        service=ev.service,
        ts=ev.ts,
        indexed_at=now or datetime.now(timezone.utc),
        text=ev.text,
        source=ev.source,
    )


UPSERT_SQL = """
INSERT INTO vector_records
    (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
ON CONFLICT (chunk_id) DO UPDATE
    SET indexed_at = EXCLUDED.indexed_at,
        text = EXCLUDED.text,
        embedding = EXCLUDED.embedding
"""


def upsert_record(conn, rec: VectorRecord, embedding: list[float]) -> None:
    conn.execute(
        UPSERT_SQL,
        (
            rec.chunk_id,
            rec.event_id,
            rec.incident_id,
            rec.service,
            rec.ts,
            rec.indexed_at,
            rec.source.value,
            rec.text,
            vec_literal(embedding),
        ),
    )


def run(
    brokers: str,
    group: str = "embedder",
    max_messages: Optional[int] = None,
    topic: str = NORMALIZED_TOPIC,
    embedder: Optional[Embedder] = None,
    dsn: Optional[str] = None,
) -> int:
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop

    emb = embedder or make_embedder("minilm")
    conn = connect(dsn)

    def handle(value: str) -> None:
        ev = Event.model_validate_json(value)
        if not ev.text.strip():
            return
        rec = to_vector_record(ev)
        [vector] = emb.encode([rec.text])
        upsert_record(conn, rec, vector)

    try:
        n = consume_loop(brokers, group, [topic], handle, max_messages, auto_commit=False)
    finally:
        conn.close()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet embedding worker (normalized.events -> pgvector)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="embedder")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--embedder", choices=["minilm", "stub"], default="minilm")
    p.add_argument("--dsn", default=None)
    a = p.parse_args()
    n = run(a.brokers, group=a.group, max_messages=a.max, embedder=make_embedder(a.embedder), dsn=a.dsn)
    print(f"[embedder] processed {n} messages")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_embedder.py`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add freshet/pipeline/embedder.py tests/test_embedder.py
git commit -m "Add embedding worker with idempotent pgvector upserts"
```

### Task 9: Generator live mode

**Files:**
- Modify: `freshet/generator/generator.py`
- Test: `tests/test_generator.py`

- [ ] **Step 1: Write the failing test (append to `tests/test_generator.py`)**

```python
def test_live_stream_stamps_wall_clock_and_preserves_count():
    from freshet.generator.generator import live_stream

    gen = EventGenerator(seed=1)
    before = datetime.now(timezone.utc)
    events = list(live_stream(gen, count=5, spacing_s=0))
    after = datetime.now(timezone.utc)
    assert len(events) == 5 + 9  # noise + scripted incident
    assert all(before <= e.ts <= after for e in events)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_generator.py`
Expected: FAIL with `ImportError: cannot import name 'live_stream'`

- [ ] **Step 3: Implement `live_stream` in `freshet/generator/generator.py`**

Add `import time` to the imports, then add after the `EventGenerator` class:

```python
def live_stream(gen: EventGenerator, count: int, spacing_s: float) -> Iterator[Event]:
    """Re-stamp ts to wall-clock now and pace emission.

    The default stream uses fixed historical timestamps for reproducibility;
    freshness (indexed_at - ts) is only meaningful when ts is real time, so
    demos and the slice run use this wrapper.
    """
    for ev in gen.stream(count):
        ev.ts = datetime.now(timezone.utc)
        yield ev
        if spacing_s > 0:
            time.sleep(spacing_s)
```

- [ ] **Step 4: Wire `--live` into `main()`**

In `main()`, add the arguments:

```python
    p.add_argument("--live", action="store_true", help="stamp ts=now and pace emission (for freshness demos)")
    p.add_argument("--live-spacing", type=float, default=0.2, help="seconds between events in --live mode")
```

and replace the `for ev in gen.stream(args.count):` line so the stream is chosen by mode:

```python
    stream = live_stream(gen, args.count, args.live_spacing) if args.live else gen.stream(args.count)
    n = 0
    try:
        for ev in stream:
            sink.write(ev)
            n += 1
```

- [ ] **Step 5: Run tests**

Run: `pytest -q`
Expected: `19 passed` (all suites so far)

- [ ] **Step 6: Commit**

```bash
git add freshet/generator/generator.py tests/test_generator.py
git commit -m "Add live mode to the generator for real-time freshness"
```

### Task 10: Query API (vector-only top-k)

**Files:**
- Create: `freshet/api/__init__.py` (empty), `freshet/api/app.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write the failing tests `tests/test_api.py`**

```python
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from freshet.api.app import app, get_deps, topk_sql
from freshet.pipeline.embedding import StubEmbedder


def test_topk_sql_filters():
    now = datetime.now(timezone.utc)
    base = topk_sql(None, None)
    assert "WHERE" not in base
    assert "ORDER BY embedding <=>" in base
    assert "service = %(service)s" in topk_sql("scheduler-api", None)
    assert "ts >= %(since)s" in topk_sql(None, now)
    both = topk_sql("s", now)
    assert "service = %(service)s" in both and "ts >= %(since)s" in both


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        return FakeCursor(self.rows)


def test_query_endpoint_returns_scored_hits():
    now = datetime.now(timezone.utc)
    rows = [("chk_evt1_0", "evt1", "scheduler-api", now, now, "alert", "5xx spike", 0.93)]
    fake = FakeConn(rows)
    app.dependency_overrides[get_deps] = lambda: (fake, StubEmbedder())
    try:
        client = TestClient(app)
        resp = client.post("/query", json={"question": "what is wrong?", "k": 3})
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 200
    hits = resp.json()["hits"]
    assert len(hits) == 1
    assert hits[0]["event_id"] == "evt1"
    assert hits[0]["score"] == 0.93
    # the SQL actually ran with the embedded question vector
    sql, params = fake.queries[0]
    assert params["k"] == 3
    assert params["qvec"].startswith("[")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_api.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.api'`

- [ ] **Step 3: Implement `freshet/api/app.py`** (and `touch freshet/api/__init__.py`)

```python
"""M2 query API: vector-only top-k over vector_records.

Deliberately minimal — hybrid retrieval, recency weighting, abstention, and
answer composition arrive in M5. Exists so the vertical slice is provable
end to end.

Run:
    uvicorn freshet.api.app:app --port 8000
Config via env: FRESHET_DSN, FRESHET_EMBEDDER (minilm|stub).
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from fastapi import Depends, FastAPI
from pydantic import BaseModel, Field

from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal


class QueryRequest(BaseModel):
    question: str
    k: int = Field(default=5, ge=1, le=50)
    service: Optional[str] = None
    since: Optional[datetime] = None


class Hit(BaseModel):
    chunk_id: str
    event_id: str
    service: str
    ts: datetime
    indexed_at: datetime
    source: str
    text: str
    score: float


class QueryResponse(BaseModel):
    hits: list[Hit]


def topk_sql(service: Optional[str], since: Optional[datetime]) -> str:
    where = []
    if service is not None:
        where.append("service = %(service)s")
    if since is not None:
        where.append("ts >= %(since)s")
    where_clause = (" WHERE " + " AND ".join(where)) if where else ""
    return (
        "SELECT chunk_id, event_id, service, ts, indexed_at, source, text,"
        " 1 - (embedding <=> %(qvec)s::vector) AS score"
        " FROM vector_records" + where_clause +
        " ORDER BY embedding <=> %(qvec)s::vector LIMIT %(k)s"
    )


def search(conn, embedder: Embedder, req: QueryRequest) -> list[Hit]:
    [qvec] = embedder.encode([req.question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "k": req.k}
    if req.service is not None:
        params["service"] = req.service
    if req.since is not None:
        params["since"] = req.since
    rows = conn.execute(topk_sql(req.service, req.since), params).fetchall()
    return [
        Hit(
            chunk_id=r[0], event_id=r[1], service=r[2], ts=r[3],
            indexed_at=r[4], source=r[5], text=r[6], score=float(r[7]),
        )
        for r in rows
    ]


_conn = None
_embedder: Optional[Embedder] = None


def get_deps():
    global _conn, _embedder
    if _embedder is None:
        _embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "minilm"))
    if _conn is None:
        from freshet.common.db import connect

        _conn = connect()
    return _conn, _embedder


app = FastAPI(title="Freshet query API")


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest, deps=Depends(get_deps)) -> QueryResponse:
    conn, embedder = deps
    return QueryResponse(hits=search(conn, embedder, req))
```

- [ ] **Step 4: Run tests**

Run: `pytest -q tests/test_api.py`
Expected: `2 passed`

- [ ] **Step 5: Add an `api` Makefile target** (add `api` to `.PHONY`)

```make
# Serve the query API on :8000 (stack must be up; FRESHET_EMBEDDER=stub to skip model).
api:
	$(PYTHON) -m uvicorn freshet.api.app:app --port 8000
```

- [ ] **Step 6: Commit**

```bash
git add freshet/api tests/test_api.py Makefile
git commit -m "Add vector-only query API"
```

### Task 11: Freshness report

**Files:**
- Create: `freshet/eval/__init__.py` (empty), `freshet/eval/freshness.py`
- Test: `tests/test_freshness.py`

- [ ] **Step 1: Write the failing tests `tests/test_freshness.py`**

```python
import pytest

from freshet.eval.freshness import freshness_report, percentile


def test_percentile_nearest_rank():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    assert percentile(vals, 50) == 5.0
    assert percentile(vals, 95) == 10.0
    assert percentile(vals, 99) == 10.0
    assert percentile([42.0], 50) == 42.0


def test_freshness_report_shape():
    rep = freshness_report([2.5, 0.5, 1.5])
    assert rep["count"] == 3
    assert rep["p50_s"] == 1.5
    assert rep["p95_s"] == 2.5


def test_freshness_report_empty_raises():
    with pytest.raises(ValueError):
        freshness_report([])
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest -q tests/test_freshness.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'freshet.eval'`

- [ ] **Step 3: Implement `freshet/eval/freshness.py`** (and `touch freshet/eval/__init__.py`)

```python
"""Freshness report: percentiles of event->queryable latency (indexed_at - ts)
read from vector_records. This is the project's headline metric; the full eval
harness (M6) builds on the same numbers.

Run (after the slice has indexed events):
    python -m freshet.eval.freshness
"""

from __future__ import annotations

import math


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile. `values` need not be pre-sorted."""
    if not values:
        raise ValueError("no values")
    vals = sorted(values)
    k = max(0, min(len(vals) - 1, math.ceil(p / 100 * len(vals)) - 1))
    return vals[k]


def freshness_report(latencies_s: list[float]) -> dict[str, float]:
    return {
        "count": len(latencies_s),
        "p50_s": percentile(latencies_s, 50),
        "p95_s": percentile(latencies_s, 95),
        "p99_s": percentile(latencies_s, 99),
    }


def main() -> None:
    from freshet.common.db import connect

    conn = connect()
    try:
        rows = conn.execute(
            "SELECT EXTRACT(EPOCH FROM (indexed_at - ts))::float8 FROM vector_records"
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        print("no records in vector_records — run the slice first (make slice)")
        return
    rep = freshness_report([r[0] for r in rows])
    print(
        f"event->queryable freshness over {rep['count']} records:"
        f"  p50={rep['p50_s']:.2f}s  p95={rep['p95_s']:.2f}s  p99={rep['p99_s']:.2f}s"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `pytest -q`
Expected: `24 passed`

- [ ] **Step 5: Commit**

```bash
git add freshet/eval tests/test_freshness.py
git commit -m "Add freshness report"
```

### Task 12: End-to-end slice integration test

**Files:**
- Create: `tests/integration/test_slice.py`
- Modify: `Makefile` (add `test-integration`)

- [ ] **Step 1: Write `tests/integration/test_slice.py`**

```python
"""End-to-end vertical-slice test: generator -> Kafka -> normalizer ->
embedder (stub) -> pgvector -> search + freshness. Requires the compose stack
(make up) with the schema applied. Run via: make test-integration.

Uses run-unique topics/groups so it is isolated from prior stack activity,
and clears vector_records (the dev stack's table) for deterministic counts.
"""

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
    yield c
    c.close()


def test_slice_end_to_end(conn):
    from freshet.api.app import QueryRequest, search
    from freshet.eval.freshness import freshness_report
    from freshet.generator.generator import EventGenerator, KafkaSink, live_stream
    from freshet.pipeline import embedder, normalizer
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.events.it{run_id}"
    norm_topic = f"normalized.events.it{run_id}"

    # produce 20 noise + 9 scripted = 29 live-stamped events
    sink = KafkaSink(BROKERS, raw_topic)
    produced = 0
    for ev in live_stream(EventGenerator(seed=3), count=20, spacing_s=0):
        sink.write(ev)
        produced += 1
    sink.close()
    assert produced == 29

    n = normalizer.run(
        BROKERS, group=f"norm-{run_id}", max_messages=29,
        raw_topic=raw_topic, normalized_topic=norm_topic,
    )
    assert n == 29

    n = embedder.run(
        BROKERS, group=f"emb-{run_id}", max_messages=29,
        topic=norm_topic, embedder=StubEmbedder(),
    )
    assert n == 29

    total, distinct = conn.execute(
        "SELECT count(*), count(DISTINCT event_id) FROM vector_records"
    ).fetchone()
    assert total == 29 and distinct == 29

    # idempotency: a fresh group re-reads the topic; row count must not change
    n = embedder.run(
        BROKERS, group=f"emb2-{run_id}", max_messages=29,
        topic=norm_topic, embedder=StubEmbedder(),
    )
    assert n == 29
    assert conn.execute("SELECT count(*) FROM vector_records").fetchone()[0] == 29

    # all three timestamps flowed through; freshness is small and non-negative
    lats = [
        r[0]
        for r in conn.execute(
            "SELECT EXTRACT(EPOCH FROM (indexed_at - ts))::float8 FROM vector_records"
        ).fetchall()
    ]
    rep = freshness_report(lats)
    assert rep["count"] == 29
    assert 0 <= rep["p50_s"] < 120

    # query path: scored, timestamped hits in descending score order
    hits = search(conn, StubEmbedder(), QueryRequest(question="error spike on scheduler-api", k=5))
    assert len(hits) == 5
    scores = [h.score for h in hits]
    assert scores == sorted(scores, reverse=True)

    # metadata filter narrows to the incident service
    hits = search(conn, StubEmbedder(), QueryRequest(question="rollback", k=5, service="scheduler-api"))
    assert all(h.service == "scheduler-api" for h in hits)
```

- [ ] **Step 2: Add `test-integration` to the Makefile** (add to `.PHONY`)

```make
# Integration tests against the running stack (make up first).
test-integration:
	$(PYTHON) -m pytest -q -m integration
```

- [ ] **Step 3: Run it against the stack**

```bash
make up          # fresh stack auto-applies db/init.sql; otherwise: make db-init
make test-integration
```

Expected: `2 passed` (test_db + test_slice). Also verify unit run still excludes them: `pytest -q` → `24 passed`.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_slice.py Makefile
git commit -m "Add end-to-end vertical slice integration test"
```

### Task 13: Slice demo script + README + final verification

**Files:**
- Create: `scripts/run_slice.sh`
- Modify: `Makefile` (add `slice`), `README.md`

- [ ] **Step 1: Create `scripts/run_slice.sh`** (then `chmod +x scripts/run_slice.sh`)

```bash
#!/usr/bin/env bash
# Vertical-slice demo: generator -> raw.events -> normalizer -> normalized.events
# -> embedder -> pgvector, then a freshness report and one example query.
#
# Assumes the stack is up (make up). For a clean freshness demo run it on a
# fresh stack (make down && make up): workers use stable consumer groups, so
# they also drain any events left on the topics by earlier runs, and old
# non-live events carry fake-historical ts values that pollute the report.
set -euo pipefail

COUNT="${COUNT:-60}"            # noise events; total emitted = COUNT + 9 scripted
SPACING="${SPACING:-0.1}"       # seconds between events in live mode
EMBEDDER="${EMBEDDER:-minilm}"  # EMBEDDER=stub skips the model download
BROKERS="${BROKERS:-localhost:9092}"
TOTAL=$((COUNT + 9))
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

"${PSQL[@]}" < db/init.sql > /dev/null

BEFORE=$("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records")
TARGET=$((BEFORE + TOTAL))

python3 -m freshet.pipeline.normalizer --brokers "$BROKERS" &
NORM_PID=$!
python3 -m freshet.pipeline.embedder --brokers "$BROKERS" --embedder "$EMBEDDER" &
EMB_PID=$!
trap 'kill $NORM_PID $EMB_PID 2>/dev/null || true' EXIT

python3 -m freshet.generator --sink kafka --brokers "$BROKERS" --count "$COUNT" --live --live-spacing "$SPACING"

echo "waiting for $TOTAL events to become queryable..."
i=0
until [ "$("${PSQL[@]}" -tAc 'SELECT count(*) FROM vector_records')" -ge "$TARGET" ]; do
  i=$((i+1))
  if [ "$i" -ge 120 ]; then
    echo "ERROR: pipeline did not index $TOTAL events within 120s"
    exit 1
  fi
  sleep 1
done

python3 -m freshet.eval.freshness

echo
echo "example query: 'what is happening with scheduler-api?'"
EMBEDDER="$EMBEDDER" python3 - <<'EOF'
import os

from freshet.api.app import QueryRequest, search
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder

conn = connect()
hits = search(
    conn,
    make_embedder(os.environ["EMBEDDER"]),
    QueryRequest(question="what is happening with scheduler-api?", k=3),
)
for h in hits:
    print(f"  {h.score:.3f}  [{h.source}] {h.ts:%H:%M:%S} -> indexed {h.indexed_at:%H:%M:%S}  {h.text[:70]}")
conn.close()
EOF
```

- [ ] **Step 2: Add a `slice` Makefile target** (add to `.PHONY`)

```make
# Run the vertical-slice demo end to end (make up first; EMBEDDER=stub to skip model).
slice:
	bash scripts/run_slice.sh
```

- [ ] **Step 3: Update `README.md`**

Retitle to "Freshet — Real-Time Incident Intelligence (M2: vertical slice)". Replace the body so it documents: unit tests (`pip install -e ".[test]"`, `pytest -q`), full stack (`make up` / `make down`), the slice demo (`pip install -e ".[embed]"` for MiniLM, then `make slice`, or `EMBEDDER=stub make slice` for no model download), the query API (`make api`, example `curl -s localhost:8000/query -X POST -H 'content-type: application/json' -d '{"question": "what is happening with scheduler-api?", "k": 3}'`), integration tests (`make test-integration`), and the updated layout (`freshet/{common,generator,pipeline,api,eval}`, `db/`, `scripts/`, `tests/`). Keep the pointer to `BRIEF.md` and the note that Kafka is on 9092, Postgres on 5433.

- [ ] **Step 4: Final verification — the real thing, twice**

```bash
make down && make up
pip install -e ".[embed]"        # one-time MiniLM install
make slice                       # real embedder
```

Expected output ends with a freshness line like `event->queryable freshness over 69 records: p50=0.xx s ...` (p50 should be low single-digit seconds or below) and three scored hits mentioning scheduler-api. Then re-run `make slice` — it must complete again (stable groups only process the new events; counts use the BEFORE baseline). Finally:

```bash
pytest -q                # 24 passed
make test-integration    # 2 passed
```

- [ ] **Step 5: Commit**

```bash
git add scripts/run_slice.sh Makefile README.md
git commit -m "Add slice demo script and update README for M2"
```

---

## Definition of done (M1 + M2, from the spec)

- [ ] `pip install -e .` works; tests are green; `make up && make smoke` passes (M1)
- [ ] LICENSE exists; `BRIEF_for_Claude_Code.md` untracked (M1)
- [ ] One make target (`make slice`) runs generator → Kafka → normalizer → embedder → pgvector (M2)
- [ ] A query returns relevant events carrying all three timestamps (M2)
- [ ] The freshness report prints real p50/p95/p99 percentiles (M2)
