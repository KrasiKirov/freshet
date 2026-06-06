# Phase 0 Infra Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out Beacon Phase 0 by adding a `docker-compose` stack (Redpanda + Postgres/pgvector), a thin Makefile, CI, and a verified produce→consume→validate run against a real broker.

**Architecture:** Two containers (single-node Redpanda on host port 9092; `pgvector/pgvector:pg16` Postgres on host port 5433→5432, container-only, no schema). A Makefile gives one-command bring-up and a smoke target. The existing host-run generator and hello-world consumer prove the round-trip through the real broker. CI runs the existing unit tests (which use the JSONL sink and need no broker).

**Tech Stack:** Docker Compose, Redpanda, Postgres + pgvector, Python 3.12/3.13, confluent-kafka, pydantic, GitHub Actions, pytest, Make.

---

## Context the engineer needs

- Repo root: `/Users/krasi/Documents/GitHub/RagKafka`. Application code lives in `beacon/`.
- Git is already initialized (identity: `KrasiKirov <krasimir.kirov@mail.mcgill.ca>`). `.gitignore` already exists and ignores `.venv/`, `__pycache__/`, `.pytest_cache/`, `events.jsonl`.
- **All commits in this repo are title-only** — a single `-m "<title>"`, no body, no `Co-Authored-By` trailers.
- The dev machine already runs a local Postgres on host port **5432**, so the container MUST publish on **5433**. Host port 9092 is free.
- Docker 29.5 / Compose v5.1 confirmed available. Python 3.13 is the local interpreter; CI uses 3.12.
- The generator (`beacon/generator`) and consumer (`beacon/pipeline/consumer_helloworld.py`) are run from the host against `localhost:9092`. Redpanda auto-creates `raw.events` on first produce.
- Run all `beacon`-relative commands from inside `beacon/` with `PYTHONPATH=.`.

## File Structure

- **Create** `beacon/docker-compose.yml` — the two-service local stack (Redpanda, Postgres).
- **Create** `beacon/Makefile` — `up` / `down` / `smoke` / `test` targets.
- **Create** `.github/workflows/ci.yml` — run `pytest` on push/PR (no broker needed).
- **Modify** `beacon/README.md` — Phase 0 run section reflects the real compose stack, make targets, and the 5433 port.

---

## Task 1: Local Python environment (verification prerequisite)

**Files:** none (environment setup).

- [ ] **Step 1: Create and activate a virtualenv**

Run (from repo root):
```bash
cd beacon
python3 -m venv .venv
source .venv/bin/activate
python --version
```
Expected: a Python 3.x version prints; prompt shows `(.venv)`.

- [ ] **Step 2: Install dependencies**

Run:
```bash
pip install -r requirements.txt
```
Expected: completes successfully, including `confluent-kafka`. Note: `sentence-transformers` pulls in `torch` (large, slow) — this is expected for the full requirements file. If the full install fails on this Python version, the Phase 0 verification only needs `confluent-kafka pydantic pytest`; install those and proceed, noting the deviation.

- [ ] **Step 3: Confirm the existing unit tests pass**

Run:
```bash
PYTHONPATH=. pytest -q
```
Expected: `10 passed`.

---

## Task 2: docker-compose.yml

**Files:**
- Create: `beacon/docker-compose.yml`

- [ ] **Step 1: Write the compose file**

```yaml
# Beacon Phase 0 local stack: Redpanda (Kafka API) + Postgres/pgvector.
# Container-only Postgres at this phase — no schema, no extension creation
# (that is Phase 1's contract). Postgres is published on host port 5433 to
# avoid colliding with a local Postgres on 5432.
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    container_name: beacon-redpanda
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=1G
      - --overprovisioned
      - --node-id=0
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://localhost:9092
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      timeout: 5s
      retries: 12

  postgres:
    image: pgvector/pgvector:pg16
    container_name: beacon-postgres
    environment:
      POSTGRES_USER: beacon
      POSTGRES_PASSWORD: beacon
      POSTGRES_DB: beacon
    ports:
      - "5433:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U beacon"]
      interval: 5s
      timeout: 5s
      retries: 12

volumes:
  pgdata:
```

- [ ] **Step 2: Validate the compose file parses**

Run (from `beacon/`):
```bash
docker compose config >/dev/null && echo OK
```
Expected: prints `OK` (no YAML/schema errors).

- [ ] **Step 3: Commit**

```bash
cd /Users/krasi/Documents/GitHub/RagKafka
git add beacon/docker-compose.yml
git commit -m "Add Phase 0 docker-compose stack (Redpanda + pgvector)"
```

---

## Task 3: Makefile

**Files:**
- Create: `beacon/Makefile`

- [ ] **Step 1: Write the Makefile**

Note: Make requires real tab indentation in recipe lines.
```makefile
COMPOSE := docker compose
PY := PYTHONPATH=. python

.PHONY: up down smoke test

# Bring the stack up and block until both containers report healthy.
up:
	$(COMPOSE) up -d
	@echo "waiting for services to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-redpanda 2>/dev/null)" = "healthy" ] \
		&& [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-postgres 2>/dev/null)" = "healthy" ]; do \
		sleep 2; echo "  ...still waiting"; \
	done
	@echo "stack healthy."

# Tear down and drop the Postgres volume.
down:
	$(COMPOSE) down -v

# Run the unit tests (no broker needed; uses the JSONL sink).
test:
	$(PY) -m pytest -q

# Produce -> consume -> validate against the real broker, and confirm Postgres.
smoke:
	$(PY) -m generator --sink kafka --brokers localhost:9092 --count 60
	$(PY) -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69
	pg_isready -h localhost -p 5433
```

- [ ] **Step 2: Sanity-check the Makefile parses**

Run (from `beacon/`):
```bash
make -n up
```
Expected: prints the `up` recipe commands without executing them; no "missing separator" errors.

- [ ] **Step 3: Commit**

```bash
cd /Users/krasi/Documents/GitHub/RagKafka
git add beacon/Makefile
git commit -m "Add Makefile with up/down/smoke/test targets"
```

---

## Task 4: Bring up the stack and verify health

**Files:** none (live verification).

- [ ] **Step 1: Bring the stack up**

Run (from `beacon/`):
```bash
make up
```
Expected: ends with `stack healthy.` If the Redpanda image tag fails to pull, switch to a current stable `v24.2.x` tag and retry.

- [ ] **Step 2: Confirm both containers are healthy**

Run:
```bash
docker compose ps
```
Expected: `beacon-redpanda` and `beacon-postgres` both show `(healthy)`.

- [ ] **Step 3: Confirm Postgres is reachable on 5433**

Run:
```bash
pg_isready -h localhost -p 5433
```
Expected: `localhost:5433 - accepting connections`. (If `pg_isready` is not on PATH, use `docker exec beacon-postgres pg_isready -U beacon`.)

---

## Task 5: Smoke test — produce → consume → validate (the Phase 0 proof)

**Files:** none (live verification; capture output as evidence).

- [ ] **Step 1: Produce events to the real broker**

Run (from `beacon/`, venv active):
```bash
PYTHONPATH=. python -m generator --sink kafka --brokers localhost:9092 --count 60
```
Expected: `wrote 69 events via kafka sink` (60 noise + 9 scripted incident events).

- [ ] **Step 2: Consume and validate from the real broker**

Run:
```bash
PYTHONPATH=. python -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69
```
Expected: 69 validated `Event` lines print, ending with `consumed 69 events`. Among them, lines with `incident=INC-DEMO-0001` showing the scripted beats: `deploy_started`, `error_spike`, `rollback`, `rca`. This proves produce→Kafka→consume→validate through a real broker.

- [ ] **Step 3: Record the evidence**

Save the tail of the consumer output (the incident lines + the `consumed 69 events` line) to paste into the task summary. This is the proof artifact the spec's "definition of done" requires.

- [ ] **Step 4: Tear the stack down to confirm clean lifecycle**

Run (from `beacon/`):
```bash
make down
```
Expected: containers stop and are removed; `pgdata` volume removed. (Optional — only to confirm `down` works; bring back `up` if continuing.)

---

## Task 6: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: beacon
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest -q
        env:
          PYTHONPATH: .
```

- [ ] **Step 2: Validate the YAML parses**

Run (from repo root):
```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('OK')"
```
Expected: prints `OK`. (If PyYAML isn't installed in the active env, run inside the venv from Task 1, or `pip install pyyaml` first.)

- [ ] **Step 3: Commit**

```bash
cd /Users/krasi/Documents/GitHub/RagKafka
git add .github/workflows/ci.yml
git commit -m "Add GitHub Actions CI running pytest"
```

Note: per the spec, do NOT push or configure a remote — the user does that.

---

## Task 7: Update the README

**Files:**
- Modify: `beacon/README.md`

- [ ] **Step 1: Replace the "Run (Phase 0)" section**

Replace the existing run instructions (lines that show `pip install` + the broker block) with the block below. Keep the intro paragraph and the Layout section intact.

````markdown
## Run (Phase 0)

Unit tests (no broker needed — uses the JSONL sink):

    cd beacon
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    PYTHONPATH=. pytest -q

Full stack (Redpanda + Postgres/pgvector via docker-compose):

    make up      # starts the broker + Postgres, waits until healthy
    make smoke   # produce -> consume -> validate against the real broker
    make down    # stop and remove the stack

Notes:
- Kafka API is on `localhost:9092`. Postgres is on `localhost:5433`
  (5432 is left free for a local Postgres), db/user/password `beacon`.
- Postgres is container-only at Phase 0 (no schema/extension yet — that is
  Phase 1). `make smoke` only exercises the Kafka round-trip.

Equivalent manual commands:

    PYTHONPATH=. python -m generator --sink kafka --brokers localhost:9092 --count 60
    PYTHONPATH=. python -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69
````

- [ ] **Step 2: Verify the README reads correctly**

Run (from repo root):
```bash
grep -n "5433" beacon/README.md && grep -n "make up" beacon/README.md
```
Expected: both matches found.

- [ ] **Step 3: Commit**

```bash
cd /Users/krasi/Documents/GitHub/RagKafka
git add beacon/README.md
git commit -m "Update README for Phase 0 docker-compose stack"
```

---

## Task 8: Final verification

**Files:** none.

- [ ] **Step 1: Confirm unit tests still pass**

Run (from `beacon/`, venv active):
```bash
PYTHONPATH=. pytest -q
```
Expected: `10 passed`.

- [ ] **Step 2: Confirm clean git state**

Run (from repo root):
```bash
git status --short && git log --oneline -6
```
Expected: working tree clean; recent commits show the compose, Makefile, CI, and README additions, each with a title-only message.

- [ ] **Step 3: Definition-of-done recap**

Confirm against the spec: `make up` brings the stack healthy; the smoke run showed validated events (incl. `INC-DEMO-0001`) consumed from a real Redpanda broker (evidence captured in Task 5); `pytest` green; CI file present and YAML-valid; repo not pushed; README accurate. Report any item that did not hold.

---

## Out of scope (Phase 1, not this plan)

Normalizer consumer, embedding worker, pgvector schema/tables/extension creation, dead-letter handling, lag/latency metrics.
