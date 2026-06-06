# Phase 0 Infra — Design

*Status: approved 2026-06-06. Scope: close out Phase 0 of the Freshet brief — local
infrastructure (`docker-compose`), a verified produce→consume→validate run against a
real broker, and CI. No pipeline logic (normalizer/embedder/schema) — that is Phase 1.*

## Context

The Freshet repo (see `freshet/`, `BRIEF_for_Claude_Code.md`) is at Phase 0: data
contract, deterministic synthetic generator, Kafka I/O helpers, a hello-world
consumer, and 10 passing unit tests. The README references a `docker-compose` stack
and CI that **do not exist yet**, so Phase 0's stated "done" criteria are not met:

> *Done when:* `docker-compose up` starts the broker + Postgres; the generator
> produces to Kafka and the hello-world consumer prints validated events; `pytest`
> is green and the generator is byte-reproducible under a fixed seed.

This increment delivers exactly that and nothing more.

## Decisions (settled during brainstorming)

- **Broker:** Redpanda, single container (the brief's primary recommendation;
  Kafka-API compatible, one container, auto-creates topics).
- **Postgres depth:** container only — `pgvector/pgvector` image, **no** `init.sql`,
  no extension creation, no tables. Schema is Phase 1's contract; we do not get ahead
  of it.
- **Postgres host port:** **5433** (host) → 5432 (container). The developer machine
  already runs a local Postgres on 5432; publishing on 5433 avoids the conflict.
- **Verification:** run and prove it in this environment (Docker 29.5 / Compose v5.1
  confirmed available; port 9092 free).
- **CI:** `git init` at the repo root and author `.github/workflows/ci.yml`, but do
  **not** push or configure a remote — that is left to the user.
- **Orchestration:** a thin `Makefile` provides the "one command brings up the stack"
  property the brief calls for.

## Components

### 1. `freshet/docker-compose.yml`

Two services.

- **`redpanda`** — `redpandadata/redpanda` in KRaft single-node mode. Kafka API
  published on host `9092`. Lightweight flags (`--smp 1`, `--memory 1G`,
  `--overprovisioned`). Healthcheck via `rpk cluster health`. Topic auto-creation
  left enabled so `raw.events` is created on first produce, matching the existing
  hello-world flow.
- **`postgres`** — `pgvector/pgvector:pg16`. Host port `5433`→`5432`. Named volume
  `pgdata` for persistence. `pg_isready` healthcheck. Default DB/user/password set
  via env (e.g. `freshet`/`freshet`/`freshet`) for local use only.

No application services in compose at this phase (the generator/consumer run from the
host venv during verification).

### 2. `freshet/Makefile`

Thin targets for reproducibility:

- `up` — `docker compose up -d` and wait until both healthchecks are healthy.
- `down` — `docker compose down -v` (also drops the volume).
- `smoke` — the verification sequence below (produce → consume → validate).
- `test` — `PYTHONPATH=. pytest -q`.

### 3. CI — `.github/workflows/ci.yml`

On push/PR: set up Python 3.12, `pip install -r freshet/requirements.txt`, run
`pytest -q` with `working-directory: freshet` and `PYTHONPATH: .`. The unit tests use
the JSONL sink, so **CI needs no broker or database**.

Supporting: `.gitignore` (venv, `__pycache__`, `.pytest_cache`, `events.jsonl`,
`*.egg-info`).

### 4. README update

Fix the Phase 0 run section to reference the real `docker-compose.yml`, the `make`
targets, and the `5433` Postgres host port. No overclaiming.

## Verification (the produce→consume→validate proof)

Run in this environment and capture real output as the proof artifact:

1. Create/activate a venv; `pip install -r freshet/requirements.txt` (proves
   `confluent-kafka` installs on this machine / Python).
2. `make up`; wait for both healthchecks green.
3. `python -m generator --sink kafka --brokers localhost:9092 --count 60` — produces
   60 noise events plus the scripted incident.
4. `python -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69` —
   prints validated `Event` lines including the `INC-DEMO-0001` incident beats,
   confirming the round-trip through a real broker.
5. `pg_isready -h localhost -p 5433` — Postgres reachable.

## Definition of done

- `make up` brings the stack healthy.
- The smoke run shows validated events (including the scripted incident) consumed
  from a real Redpanda broker — captured as evidence.
- `pytest` green (10 tests).
- `.github/workflows/ci.yml` present and YAML-valid; repo initialized; not pushed.
- README accurate to the new infra (ports, commands).

## Out of scope (deferred to Phase 1)

Normalizer consumer, embedding worker, pgvector schema/tables/extension, dead-letter
topic handling, lag/latency metrics. Stated here so the boundary is explicit.
