# Freshet — Real-Time Incident Intelligence (Phase 0)

Freshness-first streaming-RAG system for on-call engineers. See `BRIEF.md` for the
full what/why/how and build order. This repo currently contains **Phase 0**: the
data contract, a deterministic synthetic event generator, Kafka I/O helpers, a
produce→consume hello-world, and tests.

## Run (Phase 0)

Unit tests (no broker needed):

    cd freshet
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    PYTHONPATH=. pytest -q

Full stack (Redpanda + Postgres/pgvector via docker-compose):

    make up      # starts the broker + Postgres, waits until healthy
    make smoke   # produce -> consume -> validate against the real broker
    make down    # stop and remove the stack

Notes:
- Kafka API is on `localhost:9092`. Postgres is on `localhost:5433`
  (5432 is left free for a local Postgres), db/user/password `freshet`.
- Postgres is container-only at Phase 0 (no schema/extension yet — that is
  Phase 1). `make smoke` only exercises the Kafka round-trip.

Equivalent manual commands:

    PYTHONPATH=. python -m generator --sink kafka --brokers localhost:9092 --count 60
    PYTHONPATH=. python -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69

## Layout
    common/      # schemas (the contract) + kafka helpers
    generator/   # synthetic events + scripted incident scenario
    pipeline/    # consumers (Phase 0: hello-world; Phase 1+: normalizer, embedder)
    tests/       # schema + generator tests
