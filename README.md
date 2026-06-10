# Freshet — Real-Time Incident Intelligence

Freshness-first streaming-RAG system for on-call engineers. See `BRIEF.md` for the
full what/why/how and build order. This repo currently contains the foundation
(data contract, deterministic synthetic event generator, Kafka I/O helpers,
produce→consume hello-world, tests) plus the in-progress ingestion slice:
pgvector schema, embedding interface, and the normalizer worker. The
end-to-end pipeline is not wired up yet.

## Run

Unit tests (no broker needed):

    python3 -m venv .venv && source .venv/bin/activate
    pip install -e ".[test]"
    pytest -q

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

    python -m freshet.generator --sink kafka --brokers localhost:9092 --count 60
    python -m freshet.pipeline.consumer_helloworld --brokers localhost:9092 --max 69

## Layout
    freshet/common/      # schemas (the contract) + kafka helpers
    freshet/generator/   # synthetic events + scripted incident scenario
    freshet/pipeline/    # consumers: hello-world, normalizer, embeddings (worker WIP)
    tests/               # schema + generator tests
