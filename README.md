# Freshet — Real-Time Incident Intelligence

Freshness-first streaming-RAG system for on-call engineers. See `BRIEF.md` for
the full what/why/how and build order. The repo currently contains the M2
vertical slice: events flow generator → Kafka (Redpanda) → normalizer →
embedding worker → Postgres/pgvector and are queryable within seconds via a
minimal vector-search API. Hybrid retrieval, dashboards, and the evaluation
harness are upcoming milestones.

## Quickstart

    python3 -m venv .venv && source .venv/bin/activate
    pip install -e ".[test]"
    pytest -q                  # unit tests, no broker needed

    make up                    # Redpanda + Postgres/pgvector, waits until healthy
    make slice                 # full pipeline demo + freshness report (see below)
    make down                  # tear down (drops the Postgres volume)

`make slice` uses the local MiniLM embedding model (`pip install -e ".[embed]"`,
~90 MB download on first use). `EMBEDDER=stub make slice` runs with
deterministic fake embeddings and no download. Run the demo on a fresh stack
for clean freshness numbers.

## What the demo shows

1. **Generate** — emit live-stamped synthetic events (noise + a scripted
   scheduler-api incident) to Kafka `raw.events`; each event carries `ts`
   (event time).
2. **Normalize** — the normalizer worker consumes `raw.events`, stamps
   `ingested_at`, and forwards to `normalized.events`.
3. **Embed** — the embedding worker consumes `normalized.events`, stamps
   `indexed_at`, computes a vector embedding, and upserts into
   `pgvector` (`vector_records`) idempotently.
4. **Freshness report** — `freshet.eval.freshness` prints p50/p95/p99 of
   `indexed_at − ts` across all records (freshness = time from event to
   queryable).
5. **Semantic query** — an example vector search for `"what is happening with
   scheduler-api?"` returns the top-3 hits with cosine score, source, `ts`,
   and `indexed_at`.

The three timestamps (`ts`, `ingested_at`, `indexed_at`) let you measure
per-stage latency and total freshness end-to-end.

## Other commands

    make smoke            # Kafka round-trip sanity check
    make api              # serve POST /query on :8000
    make test-integration # end-to-end test against the running stack
    make db-init          # apply schema to a running stack (idempotent)

Example query against `make api`:

    curl -s localhost:8000/query -X POST -H 'content-type: application/json' \
      -d '{"question": "what is happening with scheduler-api?", "k": 3}'

## Layout

    freshet/common/      # schemas (the contract), kafka helpers, db helper
    freshet/generator/   # synthetic events + scripted incident scenario (--live mode)
    freshet/pipeline/    # workers: normalizer, embedder (+ embedding backends)
    freshet/api/         # minimal vector-search API (hybrid retrieval comes in M5)
    freshet/eval/        # freshness report (full eval harness comes in M6)
    db/                  # init.sql: pgvector extension + vector_records
    scripts/             # run_slice.sh demo
    tests/               # unit + integration tests

Notes: Kafka on `localhost:9092`, Postgres on `localhost:5433` (5432 left free),
db/user/password `freshet`.
