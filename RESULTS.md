# Results

Reproducible numbers, newest first. Hardware context: Apple Silicon laptop,
single-node Redpanda + Postgres in Docker, workers on the host.

## M4 — consumer-group scaling (embedder, all-MiniLM-L6-v2)

1,009 live events produced as an instantaneous burst into 3-partition topics,
time measured from burst start to all events queryable in pgvector
(`make scale-demo`, 2026-06-12):

| embedder instances | drain time | throughput |
|---|---|---|
| 1 | 15s | 67 ev/s |
| 3 | 10s | 100 ev/s |

Honest read: the 1.5× speedup is embedder scaling working until the *next*
bottleneck appears — at 3 instances the single normalizer caps the pipeline at
~100 ev/s, because it does a delivery-checked (per-message flushed) produce per
event. A lighter burst (309 events) shows no speedup at all: one embedder
already keeps up, so there is nothing to parallelize. Scaling consumers moves
bottlenecks; it does not delete them.

Reproduce: `make down && make up && WORKERS=1 make scale-demo` (then WORKERS=3).

## M2 — event-to-queryable freshness (slice demo, real embedder)

p50 ≈ 2–3 s, p95 ≈ 6 s over 69 live events (`make slice`; printed by
`freshet.eval.freshness`). The full eval harness with committed artifacts is
M6.
