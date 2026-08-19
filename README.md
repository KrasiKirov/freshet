# Freshet

[![CI](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml/badge.svg)](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml)

An agent that watches **42 public status feeds** and posts a cited incident brief
to Slack seconds after a provider updates — quoting a root cause when the
provider states one.

## How it works

```
42 Statuspage /history.atom feeds
  │  poller — ThreadPoolExecutor + stdlib urllib, 60s sweep,
  │  ETag conditional requests, staggered start, per-host backoff
  ▼
Kafka  raw.incidents
  │  Flink SQL — checkpointed dedup by (provider, incident, update),
  │  plus a lifecycle projection (opened / resolved)
  ▼
Kafka  normalized.updates          Kafka  incident.lifecycle
  │  embedder — batches → bge → pgvector          │
  ▼                                               ▼
hybrid retrieval                            Autopilot
dense (bge) + Postgres full-text,           cited Slack brief
RRF fusion, abstention floor                on open; postmortem
  │                                         on resolve
  ▼
LLM composer — every citation verified against the retrieved evidence
```

The feeds are polled, not pushed, so this is **a streaming pipeline over a polled
source with an initial backfill** — not a push stream. Freshness is bounded by the
60s poll cadence (~30s mean), not by the pipeline (~1s).

## Run it

Requires Docker, Java 21 (for Flink), and `ANTHROPIC_API_KEY` in `.env.local`.
Generation is not optional: a missing key fails loudly rather than degrading to
something that only looks like an answer.

```
make up          # Redpanda + Postgres/pgvector
make db-init
make stream      # downloads Flink, submits the dedup + lifecycle job
make poller      # begins polling the 42 feeds
make embedder    # indexes into pgvector
make autopilot   # posts cited briefs to Slack
make freshness   # the one measurement
```

`make test` runs the unit suite; `make test-integration` needs the stack up and
uses a dedicated `freshet_test` database so it cannot touch a running index.

## Measured

| | |
|---|---|
| providers polled | **42**, each verified robots-allowed and serving entries |
| one sweep | **3,676 updates in 1.6s** |
| warm sweep | **96% fewer** updates re-parsed (ETag/304) |
| dedup | 3,676 → **3,671** (exactly the 5 duplicate records in the corpus) |
| observed rate | ~**50 updates/day**, ~2 incidents/hour |
| incidents stating a cause | **3 of 68 (4%)** |

**Staleness — the headline number — is not yet measured.** `results/freshness.json`
reports `n = 0`: only updates posted *after* indexing began are scored, and live
updates arrive at roughly 2/hour, so it needs a multi-hour run. Derived from the
poll cadence it should land near **~58×** an hourly batch index (~31s vs ~1800s),
but that figure is an expectation, not a measurement, until `n` is meaningful.

## Honest limits

- **4% of incidents state a cause.** Most status updates announce progress, not
  causes. The brief quotes the provider's own sentence when there is one and says
  nothing when there isn't — it never infers a cause.
- **42 providers is a small corpus.** A correlated-degradation detector was built
  and then deleted: measured against 3.1 years of real data it fired **zero
  times**, because simultaneous degradation across three providers is rare at this
  scale.
- **Briefs are non-deterministic**, since an LLM writes them. Citations are
  verified on both id and timestamp, so a fabricated one is stripped rather than
  shipped.
- **Delivery is at-least-once, not exactly-once.** The sink posts, then the
  database records it. A crash between those two steps replays the incident after
  the lease expires and can post a duplicate. That is the deliberate direction to
  fail in — a duplicate alert is recoverable, a dropped one is not — and closing
  it would need a Slack idempotency key (the API has none) or a transactional
  outbox.
- The `robots.txt` on Statuspage disallows `/api/`, so ingestion uses the Atom
  feeds the platform publishes for subscription.

## Previous version

The v1 project — an LLM agent that identified root-cause commits, a synthetic
benchmark, and a 225-incident retrieval evaluation — is archived on the
[`v1-incident-agent`](https://github.com/KrasiKirov/freshet/tree/v1-incident-agent)
branch, with its numbers in that branch's `RESULTS.md`. It is not this codebase's
behaviour and its figures do not describe what runs here.

Method notes and the reasoning behind what was cut are in [RESULTS.md](RESULTS.md).
