# Freshet

[![CI](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml/badge.svg)](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml)

An agent that watches **42 public status feeds** and posts a cited incident brief
to Slack seconds after a provider updates — quoting a root cause when the
provider states one.

Slack is the only surface. Reply in the thread and it answers from the whole
corpus — 42 providers, four years of updates — through the same retrieval path
the eval measures, citing the updates it used. The brief itself is a key lookup
(an incident's updates are addressable); the follow-up questions are not, which
is where retrieval earns its place.

## How it works

```
42 Statuspage /history.atom feeds
  │  poller — ThreadPoolExecutor + stdlib urllib, 60s sweep,
  │  ETag conditional requests (persisted across restarts), staggered
  │  start, exponential per-host backoff, one batched produce per sweep
  ▼
Kafka  raw.incidents
  │  Flink SQL — checkpointed dedup by (provider, incident, update),
  │  plus a lifecycle projection (opened / resolved)
  ▼
Kafka  normalized.updates          Kafka  incident.lifecycle
  │  embedder — batches → bge → pgvector          │
  ▼                                               ▼
pgvector  ─────────────────────────────────►  Autopilot
                                              cited Slack brief on open,
                                              threaded postmortem on resolve
                                                    │
                            reply in the thread ───►│  hybrid retrieval
                                                    │  dense (bge) + full-text,
                                                    │  RRF fusion, abstention,
                                                    │  window inferred from the
                                                    │  question
                                                    ▼
                              LLM composer — every citation verified
                              against the retrieved evidence
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

For a measurement run, one supervisor replaces the four `make` targets — it
restarts any process that dies and holds the machine awake, because freshness
scores only an unbroken run:

```
./deploy/run-live.sh     # poller + embedder + autopilot, under caffeinate
```

Conditional-request validators persist to `~/.local/state/freshet/poll-cache.json`,
so a restart resumes with 304s instead of re-downloading all 42 feeds. Override the
location with `FRESHET_POLL_CACHE`, or set it to the empty string to disable.

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
| staleness | **18.04×** hourly batch, alignment-independent — n=35 updates, mean 99.78s (p50 90.19s, p95 197.65s) vs 1800.46s arm, 9:06:55 unbroken run |

**Staleness — the headline number — measures 18.04×** an hourly batch index: a
streaming mean of 99.78s (p50 90.19s, p95 197.65s) against a batch arm of
1800.46s, from **n = 35** distinct live updates (39 scored chunk rows) over the
run's unbroken 9:06:55 span (2026-09-04T19:57:01Z – 2026-09-05T05:03:56Z,
process start to clean shutdown; `results/freshness.json`). `n` counts distinct
updates (`event_id`), not chunk rows — two Cloudflare maintenance notices each
contributed 3 rows for 1 update.

**The batch arm is alignment-independent, not the HH:00:00 figure.** An hourly
batch waits for its next refresh boundary, but that boundary's phase within the
hour is an arbitrary choice — one of 3,600 possible second-offsets — and real
arrivals cluster at the top of the hour because scheduled maintenance windows
start on the hour by nature. Scoring only the HH:00:00 alignment lets this
run's own clustering pick a flattering or unflattering number: swept across all
3,600 phases, this run's arm ranges from 1298.34s (13.01×) to 2565.91s
(25.72×, which is what HH:00:00 alone would report), with a median of 1652.05s
(16.56×). The published **18.04×** is the mean across every alignment, not the
one this workload happens to be least favorable to. That derivation is why
1800.46s lands almost exactly on the textbook `interval/2` = 1800s a uniform
hourly cadence implies; the 24.39× figure this replaced did not, because it was
scoring one arbitrary alignment rather than the cadence itself.

Measured staleness also includes up to 60s of source-side rounding: 93% of the
corpus and 37 of the 39 scored rows carry a provider timestamp truncated to the
minute (only HashiCorp's rows in this window carry sub-minute precision). That
rounding inflates the measured streaming wait, which understates the ratio —
correcting it would raise 18.04×, not lower it.

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
