# Freshet

[![CI](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml/badge.svg)](https://github.com/KrasiKirov/freshet/actions/workflows/ci.yml)

An agent that watches **42 public status feeds** and posts a cited incident
brief to Slack seconds after a provider updates — quoting a root cause when
the provider states one. Reply in the thread and it answers from the whole
corpus (42 providers, four years of updates) through the same retrieval path
the eval below measures, citing what it used.

![`make demo-brief` firing a real GitHub incident: opened event, cited brief, threaded postmortem](docs/autopilot-loop.gif)

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
pgvector  ─────────────────────────────────►  Autopilot
                                              cited Slack brief on open,
                                              threaded postmortem on resolve
                                                    │
                            reply in the thread ───►│  hybrid retrieval
                                                    │  dense (bge) + full-text,
                                                    │  RRF fusion, abstention
                                                    ▼
                              LLM composer — every citation verified
                              against the retrieved evidence
```

Feeds are polled, not pushed: this is a streaming pipeline over a polled
source. The 60s sweep accounts for roughly 31s of the 99.78s measured mean,
so the poll wait is real but is not the binding term — most of the delay is
downstream of it. Note also that 93% of providers stamp updates to the whole
minute, which inflates any measurement taken from their own timestamps.

## Run it

Requires Docker, Java 21 (for Flink), and `ANTHROPIC_API_KEY` in `.env.local`.

```
make up && make db-init
make stream      # submits the Flink dedup + lifecycle job
make poller      # polls the 42 feeds
make embedder    # indexes into pgvector
make autopilot   # posts cited briefs to Slack
make demo-brief  # fires a real opened event on demand
make freshness   # the one measurement
```

For a measurement run, `./deploy/run-live.sh > logs/supervisor.log 2>&1`
replaces the poller/embedder/autopilot targets: it restarts any child that
dies and blocks idle sleep, because freshness only scores an unbroken run.

`make test` runs the unit suite; `make test-integration` needs the stack up
and uses a dedicated `freshet_test` database.

## Measured

| | |
|---|---|
| staleness | **18.04×** an hourly batch index — n=35 live updates, mean 99.78s (p50 90.19s, p95 197.65s) vs a 1800.46s batch arm, 9:06:55 unbroken run |
| retrieval (recall@5) | hybrid **0.708**, vector_only **0.851**, keyword_only 0.332, blind-recent control 0.003 — 1,144 queries, no labels |
| autonomous delivery | **3 of 3** real incidents briefed with no human trigger; all three completed open → brief → resolve → threaded postmortem |

**Staleness is reported alignment-independent, not at the flattering
alignment.** An hourly batch's refresh boundary sits at an arbitrary phase
within the hour; scored only at the top of the hour — where real arrivals
cluster, since maintenance windows start on the hour — this run's arm would
score **25.72×**. The published 18.04× is the mean across all 3,600 possible
phases, not the one this workload is least favorable to.

**Retrieval measures within-incident linking, not causal identification**:
given an incident's opening update, can retrieval find the rest of that
incident's thread in the full index? It does not measure whether what it
finds explains the cause. On this task **vector_only beats hybrid** — RRF
fusion with the weak keyword arm (0.332) drags hybrid down.

Full method, derivations, and what was built and then deleted: [RESULTS.md](RESULTS.md).

## Honest limits

- **4% of incidents state a cause.** The brief quotes the provider's sentence
  when one exists and stays silent otherwise — it never infers a cause.
- **42 providers is a small corpus.** A correlated-degradation detector was
  built and deleted after firing zero times against 3.1 years of real data.
- **Briefs are non-deterministic** (LLM-written). Citations are verified
  against retrieved evidence on both id and timestamp; a fabricated one is
  stripped, not shipped.
- **Delivery is at-least-once, not exactly-once.** A crash between posting to
  Slack and recording it can replay a brief as a duplicate after the lease
  expires. That is the deliberate failure direction: a duplicate is
  recoverable, a dropped alert is not.
- Ingestion uses Statuspage's Atom feeds, not its `/api/`, which `robots.txt`
  disallows.

v1 (an LLM root-cause agent, a synthetic benchmark, a 225-incident retrieval
eval) is archived on [`v1-incident-agent`](https://github.com/KrasiKirov/freshet/tree/v1-incident-agent);
its numbers describe that branch, not this one.
