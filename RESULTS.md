# Results

Method and derivations behind the README's numbers. v1's numbers live on
[`v1-incident-agent`](https://github.com/KrasiKirov/freshet/tree/v1-incident-agent)
and do not describe this code.

## Staleness — `make freshness` → `results/freshness.json`

- **t0** = the provider's own `created_at`, so the poll wait is included — it's
  the delay a user actually experiences.
- **t1** = the moment the update is queryable in pgvector.
- **Batch arm** = an hourly index's mean wait, averaged over all 3,600
  possible refresh-boundary phases — not the single top-of-hour alignment,
  which a workload's own arrival clustering can flatter or punish.
- Only **live** arrivals are scored (posted after indexing began); backfilled
  history is excluded so the metric reflects pipeline speed, not when
  indexing was switched on.

**n = 35 updates (39 rows), mean 99.78s (p50 90.19s, p95 197.65s), ratio
18.04×** against the 1800.46s batch arm. Scored across the run's unbroken
span, 2026-09-04T19:57:01Z – 2026-09-05T05:03:56Z (9:06:55), zero restarts.
`n` counts distinct `event_id`s; `n_rows` is chunk rows — two Cloudflare
maintenance notices produced 3 rows each for 1 update.

**Alignment sensitivity, disclosed rather than buried:** sweeping all 3,600
boundary phases gives mean 1800.46s (18.04×), median 1652.05s (16.56×), range
1298.34s (13.01×) – 2565.91s (25.72×). This run's arrivals cluster right
after the hour, so a batch aligned exactly at HH:00:00 would score **25.72×**
— not because that alignment is realistic, but because it's the one phase
this run's own clustering is least favorable to. The published 18.04× holds
regardless of which alignment a batch system actually uses.

Up to 60s of source-side timestamp rounding (93% of the corpus, 37 of the 39
scored rows, is truncated to the minute) inflates the measured streaming
wait, which *depresses* the ratio — correcting it would raise 18.04×, not
lower it.

An earlier version of this measurement, scored without the live-arrival
filter, reported a ratio of 0.06 (streaming losing to batch) — an artifact of
a 14-hour outage whose catch-up burst got scored as pipeline staleness.
Uptime is now proven by an embedder heartbeat, and only the current unbroken
run is scored.

## Retrieval — `make live-eval` → `results/live_retrieval.json`

The old labeled-fixture eval — hand-labeled cause updates, one human reading
per incident — is deleted, not archived: it couldn't scale with the corpus
or be re-run to catch a regression. Its replacement needs no labels: the
query is an incident's first update, ground truth is any other update of the
same incident, and it regenerates against however much the live index has
accumulated.

**Measures within-incident linking, not causal identification.** A high
score says retrieval can find "more of this incident," not that what it
finds explains the cause.

Over 1,144 eligible incidents against a 5,802-chunk / 4,882-event /
42-provider live index:

| arm | recall@5 | mrr | top1_cite |
|---|---|---|---|
| hybrid | 0.708 | 0.513 | 0.392 |
| vector_only | 0.851 | 0.717 | 0.632 |
| keyword_only | 0.332 | 0.240 | 0.183 |
| blind_recent | 0.003 | 0.001 | 0.001 |

`blind_recent` returns the most recent chunks regardless of the query and
scores near zero — hybrid minus blind is 0.705, so the task isn't solvable by
recency alone.

**`vector_only` (0.851) beats `hybrid` (0.708)** on this task, contradicting
this project's own framing of hybrid as the better default. RRF fusion with
the weak keyword arm (0.332) drags hybrid down. Reported as measured; the
production default is unchanged pending a live eval of the fuller
cause-finding task this one doesn't cover.

## Autonomous delivery

Three real incidents opened and were briefed with no human trigger:

| incident | opened | brief | gap | resolved | postmortem | gap |
|---|---|---|---|---|---|---|
| github:31355391 (Copilot Code Review) | 20:39:00Z | 20:41:27.69Z | +147.7s | 22:26:00Z | 22:27:55.56Z | +115.6s |
| github:31355785 (repos contents API) | 22:02:00Z | 22:03:45.28Z | +105.3s | 22:23:00Z | 22:24:47.78Z | +107.8s |
| cloudflare:ftvf8c3m4mv5 (R2 503s, E. North America) | 21:10:00Z | 21:12:34.65Z | +154.6s | 00:42:00Z (+1d) | 00:46:48.24Z | +288.2s |

All three completed open → brief → resolve → threaded postmortem. Timestamps
are `incidents.brief_delivered_at` / `postmortem_delivered_at`, written when
the sink actually posts, not when the consumer claims the lease. `slack_ts`
is written only from a real Slack API response — the dry-run sink returns
`None` — so a non-null value on all three confirms real delivery. The demo
tool couldn't have produced them either: it requires ≥3 indexed updates
before listing a candidate, and these incidents had as few as 1 when briefed.

**3 of 3, not 3 of 13**: 10 more incidents opened in the run window, all
scheduled maintenance (Twilio, Cloudflare ×2). The lifecycle projection
correctly declines to emit an `opened` event for maintenance, so none was
briefed — briefing a maintenance window as an outage would be a false alarm.

8 briefs were delivered over the run in total: the 3 above, plus 5 delivered
in an 11-second catch-up burst after an earlier Kafka outage (open-to-brief
gaps 182s–10,503s, measuring outage delay, not steady-state latency — not
counted above).

## Things built and then deleted

- **Correlated-degradation detector** (≥3 providers degrading in one window)
  — fired zero times against 3.1 years of real data; 42 providers is too few
  for simultaneous degradation.
- **Adversarial root-cause benchmark** (v1) — a blind positional rule scored
  1.000 and beat the LLM by exploiting a constant trap offset in the
  generator.
- **Cross-encoder reranking / multi-query expansion** (v1) — rerank measured
  neutral, multi-query +0.05 while needing an extra API key; neither
  justified its cost.
- **Recency decay** — every practical half-life cost recall on retrospective
  queries, and the shipped default underflowed every score to 0.0 at
  realistic event ages.
- **Labeled-fixture retrieval eval** — required hand-labeling every
  incident's cause update; replaced by the label-free eval above.

## Honest limits

- 4% of incidents state a cause. The brief quotes the provider's sentence
  when one exists, or stays silent — it never infers a cause.
- Freshness is bounded by the 60s poll cadence, not the pipeline.
- Briefs are non-deterministic (LLM-written). Citations are verified against
  retrieved evidence on both id and timestamp, so a fabricated one is
  stripped, not shipped.
- Delivery is at-least-once: a failed Slack post now raises and retries
  instead of being recorded as delivered, so a crash after posting but
  before the database write can duplicate an alert instead. Exactly-once
  needs an outbox; a duplicate is the cheaper failure.
