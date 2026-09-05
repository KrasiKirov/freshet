# Results

What is measured, how, and what was deleted after measuring it. Every figure here
is reproducible from this branch; v1's numbers live on the
[`v1-incident-agent`](https://github.com/KrasiKirov/freshet/tree/v1-incident-agent)
branch and do not describe this code.

## The one measurement: end-to-end staleness

`make freshness` → `results/freshness.json`

- **t0** = the provider's own `created_at`. This deliberately includes the poll
  wait we do not control, because it is the delay a user experiences. Measuring
  from *fetch* time instead would flatter the number by excluding its dominant
  term.
- **t1** = the moment the update is queryable in pgvector.
- **Comparison arm** = an hourly batch index. `batch_staleness` waits for the
  next refresh boundary after an update posts — but that boundary's phase
  within the hour is an arbitrary choice, and real arrivals cluster at the top
  of the hour (scheduled maintenance windows start on the hour by nature).
  Scoring one phase alone — HH:00:00 exactly — lets the workload's own
  clustering pick the most flattering (or least flattering) alignment. The
  arm reported below is the mean batch wait **averaged over every one of the
  3,600 possible second-offsets the boundary could sit at**, which is what
  makes uniformly-arriving events average `interval/2`: that derivation only
  holds once the alignment itself stops being a free variable.

**Only live arrivals are scored** — updates posted after indexing began
(`ts >= min(indexed_at)`). This matters more than it sounds. Scored without that
filter over a 24h window, the pipeline reported a mean staleness of **41,995s and
a ratio of 0.06** — streaming apparently *losing* to hourly batch — purely because
three years of backfilled history had all been indexed at once. The filter is what
makes the metric measure pipeline speed rather than when it was switched on.

**Measured: n = 35 updates (39 scored rows), mean 99.78s, p50 90.19s,
p95 197.65s**, ratio **18.04×** against the alignment-independent hourly-batch
arm (1800.46s). `n` counts distinct updates (`event_id`); `vector_records` holds
one row per chunk, and two Cloudflare maintenance notices in this window each
contributed 3 rows for 1 update, which is where the 39 → 35 difference comes
from. Scored across the run's unbroken span from process start to clean
shutdown, 2026-09-04T19:57:01Z – 2026-09-05T05:03:56Z (**9:06:55**, basis:
`logs/supervisor.log`'s matched start/stop lines), zero child restarts.

**The alignment sensitivity, disclosed rather than buried:** sweeping the same
35 updates across all 3,600 possible boundary phases gives a mean of 1800.46s
(ratio 18.04×), a median of 1652.05s (16.56×), and a range from 1298.34s
(13.01×) to 2565.91s (25.72×). The phase this run's workload is *least*
favorable to happens to be almost exactly HH:00:00 — an hourly batch aligned
there would score **25.72×**, not because that alignment is realistic but
because this run's arrivals cluster right after the hour, which is the one
phase against which they each wait nearly a full interval. The published
18.04× does not assume that alignment; it is the figure that holds regardless
of which one a batch system actually uses.

The pipeline stopped at 2026-09-05T05:03:56Z, after a clean shutdown
(`logs/supervisor.log` shows three matched `stopping` lines, `restarts=0`, no
orphaned children) — this is a completed run, not one still in progress.

**Source-timestamp granularity is an unmodelled term.** 93% of corpus rows
(5,393 of 5,802) and 37 of the 39 scored rows carry `ts` truncated to a whole
minute by the source itself — of the providers in this run's scored window,
only HashiCorp publishes sub-minute precision. Measured staleness therefore
includes up to 60s of source-side rounding that the pipeline did not cause.
This is *conservative*: truncation rounds `posted_at` down, which inflates the
measured streaming wait, which *depresses* the ratio — correcting it would
raise 18.04×, not lower it. It stays an open, unmodelled term in this
project's one measurement rather than something folded into the headline.

The earlier number here (`n=33`, ratio 0.06 — streaming apparently 14x SLOWER than
hourly batch) was an artifact and has been deleted. Its cause is worth recording:
the filter `ts >= min(indexed_at)` excludes backfilled HISTORY but cannot tell a
slow pipeline from a stopped one. The pipeline had been down ~14 hours; every
update posted during the outage was indexed in the catch-up burst afterwards and
scored as ~9.8 hours of staleness. The measurement was of my own outage.

Uptime is now proven rather than assumed: the embedder writes a heartbeat, and
only the CURRENT unbroken run is scored (a gap over 5 minutes starts a new run).
A restart therefore resets the window instead of charging its backlog to the
pipeline's speed. If there are no live arrivals, the report carries `status: not
yet measured` and no ratio at all, and `FRESHNESS_MIN_N` fails the run rather
than reporting a thin sample.

## Measured on the live pipeline

| | |
|---|---|
| providers | 42, each verified robots-allowed and serving entries |
| one sweep | 3,676 updates from 42 feeds in 1.6s |
| warm sweep | 96% fewer updates re-parsed (ETag → 304) |
| dedup | 3,676 → 3,671, matching the 5 duplicate records counted independently |
| observed rate | ~50 updates/day (30-day), ~88/day (7-day) |
| lifecycle | 205 opened / 195 resolved per 400 events — a plausible balance |
| stated causes | 3 of 68 incidents (4%) |

## Things that were built and then deleted

The useful part of this project is what the measurements killed.

**A correlated-degradation detector** (≥3 providers degrading in one 5-minute
event-time window). Measured against 3.1 years of real data it fired **zero
times**; even a 60-minute window fires ~6×/year. 42 providers are too few for
simultaneous degradation. It had been designed to justify using Flink rather than
to serve the objective — the wrong direction of reasoning, and the measurement
made that visible.

**An adversarial root-cause benchmark**, deleted with the v1 agent. Worth
recording why: it was *game-able*. A blind positional rule ("second-to-last
change") scored **1.000 and beat the LLM** while understanding nothing, because
the generator planted every trap at a constant offset. Rebuilding it — randomised
trap counts, a permanent guard scoring blind index rules against an explicit
chance ceiling — was what made its later numbers meaningful.

**Cross-encoder reranking and multi-query expansion.** v1 measured rerank as
neutral at benchmark scale and multi-query at +0.05 while requiring an API key.
Neither justified its cost.

**Recency decay.** Every practical half-life cost recall on retrospective queries,
and the shipped 30-minute default underflowed every score to 0.0 at realistic
event ages — a feature that silently did nothing.

## Retrieval eval

`make live-eval` -> `results/live_retrieval.json`

The labeled-fixture retrieval eval (`retrieval_eval.py`, `chunk_sweep.py`,
`calibrate_abstention.py`, and the hand-labeled `labels_live.json` /
`fixtures/real/` corpus behind them) has been deleted, not archived. It
required a human to read each incident and label its cause update, which
does not scale with the corpus and cannot be re-run to check that a change
did not regress it. Every figure it ever produced — including the chunk-size
sweep's recall@5 0.917 and the audit's calibrated-floor numbers — is deleted
along with it rather than kept as a historical claim, because nothing can
check whether it still holds.

Its replacement, `freshet/eval/live_retrieval.py`, needs no labels: the query
is an incident's first update, the correct answer is any OTHER update of the
SAME incident, and ground truth is the incident id column retrieval already
carries. It regenerates against whatever the live index has accumulated, so
it grows with the corpus instead of going stale.

**Scope, precisely — this is a narrower claim than the retired eval made.**
It measures WITHIN-INCIDENT LINKING: given the opening symptom of an
incident, can retrieval surface the rest of that incident's thread out of the
whole index? It does **not** measure causal identification — the retired
eval's claim, backed by hand-labeled cause updates naming which update in an
incident stated the cause. A high score here says retrieval can find "more
of this same incident"; it says nothing about whether what it finds explains
it.

Measured over 1,144 eligible incidents (each with >=2 distinct updates)
against a 5,802-chunk / 4,882-event / 42-provider live index:

| arm | recall@5 | mrr | top1_cite |
|---|---|---|---|
| hybrid | 0.708 | 0.513 | 0.392 |
| vector_only | 0.851 | 0.717 | 0.632 |
| keyword_only | 0.332 | 0.240 | 0.183 |
| blind_recent | 0.003 | 0.001 | 0.001 |

`blind_recent` is the gameability guard — a query-blind rule that just
returns the most recent chunks. It scores near zero, and hybrid minus blind
is 0.705 (verdict: "meaningful"), so the task is not solvable by recency
alone.

**`vector_only` (0.851) beats `hybrid` (0.708) on this task.** That
contradicts this project's own framing of hybrid retrieval as the better
default, stated elsewhere in this document and in the README. It is reported
rather than smoothed over: on within-incident linking specifically, RRF
fusion with the keyword arm (0.332) drags hybrid below vector alone. This is
one eval of one narrower task, not a case for dropping hybrid — the
production default is unchanged pending a live eval of the fuller
cause-finding task this result does not cover. Recorded here so the next
person does not have to rediscover it.

## Autonomous delivery

Three real incidents opened and were briefed with no human trigger:

| incident | title | opened | brief delivered | gap | resolved | postmortem delivered | gap |
|---|---|---|---|---|---|---|---|
| `github:31355391` | Disruption with Copilot Code Review | 20:39:00Z | 20:41:27.69Z | **+147.7s** | 22:26:00Z | 22:27:55.56Z | +115.6s |
| `github:31355785` | Degradation in repos contents API | 22:02:00Z | 22:03:45.28Z | **+105.3s** | 22:23:00Z | 22:24:47.78Z | +107.8s |
| `cloudflare:ftvf8c3m4mv5` | Elevated R2 503 errors, Eastern North America | 21:10:00Z | 21:12:34.65Z | **+154.6s** | 2026-09-05 00:42:00Z | 2026-09-05 00:46:48.24Z | +288.2s |

"Brief delivered" and "postmortem delivered" are `incidents.brief_delivered_at`
and `incidents.postmortem_delivered_at` — the timestamps `mark_brief_delivered`
writes once the sink actually posts. An earlier version of this table quoted
`briefed_at`/`postmortem_at`, which is when the consumer *claims the lease*
before generating and posting the brief; that made all six gaps 1.7–1.9s
smaller than the delivery the numbers claim to measure.

All three timestamps are direct reads from `incidents` after the run ended, not
carried forward from an earlier check — `cloudflare:ftvf8c3m4mv5` had not yet
resolved the last time this was measured, and re-querying is what turned up its
resolve and postmortem rows.

**No human triggered any of these.** `make demo-brief`'s candidate query
(`freshet/autopilot/demo_trigger.py`) requires `count(DISTINCT event_id) >= 3`
indexed updates before it will even list an incident; `github:31355391` had
exactly one indexed row when it briefed, so the demo tool could not have
selected it — not merely "wasn't used," but structurally incapable of it. And
`slack_ts` has exactly one writer in the codebase, `mark_brief_delivered`
(`freshet/autopilot/consumer.py`), fed only by `sink.deliver()`'s return value;
the dry-run sink returns `None`. A non-null `slack_ts`, which all three rows
carry, can only be a real Slack API response.

**Coverage is 3 of 3 real incidents, not 3 of 13.** 13 incidents opened during
this run window: the 3 above, and 10 scheduled-maintenance rows (Twilio SMS
carrier-partner maintenance across several regions, and two Cloudflare
datacentre maintenance windows in Canberra and Fukuoka). None of the 10 was
briefed. The `incidents` table records an `opened_at` for maintenance rows same
as for outages, but the lifecycle projection's status predicate correctly
declines to emit an `opened` event for them — the two paths disagree, and only
the briefing path's behavior is user-visible. Briefing a maintenance window as
an outage would be a false alarm, so the 3-of-3 figure, not 3-of-13, is the
right one to quote.

**8 briefs were delivered over the run in total**: the 3 above, plus 5 more at
start-up (`grafana:31354051`, `zoom:31354321`,
`hashicorp:01M1PRS06XEYC48D769GDVZ0YW`, `vercel:31354879`,
`cloudflare:6ztvhhp2ll11`) for incidents that had opened during an earlier
Docker/Kafka outage. Those 5 were caught up within an 11-second window once the
pipeline came back; their open-to-brief gaps (182s–10,503s) measure how long
the outage delayed them, not the pipeline's steady-state latency, and are not
counted among the latency figures above.

## Honest limits

- **4% of incidents state a cause.** The brief quotes the provider's sentence when
  one exists and stays silent otherwise. Conservative filters reject promised
  RCAs ("a detailed root cause analysis will be shared"), progress announcements
  ("we have identified the root cause and reverted the change"), and ongoing
  investigations — each rule derived from a real false positive, not speculation.
- **The source is polled, not pushed.** Freshness is bounded by the 60s cadence.
- **Briefs are non-deterministic**, since an LLM writes them. Citations are
  verified on both event id and timestamp against the retrieved evidence, so a
  fabricated one is stripped rather than shipped.
- **Delivery is at-least-once.** A failed Slack post now raises: the consumer
  releases its claim and the Kafka offset stays uncommitted, so the brief is
  retried instead of being recorded as delivered. The cost of that choice is the
  opposite failure — a crash after the post but before the database write can
  duplicate an alert. Exactly-once would need an outbox; a duplicate alert is the
  cheaper of the two failures.
