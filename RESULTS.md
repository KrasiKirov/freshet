# Results

Method and derivations behind the README's numbers. Figures from the earlier
version of this project are not reproducible here and are not cited as current.

The latest retrieval refresh on 2026-10-03 covered the current public-feed
snapshot: 5,763 events, 6,904 chunks, and 42 providers. Retrieval was rerun over
all 1,312 eligible incidents. A corrected freshness run collected 20 live updates
from October 4–5, 2026 after the indexing backlog reached zero.

## Staleness — `make freshness` → `results/freshness.json`

**Corrected live-arrival result: n = 20 updates, mean 149.71s, p50 82.93s,
p95 246.20s.** The run began after catch-up at 2026-10-04T18:19:00Z and
completed at 2026-10-05T12:47:38Z. The measured ratio is 12.03× against a
modeled hourly batch mean of 1800.47s. Alignment sensitivity for the modeled
batch arm ranges from 7.69× to 16.10×, with a median of 12.09×. The sample is
small, so p95 and the ratio should be treated as an initial measurement rather
than a performance guarantee.

Version 2 records `event_indexing.first_queryable_at` only after all of an
update's chunk writes have been acknowledged by Postgres in autocommit mode.
This is a conservative upper bound on the time the full event became searchable:
it includes embedding and writes, plus the small completion-receipt overhead.
Replays preserve the first completion and advance `last_queryable_at`. Existing
rows without first-index observations stay in the corpus but are excluded from
the benchmark. A crash before completion leaves the receipt unfinished so a
retry can complete it; no partial write is scored as completed indexing.

- **t0:** the provider's timestamp, including its rounding uncertainty.
- **t1:** the first acknowledged-completion receipt for the event.
- **Window:** by default, events posted within the current embedder-heartbeat
  span; catch-up history is excluded. This heartbeat does not prove every
  upstream component was continuously available. `--since-minutes` explicitly
  replaces this window with a provider-timestamp window.
- **Batch arm:** a modeled hourly refresh, averaged over all 3,600 boundary
  phases, with the alignment range disclosed. There is no separately deployed
  batch system in this experiment.
- **Counting:** one observation per event; `n_rows` also reports chunk count.

The September 4–5 historical run reported n = 35 updates (39 chunks), mean
99.78s, p50 90.19s, p95 197.65s, and 18.04× against a modeled 1800.46s hourly
batch wait. **Those figures used the old timestamp captured before embedding
and persistence. They undercounted completion latency and must not be quoted
as validated current performance.** Reindexing also overwrote the old timestamp.
The historical alignment range was 13.01×–25.72×; 18.04× was an average across
phases, not a guarantee for every batch schedule. Source timestamp rounding and
omitted embedding/write time introduce different biases, so no corrected ratio
can be inferred from those aggregates.

`FRESHNESS_MIN_N=20 make freshness` rejects an undersized sample. The current
machine-readable report records this completed sample and its index snapshot.

## Retrieval — `make live-eval` → `results/live_retrieval.json`

The old labeled-fixture eval — hand-labeled cause updates, one human reading
per incident — is deleted, not archived: it couldn't scale with the corpus
or be re-run to catch a regression. Its replacement needs no labels: the
query is an incident's first update, ground truth is any other update of the
same incident, and it regenerates against however much the live index has
accumulated.

**Measures within-incident ranking before abstention.** The scorer uses the
retriever's hits even when it would abstain. It does not measure end-to-end Slack
answers, cause identification, or whether generated prose follows its citations.
`recall@5` is the retained field name for a hit rate: the fraction of queries with
at least one other update from the incident among the top five distinct returned
events. It is not the fraction of all relevant updates retrieved. `top1_cite`
means the first result belongs to that incident, not an LLM citation audit.

The current run covered 1,312 eligible incidents against a 6,904-chunk /
5,763-event / 42-provider live index:

| arm | recall@5 | mrr | top1_cite |
|---|---|---|---|
| hybrid | 0.699 | 0.510 | 0.391 |
| vector_only | 0.838 | 0.707 | 0.625 |
| keyword_only | 0.330 | 0.242 | 0.189 |
| blind_recent | 0.002 | 0.001 | 0.001 |

`blind_recent` returns the most recent chunks regardless of the query and
scores near zero — hybrid minus blind is 0.697, so the task isn't solvable by
recency alone.

**`vector_only` (0.838) beats `hybrid` (0.699)** on this task, contradicting
this project's own framing of hybrid as the better default. RRF fusion with
the weak keyword arm (0.330) drags hybrid down. Reported as measured; the
production default is unchanged pending an evaluation of real follow-up
questions and abstention, which this incident-linking task does not cover.

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
`None` — so a non-null value on all three confirms real delivery. The historical
run was recorded as autonomous. The current demo trigger can reset delivery
state, so demo runs must be kept separate from this evidence;
non-null Slack timestamps alone establish posting, not its trigger provenance.

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

- An earlier sample found explicit cause text in about 4% of incidents. The
  extractive cause field quotes provider text. It is separate from the LLM's
  narrative and is not a current whole-corpus prevalence measurement.
- Citation provenance checks validate an ID against the supplied evidence and
  substitute that evidence's timestamp. Unknown IDs are stripped; the prose
  remains. This does not establish semantic support or rule out hallucination.
- Delivery is at-least-once. Pending progress/postmortem flags survive failures
  and lease expiry, and an independent drain retries them after the root brief.
  Posting successfully and crashing before recording delivery can still duplicate
  a message. A durable outbox alone would not remove that external-side-effect
  ambiguity; the sink would also need an idempotency/reconciliation mechanism.
- The repeatable `make demo` uses captured evidence and temporary tables. Its
  default composer is extractive and its vectors are stubbed; `--llm` exercises
  the real composer. Neither mode measures Kafka/Flink or retrieval quality.
