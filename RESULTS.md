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
- **Comparison arm** = an hourly batch index. Derived, not guessed: uniformly
  arriving events wait `interval/2` on average, so the hourly arm is ~1800s.

**Only live arrivals are scored** — updates posted after indexing began
(`ts >= min(indexed_at)`). This matters more than it sounds. Scored without that
filter over a 24h window, the pipeline reported a mean staleness of **41,995s and
a ratio of 0.06** — streaming apparently *losing* to hourly batch — purely because
three years of backfilled history had all been indexed at once. The filter is what
makes the metric measure pipeline speed rather than when it was switched on.

**Status: not yet measured.** `n = 0`. Live updates arrive at ~2/hour across 42
providers, so a meaningful sample needs a multi-hour run. Derived from the 60s
cadence the figure should land near **58×** (~31s vs ~1800s), of which ~30s is
poll cadence and ~1s is this pipeline — but that is an expectation, and it stays
labelled as one until `n` supports it.

## Retrieval quality

`make retrieval-eval` (frozen corpus) and `RETRIEVAL_EVAL_SOURCE=live make retrieval-eval`.

v2 shipped with no retrieval measurement at all, which made every retrieval change
unfalsifiable. Two corpora, both real provider text — no synthetic data:

| | frozen fixture | live index |
|---|---|---|
| incidents | 225 (5 providers) | ~1,180 (42 providers) |
| labeled | **12**, human-reviewed | **81**, LLM-judged (draft) |
| reproducible anywhere | yes | no — needs a populated index |

**Live index, n = 81** — the larger sample, and the one that separates the arms:

| arm | recall@5 | MRR | top-1 cite |
|---|---|---|---|
| **hybrid (shipped)** | **0.654** | 0.462 | 0.346 |
| vector-only | 0.630 | 0.461 | **0.358** |
| keyword-only | 0.556 | 0.360 | 0.235 |
| *blind recency (guard)* | *0.000* | *0.000* | *0.000* |

**Frozen fixture, n = 12**: hybrid 0.917 / 0.583 / 0.417 — statistically identical to
v1's 0.917 / 0.576 / 0.417 on the same corpus, which is the check that v2's retrieval
did not regress when rerank, multi-query and recency decay were deleted. At n = 12 the
arms are indistinguishable (vector-only shows a higher MRR); at n = 81 hybrid leads on
recall@5, which is the evidence for keeping it.

**The guard.** A query-blind ranker (recency order, ignores the question) is scored
every run and reported next to the system. v1's root-cause benchmark was game-able —
a positional rule that understood nothing scored 1.000 — so a benchmark that a blind
rule can win is treated as void. It scores 0.000 here.

**Abstention on real language**: 0/12 on-corpus abstentions on the fixture, 13/81 on the
live set (the system declines 16% of labeled questions), and 6/6 off-corpus questions
abstain.

**Honest limits.** The live labels are `curated: draft`: candidates are shortlisted by
cause marker, then an LLM judges whether each update NAMES a cause (it never sees the
query or the ranking), and each question is generated from the incident TITLE alone so
it cannot encode which update is the answer. Spot-checking shows real errors — at least
one symptom restatement labeled as a cause, and several questions that ask about
symptoms rather than causes. 0.654 is therefore a floor, not a clean measurement, until
the labels are human-reviewed.

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
