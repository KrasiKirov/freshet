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

**Live index, n = 55.** Every part of this is real provider text — the documents,
the queries, and the ground truth:

| arm | recall@5 | MRR | top-1 cite |
|---|---|---|---|
| **hybrid (shipped)** | **0.455** | **0.321** | **0.255** |
| vector-only | 0.418 | 0.313 | 0.236 |
| keyword-only | 0.364 | 0.253 | 0.182 |
| *blind recency (guard)* | *0.000* | *0.000* | *0.000* |

**The query is the incident's own first update**, verbatim, with the title prefix
stripped — the symptom as the provider wrote it, which is what an on-call engineer
actually reacts to. The task is then: from that symptom, retrieve the update where
the provider states the cause. The query's own document is excluded from scoring
(otherwise it is trivially its own top hit, which forced top-1 to 0.000 on every
arm — an artifact of the setup, not a property of retrieval).

**Why generated questions were scrapped.** An earlier version asked an LLM to write
the questions. A check across those 64 paraphrases found **7 that invented a
specific the incident never involved** — Postman became "the mail delivery
application", Render "the rendering system". Fabricated inputs were feeding the
benchmark, so the numbers they produced (hybrid 0.422) are withdrawn.

**Hybrid leads on all three metrics** here, which is the clearest evidence for it so
far: on title-derived questions hybrid and keyword-only were 0.031 apart, and on
generated paraphrases hybrid and vector-only were indistinguishable on MRR.

**A correction to an earlier finding.** The paraphrased run reported 33 of 64
questions abstaining, and I read that as the 0.70 floor being miscalibrated for real
language. On real text it is **0 of 55**, with off-corpus still 6 of 6. The floor was
not the problem — short synthetic questions were. No threshold was changed.

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

**How the live labels were built.** Candidates are shortlisted by cause marker, then an
LLM judges whether each update NAMES a cause — it never sees the query or the ranking,
so it cannot leak an answer into the metric. Each question is generated from the
incident TITLE alone, so it cannot encode which update is correct. The judge was
validated first on the six cases keyword matching gets wrong ("The root cause has been
fixed" names nothing; "triggered by date time triggers" is a false match) and scored
6/6 before being trusted with the corpus.

**Outstanding: a human has not signed off these labels.** The next step is a
20-row spot check — read 20 of the 64 `labels_live.json` entries, confirm the
cause sentence names a cause and the question seeks one, then set
`curated: reviewed`. Until that happens the live numbers stay indicative.

- [ ] 20-row human review of `freshet/eval/fixtures/labels_live.json`

**Honest limits.** The labels are `curated: assistant-reviewed` — reviewed against the
criterion above, but not signed off by a human, and the reviewer is the same system
that produced them. Every rejection and collapse is recorded in `_review` so the calls
are auditable. Weak-but-kept causes remain ("This was caused by a database impairment"
names little), and title-derived questions flatter the lexical arm.

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
