# Results

Reproducible numbers, newest first. Hardware context: Apple Silicon laptop,
single-node Redpanda + Postgres in Docker, workers on the host.

## Impact estimation (indicative, keyless)

The autopilot's impact line is a *derived indicator* — Low/Medium/High from breadth,
duration, and error-percentages quoted in the incident text. It is **not** measured
user impact (no error-rate/affected-user metrics exist in the corpus). `make
impact-eval` measures how well those observable proxies recover an **authored,
severity-driven** label on a dedicated benchmark (12 incidents spanning Low/Med/High;
the shared retrieval benchmark is untouched): exact agreement 0.583, adjacent-
tolerant agreement 0.917 (Low↔High counts as a worse miss than Low↔Medium). The
misses are honest: incidents that were severe but quiet in their observable signals
(under-estimated), and a small-but-loud spike (over-estimated).

## M14 — RAG quality: stronger retriever + query transformation

This is a standard production-RAG stack — dense + lexical hybrid, RRF fusion,
cross-encoder reranking, citation verification, measured on a 160-query benchmark.
M14 levels up the two pieces that were still weak: the embedding model and query
transformation.

**Embedding model — MiniLM-L6 → bge-base-en-v1.5 (768-dim).** Re-running the
deterministic 160-query benchmark with the upgraded retriever (query-side
instruction prefix included):

| mode | recall@5 (MiniLM → bge) | nDCG@5 (MiniLM → bge) |
|---|---|---|
| keyword-only | 0.609 → 0.584 | 0.502 → 0.490 |
| vector-only | 0.672 → **0.803** (+0.13) | 0.517 → 0.567 |
| **hybrid** | 0.697 → **0.797** (+0.10) | 0.535 → **0.624** (+0.09) |

Honest read: bge is a large, real win on the embedding-dependent arms — hybrid
recall@5 **0.70 → 0.80** and nDCG@5 **0.54 → 0.62**, vector recall@5 **0.67 →
0.80**. Keyword-only uses no embeddings, so its recall is fixed by the corpus,
the queries, and the tie-break — the MiniLM→bge delta there (0.609 → 0.584) is
**not** an embedding effect but the SQL tie-order noise this benchmark used to
carry: the keyword arm's `ts_rank` produces many ties, and until a deterministic
`ORDER BY … , chunk_id` tiebreak was added they resolved by physical heap order,
so each run (the eval DELETEs and re-INSERTs the corpus) drew a slightly
different number. That is now pinned — re-running yields byte-identical JSON (see
M12). On recall@5, hybrid (0.797) and vector (0.803) are a **statistical dead
heat** — within one query of each other — but hybrid decisively wins **nDCG@5
(0.624 vs 0.567)** and MRR (0.616 vs 0.541): it ranks the relevant events higher
even when the retrieved set is comparable. The MiniLM column is a frozen snapshot
of the prior committed run (`results/retrieval_metrics_minilm.json`), taken
before the tiebreak fix; the pgvector column is a fixed dimension, so 384-dim and
768-dim models cannot index into the same DB, and only the bge "after" is run
live. Reproduce: `make up && make embedding-compare`.

**Query transformation — LLM multi-query.** An LLM rewrites the question into
paraphrases; each is retrieved and the results are RRF-fused. Measured single-vs-
multi on 20 benchmark queries:

| config | recall@5 |
|---|---|
| single-query | 0.775 |
| **multi-query** | **0.825** (+0.05) |

Honest read: a real, modest lift (+0.05) even on the benchmark's already-clean
auto-derived queries. **Indicative and non-deterministic** (one committed run;
paraphrases by `claude-sonnet-4-6`) — an earlier run scored +0.10, which is the
point of labeling it indicative. Key-gated. Reproduce (needs a key):
`make multiquery-eval`. Multi-query is also an opt-in `/query` flag (off by
default, key-gated).

## M11 — multi-step retrieval vs single-shot baseline

> **Framing note (revised in M14, ablation run 2026-07-15):** this was previously
> sold as "agentic RAG." The honest read is narrower. What it shows is that a
> **non-semantic temporal lookup** (`get_events_around`, "what happened just
> before the spike?") closes a gap that single-shot semantic retrieval cannot —
> *not* that the LLM agency adds measurable value over a fixed pipeline. The
> ablation now confirms this: a keyless, deterministic `fixed-two-step` arm
> (identical whole-corpus search, then `events_around` anchored on the top spike
> hit) **exactly matches the agent** at 1.000/1.000. The win is the **retrieval
> capability**, not the agent loop.

M12 measured a sharp gap: at **whole-corpus scale** (no service hint), single-shot
retrieval scored **0.0 cause-recall** under the old MiniLM retriever — a terse
`Deploy v2.15.0 started` event is not semantically similar to "what caused this
incident?". The stronger bge retriever (M14) lifts the baseline but does **not**
close the gap; the multi-step investigator re-retrieves with the temporal lookup to
recover the rest.

Measured head-to-head on **12 sampled incidents (2 per archetype)**, all three
arms at whole-corpus scale, under the bge retriever (agent runs on
`claude-sonnet-4-6`; run 2026-07-15):

| config | cause_recall | fix_recall |
|---|---|---|
| single-shot (keyless baseline) | 0.167 | 0.417 |
| **fixed-two-step (keyless ablation)** | **1.000** | **1.000** |
| agent (LLM tool loop) | 1.000 | 1.000 |

Honest read: the temporal lookup recovers the true cause and fix on **all 12
incidents across all six archetypes** — and the deterministic `fixed-two-step`
arm does it **without an LLM**. The agent adds exactly nothing on this benchmark
(+0.000/+0.000 over the ablation); the entire +0.833/+0.583 lift over single-shot
belongs to the retrieval capability. The bge upgrade raised the single-shot
baseline from 0.0/0.25 (MiniLM) to 0.17/0.42 — a better retriever genuinely helps —
but it still misses most causes, which the temporal lookup recovers. Caveats kept
in front: (1) the agent arm is **indicative and non-deterministic**; the other two
arms are keyless and deterministic. (2) The sample is small (12) by design. (3) On
messier real corpora, where the fixed anchor-on-top-spike heuristic can pick the
wrong anchor, the agent loop may re-earn its keep — the synthetic benchmark
cannot show that either way.

Reproduce: `make up && make agent-eval` — keyless runs score the single-shot and
fixed-two-step arms (both deterministic); the agent arm needs `ANTHROPIC_API_KEY`.
A sample investigation transcript a keyless clone can read is committed at
[`results/agent_transcript.md`](results/agent_transcript.md), and
`make agent-demo` regenerates it.

## M12 — benchmark-scale evaluation (supersedes the toy-scale numbers below)

The earlier evals ran on a handful of queries against a single incident. M12
replaces them with a **seeded 40-incident benchmark spanning six failure
archetypes** (deploy regression, config change, dependency outage, resource
exhaustion, cert expiry, bad migration). Ground truth (each incident's spike,
cause, and fix event) is authored *with the corpus*, and ~160 labeled retrieval
queries are **auto-derived** from it — so the numbers are not hand-picked to
flatter. Recency decay is disabled in eval (`tau≈∞`) and both retrieval arms
break score ties on `chunk_id` (so tied rows never fall back to non-deterministic
physical heap order), making every figure deterministic; re-running produces
byte-identical JSON regardless of `PYTHONHASHSEED`.

Reproduce: `make up && make eval && make rootcause-eval`.

**Retrieval quality** — 160 auto-derived queries (mean, k=5), all-MiniLM-L6-v2.
*(Superseded by the bge numbers in M14 above; kept as the MiniLM baseline.)*

| mode | recall@5 | precision@5 | MRR | nDCG@5 |
|---|---|---|---|---|
| keyword-only | 0.609 | 0.144 | 0.474 | 0.502 |
| vector-only | 0.672 | 0.170 | **0.500** | 0.517 |
| **hybrid** | **0.697** | **0.179** | 0.499 | **0.535** |

Honest read: at 160 varied queries, **hybrid wins recall@5 and nDCG@5** over
either arm alone — the headline claim survives the harder benchmark. MRR is a
**dead heat** (vector edges hybrid by 0.001), so hybrid's win is about surfacing
*more* relevant events, not ranking the first one higher. precision@5 is low
across the board (~0.18) because each query has only a few relevant events, which
caps precision@5 mechanically; recall@5 and nDCG@5 are the meaningful columns.
Every number is lower than the 6-query table below — that is the point: this is a
credible measurement, not a flattering one.

![retrieval quality](results/retrieval_quality.png)

**Root-cause completeness** — 40 incidents, service-scoped retrieval (k=12,
mirroring the product's root-cause path), generalized timeline:

| config | cause_recall | fix_recall | key_event_recall |
|---|---|---|---|
| hybrid | 1.000 | 1.000 | 1.000 |
| hybrid+rerank | 1.000 | 1.000 | 1.000 |

Honest read: once an incident is in scope, the generalized timeline recovers its
true cause and fix for **all 40 incidents across all six archetypes** — not just
deploy/rollback but config reverts, dependency failovers, scale-ups, cert
renewals and migration reverts (`CHANGE_TYPES`/`REMEDIATION_TYPES`). This eval
isolates *synthesis*; the hard *retrieval* number is the table above.
Cross-encoder reranking is **neutral** here (both 1.0) — at benchmark scale with
isolable incidents it neither helps nor hurts cause/fix capture, which updates
the toy-scale M10a observation that rerank appeared to hurt completeness.

![root-cause completeness](results/rootcause_completeness.png)

### Root-cause (hard tier)

The `easy` benchmark tier saturated (hybrid and hybrid+rerank both 1.0/1.0), so it is
retained only as a fast smoke/regression baseline. The `hard` tier interposes a benign
decoy change between the true cause and the spike (near-duplicate vocab) plus
same-service distractor volume, so retrieval and cause selection must actually work.

Metrics over the keyword → hybrid → hybrid+rerank ladder, naive (last-before-spike) vs
score-aware (retrieval-rank × spike-proximity) selection, 40 hard-tier incidents
(`results/rootcause_eval.json`):

| arm | recall@k | accuracy (naive) | accuracy (score-aware) | MRR (score-aware) |
|---|---|---|---|---|
| keyword | 0.625 | 0.375 | 0.375 | 0.446 |
| hybrid | 0.575 | 0.425 | 0.400 | 0.483 |
| hybrid+rerank | 0.650 | 0.550 | 0.600 | 0.613 |

Honest reading: the score-aware selector reliably helps only on the **hybrid+rerank**
arm (0.55 → 0.60), where the cross-encoder gives it an informative ranking. On
**keyword** the rank is uninformative, so it ties naive (0.375). On plain **hybrid** it
is marginally *worse* than naive (0.400 vs 0.425 — a single incident): the first-stage
fused rank can seat a benign decoy above the true cause and mislead the selector, and
only reranking separates them cleanly enough to win. Cause accuracy still rises with arm
sophistication (naive 0.375 → 0.425 → 0.55; score-aware 0.375 → 0.40 → 0.60). recall@k
is non-monotonic (0.625 → 0.575 → 0.65) — plain hybrid's tighter top-k drops the true
cause more often than keyword's looser match, and reranking pulls it back into the cut.
These figures are byte-reproducible: a deterministic `chunk_id` tiebreak in the
retrieval SQL (same branch) removes the heap-order non-determinism that previously
inflated keyword recall to a spurious 1.0.

Real-data face validity: over the committed real status-feed incidents (symptom-only,
`make rootcause-facevalidity`), the cause selector abstains on **1/1 = 1.00** of
incidents — it does not fabricate a root cause when no change event is in evidence.
One honest nuance: the fixture incident's update text names a deploy in prose ("a bad
WAF rule deploy is the cause"), but its event type is `identified` (a status label, not
a change type), so the extractive selector correctly does **not** fabricate a
structured cause from prose — disciplined abstention, not a miss. This is face
validity, not accuracy: public status feeds carry no event-level cause labels, so the
labeled ladder above is synthetic. Event-level real root-cause labels require internal
deploy+incident+postmortem access no public API provides.

## M6 — retrieval quality + streaming-vs-batch (the differentiator)

> The retrieval table here is the original **toy-scale** measurement (6 queries,
> one incident), kept for history. It is superseded by the 160-query benchmark in
> M12 above. The streaming-vs-batch result below is unchanged and still current.

Reproduce: `make up && make eval` (needs `.[embed]` `.[eval]`). Deterministic —
fixed-seed corpus + MiniLM. Synthetic-data numbers are indicative, not a
real-world benchmark; the batch side of the staleness graph is a model computed
from a steady event stream at the generator's cadence (the comparison isolates
ingestion cadence, not the scripted incident's narrative timing).

**Retrieval quality** over 6 authored queries (mean, k=5), all-MiniLM-L6-v2:

| mode | recall@5 | precision@5 | MRR | nDCG@5 |
|---|---|---|---|---|
| keyword-only | 0.667 | 0.200 | 0.417 | 0.490 |
| vector-only | 0.667 | **0.233** | 0.389 | 0.481 |
| **hybrid** | **0.722** | 0.200 | **0.431** | **0.504** |

Honest read: **hybrid wins recall@5, MRR, and nDCG@5** — fusing the two arms
surfaces relevant events neither finds alone. It does **not** win precision@5:
vector-only is tightest at the very top (0.233), because fusion pulls in extra
keyword candidates that dilute precision while lifting recall. That trade-off is
the expected shape of reciprocal-rank fusion, reported rather than hidden. (The
keyword arm uses OR semantics — ANDing every word of a natural-language question
against terse events zeroes recall and is a strawman baseline.)

![retrieval quality](results/retrieval_quality.png)

**Streaming vs batch staleness** — mean data staleness **5.0s (streaming)** vs
**1778s (batch at a 3600s cadence)**: **~356× fresher**. At a real nightly
cadence (86400s) the gap is ~four orders of magnitude. Staleness = query-time
minus newest queryable event.

![streaming vs batch](results/streaming_vs_batch.png)

Resilience drills (worker kill/recovery, replay re-index, burst backpressure)
with evidence graphs: see [`DRILLS.md`](DRILLS.md).

## M4 — consumer-group scaling (embedder)

1,009 live events produced as an instantaneous burst into 3-partition topics,
time measured from burst start to all events queryable in pgvector
(`make scale-demo`).

**Re-run 2026-07-15, after producer batching** (`BufferedProducer` + batched
offset commits replaced the normalizer's per-message flushed produce), on the
bge embedder (the current 768-dim schema default):

| embedder instances | drain time | throughput | scaling |
|---|---|---|---|
| 1 | 38s | 26 ev/s | — |
| 3 | 12s | 84 ev/s | **3.2×** |

A stub-embedder run (5,009-event burst, model cost removed) measures the
non-embedding pipeline — generator → normalizer → DB upserts — at **834 ev/s**,
the stage that previously capped at ~100 ev/s.

Honest read: embedder scaling is now near-linear (3.2× with 3 workers) because
the normalizer no longer caps the pipeline. bge is a heavier model than the old
MiniLM (26 vs 67 ev/s per worker), so absolute throughput at 1 worker dropped
while headroom rose: the next ceiling is now ~8× further out. The original run
(2026-06-12, MiniLM, pre-batching) scaled only 67→100 ev/s (1.5×) — at 3
instances the single normalizer's delivery-checked produce-per-event was the
bottleneck. Scaling consumers moves bottlenecks; batching moved this one.

Reproduce: `make up && WORKERS=1 make scale-demo` (then WORKERS=3) — topics need
their 3 partitions, so start from `make up`, not a single-partition dev stack.

## M2 — event-to-queryable freshness (slice demo, real embedder)

p50 ≈ 2–4 s, p95 ≈ 6–8 s over 69 live events (`make slice`; printed by
`freshet.eval.freshness`). This measured streaming freshness is the floor used
for the M6 streaming-vs-batch comparison above.
