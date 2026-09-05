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

**Measured: n = 38, mean 101.82s, p50 90.19s, p95 197.65s**, ratio **24.39×**
against this run's derived hourly-batch arm (2483.33s). Scored across the
current run's unbroken 09:00:00 span (2026-09-04T19:57:01Z – 2026-09-05T04:57:00Z,
still running), zero child restarts. `n` counts individual updates, not
incidents — several updates in this run belong to the same incident thread.

The earlier number here (`n=33`, ratio 0.06 — streaming apparently 14x SLOWER than
hourly batch) was an artifact and has been deleted. Its cause is worth recording:
the filter `ts >= min(indexed_at)` excludes backfilled HISTORY but cannot tell a
slow pipeline from a stopped one. The pipeline had been down ~14 hours; every
update posted during the outage was indexed in the catch-up burst afterwards and
scored as ~9.8 hours of staleness. The measurement was of my own outage.

Uptime is now proven rather than assumed: the embedder writes a heartbeat, and
only the CURRENT unbroken run is scored (a gap over 5 minutes starts a new run).
A restart therefore resets the window instead of charging its backlog to the
pipeline's speed. With no live arrivals yet the report carries `status: not yet
measured` and no ratio at all, and `FRESHNESS_MIN_N` fails the run rather than
reporting a thin sample.

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

## Embedding audit remediation

Measured against a frozen snapshot of the live index (`freshet_embed_audit`,
12,155 chunks / 7,166 events / 42 providers, all `BAAI/bge-base-en-v1.5`) so
before/after numbers are comparable — the live index grows under a running
poller and moved 11,069 -> 12,155 chunks during the audit itself. Findings and
baselines: `docs/embedding-audit.md`.

### F2 — the abstention metric could not fail

`_main_live` excluded the query's own document from the ranking metrics via
`dedupe_events`, but the abstention count came from `hybrid_search`'s internal
hits, which still contained it. The live labels are verbatim indexed update
text, so the top similarity was a near-self-match: the reported "0 on-corpus
abstentions" was measuring "is this exact text in the index", which is trivially
yes. `exclude_event_id` now drops that document in SQL, so both arms, both
ranking metrics and the abstention decision see one candidate set.

| arm | recall@5 before | after | mrr before | after |
|---|---|---|---|---|
| hybrid | 0.455 | 0.455 | 0.321 | 0.326 |
| vector_only | 0.436 | 0.436 | 0.303 | 0.306 |
| keyword_only | 0.345 | 0.345 | 0.254 | 0.260 |

| abstention | before | after |
|---|---|---|
| on-corpus (of 55) | 0 | **4** |
| off-corpus (of 6) | 6 | 6 |

No `recall@5` moved. The small MRR shifts are the SQL-level exclusion changing
which rows fill the per-arm `LIMIT`. The corrected on-corpus figure of 4/55 is
the honest one, and it is the number F1's floor work is measured against.

### F1 — the abstention floor was calibrated on the wrong distribution

`MIN_SIMILARITY_BGE`'s comment cites a clean gap ("on-corpus >= 0.735 vs hardest
off-corpus 0.662"). That reproduces on the fixture corpus and inverts on the
live index, because bge's cosine space is anisotropic: random UNRELATED chunk
pairs average 0.594 and **12.2% of them clear the 0.70 floor**. The floor was
cutting a percentile, not a meaning.

Abstention now measures in the mean-centered space — subtract the stored
per-model centroid (`index_stats`) from both document and query, which `<=>`
then normalizes. Ranking is untouched and stays in raw cosine.

`make calibrate-abstention`, which refuses to propose a floor unless the
distributions actually separate, now reports both spaces:

| space | on-corpus min (answerable) | off-corpus max | verdict |
|---|---|---|---|
| raw | 0.632 | 0.687 | **OVERLAP** — no threshold separates them |
| centered | 0.453 | 0.434 | **gap exists**; proposes 0.443 (shipped: 0.44) |

That is the finding stated in its own terms: the tool that previously could not
justify any floor can justify one once the geometry is corrected.

| abstention (55 live labels) | raw @0.70 | centered @0.44 |
|---|---|---|
| false abstentions | 4 | **2** |
| off-corpus rejected | 6/6 | 6/6 |

Ranking is unchanged — hybrid `recall@5` 0.455, vector_only 0.436, keyword_only
0.345 across both, and the query-blind guard still reports `meaningful`.

A missing centroid is not an error: the column comes back NULL and abstention
falls back to the raw floor, so an un-refreshed index degrades to today's
behaviour rather than failing.

### F8a — the OR-swap inverted negation

The keyword arm swaps `&` for `|` in the parsed tsquery to buy recall. On a
negated query that changes meaning rather than breadth: `outage -maintenance`
parses as `'outag' & !'mainten'`, and the swap makes it `'outag' | !'mainten'` —
every row that merely LACKS "maintenance".

| query | correct (AND) | naive OR-swap |
|---|---|---|
| `outage -maintenance` | 163 rows (1%) | 10,708 rows (**88%**) |
| `database errors -scheduled` | 14 rows (0%) | 11,808 rows (**97%**) |

The arm degenerated to near-everything, `ts_rank` tied out across it, and 20
effectively arbitrary candidates entered RRF at full weight. Negated queries now
keep websearch's AND form; everything else keeps the swap.

Found by a parallel review session and reproduced here before acting on it.
Every existing keyword-arm test asserts on SQL *strings* against a fake
connection, which is why it survived — the bug is in what the tsquery means, not
in how the SQL reads. `tests/integration/test_keyword_negation.py` seeds a known
corpus and counts rows instead.

| arm | recall@5 before | after |
|---|---|---|
| hybrid | 0.455 | 0.455 |
| vector_only | 0.436 | 0.436 |
| keyword_only | 0.345 | 0.309 |

`keyword_only` drops by exactly 2/55 = 0.036, and 2 of the 55 label queries parse
as negated — the entire delta is those two losing accidental recall from matching
~90% of the index. Hybrid is unchanged, so fusion was already discarding the
noise. A lexical arm that answers the opposite of what was asked is a bug whether
or not the benchmark rewards it.

### F8b — ranking the keyword arm on cover density

`ts_rank` counts term frequency, which ties heavily across terse operational
updates. With OR semantics the candidate set is large, so which 20 rows survived
the `LIMIT` was effectively decided by the `chunk_id` tiebreak — deterministic,
as the old comment claimed, but an id hash rather than a relevance signal.
`ts_rank_cd` scores cover density (how close the matched terms sit), with
normalization flag 32 dividing by rank+1 so long chunks cannot win on term count.

| arm | recall@5 before | after | mrr before | after |
|---|---|---|---|---|
| hybrid | 0.455 | **0.473** | 0.328 | 0.313 |
| keyword_only | 0.309 | **0.364** | 0.232 | **0.251** |
| vector_only | 0.436 | 0.436 | 0.306 | 0.306 |

Kept: hybrid `recall@5` improves and the keyword arm improves on both metrics.
Read it with the sample size in mind — at n=55 a move of 0.018 is one query, so
the honest claim is "the arm ranks on something meaningful now and nothing got
worse at k=5", not a 4% improvement. Hybrid MRR slips by a similar single-query
margin. Abstention is unchanged at 2/55 and 6/6, and the query-blind guard still
reports `meaningful`.

**Query plan** (live snapshot, 12,155 chunks, query "why did the api return
errors for customers in europe"): sequential scan matching 6,668 rows, top-N
heapsort, 52 ms. The arm reads the whole table on every query. That is affordable
at this corpus size and is the thing to revisit before an ANN index, not after.

### F3 — candidate depth was NOT the bottleneck

The vector arm's recall curve says the evidence is there and the k=5 cut is
losing it: recall@1 0.218, @5 0.436, @10 0.527, @20 0.655, @50 0.764, @100 0.782.
About a third of the answers sit between rank 6 and 50, and `ARM_K = 20` caps
each arm below them. Raising it costs one larger `LIMIT` and no LLM tokens, so it
looked like the cheapest large lever available. It is not a lever at all:

| ARM_K | hybrid recall@5 | mrr | top1 | false abstentions |
|---|---|---|---|---|
| **20** | **0.473** | 0.313 | 0.236 | 2/55 |
| 30 | 0.455 | 0.319 | 0.236 | 2/55 |
| 50 | 0.436 | 0.309 | 0.236 | 3/55 |
| 80 | 0.455 | 0.312 | 0.236 | 3/55 |
| 120 | 0.455 | 0.312 | 0.236 | 3/55 |

Kept at 20; deeper is flat-to-worse and costs a false abstention.

The reason is RRF's own arithmetic. Fusion scores a document `1/(60 + rank)`, so
a hit at vector-rank 50 contributes 0.0092 against 0.0167 for rank 0 — it cannot
overtake five shallower documents on rank alone, no matter how strong its cosine.
Depth makes those documents *visible* to fusion without making them *winnable*.

So the finding is sharper than the audit stated: the evidence is retrievable, and
the thing discarding it is that RRF throws away magnitude. Recall@50 = 0.764 is
the ceiling a *score-aware* fusion could reach; it is not a ceiling depth alone
can move. `FRESHET_ARM_K` is left in place so the next person can re-sweep
against a different fusion without editing code.

Raising the *delivered* k (currently 6 in `freshet/autopilot/thread_agent.py`) is
the other half and is not free — it sends more chunks to the model, and the git
history already contains one fix for a 179-update incident sending 58k tokens
twice. It belongs against `freshet/rag/budget.py`'s caps, not in this change.

### F7 — chunk size: measured, and left at 400

`DEFAULT_MAX_CHARS = 400` sits against bge's 512-token window while chunks
average 34 tokens (p95 80, max 115) — about 7% of it. The cap buys no truncation
safety and fragments 40% of live events, so raising it looked free. It is not.

Fixture corpus (12 labels), `make chunk-sweep`:

| max_chars | chunks | multi-chunk events | recall@5 | mrr | top1 |
|---|---|---|---|---|---|
| **400** | 975 | 7.3% | 0.917 | 0.701 | 0.583 |
| 600 | 903 | 5.0% | 0.917 | 0.715 | 0.583 |
| 800 | 877 | 3.7% | 0.917 | **0.771** | **0.667** |
| 1200 | 852 | 1.3% | 0.917 | 0.715 | 0.583 |

`recall@5` is flat at every size, because at 7.3% multi-chunk the fixture corpus
can barely see this parameter. That is the finding: **the CI benchmark cannot
validate a chunking change.** So 800 was re-validated on live-shaped data —
each event's text reconstructed from its ordered chunks in the frozen snapshot,
re-chunked, re-embedded into a throwaway database, scored on the 55 live labels:

| max_chars | chunks | recall@5 | mrr | false abstentions |
|---|---|---|---|---|
| **400** | 12,166 | 0.473 | 0.312 | **1/55** |
| 800 | 8,484 | 0.473 | 0.322 | **5/55** |

Recall is identical and MRR moves by a single query, but false abstentions go up
5x. Larger chunks dilute the vector: more text per embedding means a weaker match
on the one sentence that states the cause, which drags the top similarity below
the floor. **Kept at 400**, on the live evidence rather than the fixture's.

Two things this does not settle: chunk *overlap* is still untested (the sweep
varies size, not overlap), and the reconstructed corpus joins chunks with a
single space, so it approximates the original text rather than reproducing it.

### Measured and rejected

Recorded because the next person will otherwise spend a day rediscovering them.

**Incident title on every chunk.** Flink prepends `"<name>: "` to the update text
and only chunk `_0` keeps it, leaving 4,842 of 11,907 live chunks (40.6%) with no
incident context. Re-embedding exactly those with the title restored: recall@5
0.436 -> 0.400, mrr 0.303 -> 0.284. The repeated title dominates short chunks and
crowds out the body. **Rejected.**

**`bge-reranker-base` over the vector arm's top-50.** recall@5 0.436 -> 0.382,
mrr 0.303 -> 0.240. **Rejected**, and the reason generalizes: every update inside
one incident is topically near-identical — "We are investigating elevated error
rates" and "caused by an expired certificate on the edge tier" are the same
subject in the same vocabulary. A relevance reranker ranks topical fit, and
topical fit does not discriminate here. The task is not similarity; it is "which
of these near-identical updates STATES a cause", a property of the sentence
rather than of its distance to the query.

**Deeper candidate pools.** See F3 — RRF's `1/(60+rank)` cannot promote a deep
hit past five shallow ones, so depth makes evidence visible to fusion without
making it winnable.

**Larger chunks.** See F7 — flat recall, 5x the false abstentions on live data.

**Corpus shape mismatch, which is why three of the four above needed live data
to reject.** Fixture corpus: 159 mean chars, 7.3% multi-chunk events, 13.7%
non-first chunks. Live index: 235, 40.7%, 40.6%. The title experiment looked free
on the fixture and cost 3.6 points of recall live. `retrieval_eval` now publishes
`corpus_shape` on every run so the drift is visible rather than assumed.

### CORRECTION — the audit corpus was 61% parser-bug duplicates

Measured 2026-09-02 16:25Z. The 12,155-chunk index every figure in this section
was taken on contained 7,435 amplified duplicates (openai 5,582, hashicorp 1,853)
from a source-adapter bug, since purged. Full accounting in
`docs/embedding-audit.md`. Two consequences:

- **F1's headline was inflated.** Unrelated pairs clearing the 0.70 floor: 12.2%
  on the amplified index, **3.3%** on the clean one. Near-duplicates raise the
  high tail, which is precisely what that statistic measures. The mean barely
  moved (0.594 -> 0.574), so the anisotropy is real; the magnitude was not.
- **The centered floor is confirmed, unchanged.** On clean data on-corpus min
  0.452 vs off-corpus max 0.435, and `calibrate_abstention` proposes 0.443
  against the shipped 0.44. The raw floor is now provably too high: the lowest
  answerable query scores 0.687, under the shipped 0.70.

**Second casualty, same cause.** The corpus-shape argument behind F6 and F7 also
rested on the duplicates. Live non-first chunks: 40.6% reported, **14.1%** clean,
against the fixture corpus's 13.7% — the purged records were long multi-chunk
documents, so they inflated fragmentation and mean length together. The CI corpus
is in fact a reasonable proxy for the clean index on chunk shape, so "the fixture
cannot validate a chunking change" is withdrawn. The chunk-size decision (leave
400) stands on its own evidence: 1/55 false abstentions against 5/55 at 800.

Every recall/MRR number in this section is superseded and needs re-running on a
settled clean index. On the current 8,966-chunk index: hybrid recall@5 0.345,
vector_only 0.345, keyword_only 0.255, abstention 0/55 and 6/6, guard
`meaningful`. That is a different corpus, not a regression.

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
