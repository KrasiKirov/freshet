# Embedding subsystem audit

Measured read-only against the running index on 2026-08-22. Index: 11,907
chunks / 6,986 events / 1,251 incidents / 42 providers, all
`BAAI/bge-base-en-v1.5`. Labels: `freshet/eval/fixtures/labels_live.json`,
n=55, self-document excluded unless stated.

Ranking figures reproduce the vector arm exactly — exact cosine over unit
vectors is what `1 - (embedding <=> q)` computes with no ANN index present.

## Baselines

| quantity | value |
|---|---|
| vector-arm recall@1 / @5 / @10 / @20 / @50 / @100 | 0.218 / 0.436 / 0.527 / 0.655 / 0.764 / 0.782 |
| on-corpus max-sim range | 0.562 – 0.961 (median 0.835) |
| off-corpus max-sim range (n=6) | 0.485 – 0.687 |
| random unrelated chunk pairs, mean cosine | 0.594 |
| ...scoring >= 0.60 / 0.65 / **0.70** / 0.75 / 0.80 | 34.3% / 19.6% / **12.2%** / 9.2% / 6.9% |
| false abstentions, raw @ 0.70 | 4/55 (off-corpus 6/6 rejected) |
| false abstentions, centered @ 0.44 | 2/55 (off-corpus 6/6 rejected) |
| non-first chunks (no incident title) | 4,842 / 11,907 = 40.6% |
| chunk tokens vs bge's 512 window | mean 34, p95 80, max 115 |
| `vec_literal` size | 16,285 bytes/vector (float32 binary: 3,072) |
| encode throughput, 2 torch threads | batch=1: 107/s, batch=4: 311/s, batch=32: 507/s |
| marker-bearing events | 392 / 7,065 = 5.5% |

## Findings

**F1 — the abstention floor is calibrated on the wrong distribution.**
`MIN_SIMILARITY_BGE`'s comment cites "on-corpus >= 0.735 vs hardest off-corpus
0.662; 0.7 is the gap midpoint". That reproduces on the fixture corpus (0.822 vs
0.661). On the live index it inverts: on-corpus reaches down to 0.562 while
off-corpus reaches up to 0.687. The mechanism is anisotropy — 12.2% of random
unrelated chunk pairs clear 0.70, so the floor cuts a percentile, not a meaning.
`calibrate_abstention.py` already found the overlap and correctly declined to
move the floor; this is the mechanism behind that result.
Files: `freshet/pipeline/embedding.py`, `freshet/rag/retrieval.py`.

**F2 — the live abstention metric cannot fail.** `_main_live` excludes
`query_event_id` from the ranking metrics via `dedupe_events`, but the abstention
count comes from `hybrid_search`'s internal hits, which still contain that
document. Live queries are verbatim indexed text, so the top similarity is a
near-self-match. Reported: 0/55. Self-document excluded: 4/55.
Files: `freshet/eval/retrieval_eval.py`, `freshet/rag/retrieval.py`.

**F3 — ranking, not recall, is the bottleneck.** recall@5 0.436 against
recall@20 0.655 and recall@50 0.764. About a third of the answers the system
already retrieves sit between rank 6 and 50. `ARM_K = 20` caps each arm below
where they live, so fusion never sees them.
File: `freshet/rag/retrieval.py`.

**F4 — a generic cross-encoder reranker makes it worse.** See "Measured and
rejected".

**F5 — a cause-salience prior works, but its magnitude is untrustworthy.**
An additive boost for chunks containing one of the nine `_CAUSE_MARKERS`
phrases, over the vector arm's top-50:

| ranker | recall@5 | mrr |
|---|---|---|
| vector arm (today) | 0.436 | 0.303 |
| + marker prior, w=0.05 | 0.564 | 0.423 |
| + marker prior, w=0.10 | 0.655 | 0.469 |
| + marker prior, w=0.20 | 0.691 | 0.538 |
| + incident expansion | 0.564 | 0.441 |

**Caveat, and it is the whole point:** `label_live.py:candidates()` shortlists by
cause marker before the judge sees anything, so all 86 labeled cause updates
contain a marker by construction. The benchmark cannot penalize the prior for
the cause statements it misses. The obvious exploit check is clean — a
query-blind "marker + recency" ranker scores 0.000 — so this is a real prior
rather than a benchmark hack, but the magnitude is not evidence.

**F6 — 40.6% of live chunks carry no incident title, and restoring it hurts.**
See "Measured and rejected". The actionable half is the corpus-shape gap.

**F7 — chunking is tuned for a window it barely uses.** `DEFAULT_MAX_CHARS = 400`
against bge's 512-token window; chunks average 34 tokens, p95 80, max 115 — about
7% of the window. The cap buys no truncation safety while fragmenting 40% of live
events, with no overlap to carry context across a boundary. Never swept.
File: `freshet/pipeline/chunking.py`.

**F8 — OR-tsquery makes the keyword arm's top-20 near-arbitrary.** `_OR_TSQUERY`
swaps `&` for `|` to buy recall (sound). The consequence: a large candidate set,
`ts_rank` ties heavily across terse operational text, and the tiebreak is
`chunk_id` — deterministic, but an id hash. Separately, that arm computes
`1 - (embedding <=> qvec)` in its select list over every OR-matching row; worth
one `EXPLAIN ANALYZE`.
File: `freshet/rag/retrieval.py`.

**F9 — a blank-text update never creates its incident row.** `handle()` returns
on `if not records` before reaching `ensure_incident`. Without that row,
autopilot's claim UPDATE matches nothing and the incident is silently never
briefed. Narrow (text is `"<name>: <body>"`) but silent.
File: `freshet/pipeline/embedder.py`.

**F10 — `min_similarity` is load-bearing but not on the Protocol.** Retrieval
reaches for it via `getattr(..., DEFAULT_MIN_SIMILARITY)` and `make_embedder`
needs `# type: ignore[misc]`. An embedder that omits it inherits the MiniLM floor
of 0.3 — which, given that unrelated pairs average 0.594, means "never abstain".
There is also no dimension guard at the encode boundary; a wrong-dim vector fails
inside psycopg as an infrastructure error rather than the config error it is.
Files: `freshet/pipeline/embedding.py`, `freshet/pipeline/embedder.py`.

**F11 — vectors travel as decimal text; encoding never batches.** `str(float)`
emits 17 significant digits for float32 values: 16,285 bytes per vector, ~194 MB
across a full re-index, and the query path pays it twice. `encode` is called once
per Kafka message (1-3 chunks), so the model runs at a fifth of its throughput
during a catch-up burst.
File: `freshet/pipeline/embedding.py`.

## Measured and rejected

**Incident title on every chunk.** Flink prepends `"<name>: "` to the update
text and only chunk `_0` keeps it, leaving 4,842/11,907 live chunks (40.6%) with
no incident context. Re-embedding exactly those with the title restored:
recall@5 0.436 -> 0.400, mrr 0.303 -> 0.284. The repeated title dominates short
chunks and crowds out the body. **Rejected.**

**`bge-reranker-base` over the vector arm's top-50.** recall@5 0.436 -> 0.382,
mrr 0.303 -> 0.240. **Rejected.** Every update inside one incident is topically
near-identical — "We are investigating elevated error rates" and "caused by an
expired certificate on the edge tier" are the same subject in the same
vocabulary. A relevance reranker ranks topical fit, and topical fit does not
discriminate here. The retrieval task is not similarity; it is "which of these
near-identical updates states a cause", a property of the sentence rather than of
its distance to the query. That is what motivates F5.

**Corpus shape mismatch.** Fixture corpus: 159 mean chars, 7.3% multi-chunk
events, 13.7% non-first chunks. Live index: 235, 40.7%, 40.6%. The title
experiment looked free on the fixture and cost 3.6 points of recall live. Any
embedding or chunking change must be validated on the live labels, not only in CI.

## What is already right

- `chunk_id`-derived idempotency plus the orphan-chunk DELETE. Re-embedding a
  shorter text is the case most pipelines get wrong, and it fails silently.
- The `model` provenance column and `check_index_model` raising rather than
  warning. A mixed-model index is invisible in the scores; everything just
  abstains.
- Production cannot reach `StubEmbedder` (`--embedder choices=["bge"]`).
- The keyword arm reporting real cosine instead of 0.0 — abstention keys off
  cosine, so a lexical-only hit defaulting to zero read as "no evidence".
- `calibrate_abstention.py` refusing to write the floor. F1 exists only because
  that refusal preserved the evidence.
- The `blind_recent` gameability guard. It scores 0.000 and looks like dead
  weight until F5 turns up.
