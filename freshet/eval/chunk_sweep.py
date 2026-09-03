"""Sweep the chunk-size cap against the labeled corpus.

DEFAULT_MAX_CHARS has never been swept. Measured against bge's 512-token
window, chunks use about 7% of it — mean 34 tokens, p95 80, max 115 — so the
cap buys no truncation safety while fragmenting 40% of live events, with no
overlap to carry context across a boundary.

Larger chunks also DILUTE similarity: more text per vector means a weaker match
on the one sentence that matters. That is why this is a sweep and not a change.

Also prints the corpus SHAPE, because the fixture corpus is not shaped like the
live index (159 vs 235 mean chars; 7.3% vs 40.7% multi-chunk events) and an
embedding decision validated on the wrong shape is not validated.

Run: make chunk-sweep
"""

from __future__ import annotations

import json
import os
import pathlib
import statistics

RESULTS = pathlib.Path("results/chunk_sweep.json")
SIZES = (400, 600, 800, 1200)


def corpus_shape(per_event_chunks: list[list[str]]) -> dict:
    """Distribution facts about a chunking, for comparing two corpora."""
    flat = [c for chunks in per_event_chunks for c in chunks]
    lengths = sorted(len(c) for c in flat) or [0]
    multi = sum(1 for chunks in per_event_chunks if len(chunks) > 1)
    non_first = sum(max(0, len(chunks) - 1) for chunks in per_event_chunks)
    return {
        "n_events": len(per_event_chunks),
        "n_chunks": len(flat),
        "mean_chars": round(statistics.mean(lengths), 1),
        "median_chars": int(statistics.median(lengths)),
        "p95_chars": lengths[int(0.95 * (len(lengths) - 1))],
        "max_chars_seen": lengths[-1],
        "multi_chunk_events": multi,
        "multi_chunk_frac": round(multi / len(per_event_chunks), 3) if per_event_chunks else 0.0,
        "non_first_frac": round(non_first / len(flat), 3) if flat else 0.0,
    }


def main() -> None:
    import psycopg

    from freshet.eval.retrieval_eval import (
        K,
        aggregate,
        cause_event_ids,
        dedupe_events,
        ensure_eval_db,
        load_corpus,
        load_labels,
        score_one,
    )
    from freshet.pipeline import chunking
    from freshet.pipeline.embedder import records_for_event, upsert_record
    from freshet.pipeline.embedding import make_embedder
    from freshet.pipeline.index_stats import clear_cache, refresh_centroid
    from freshet.rag.retrieval import hybrid_search

    events = load_corpus()
    labels = load_labels()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    dsn = ensure_eval_db()
    out: dict[str, dict] = {}

    for size in SIZES:
        saved = chunking.DEFAULT_MAX_CHARS
        chunking.DEFAULT_MAX_CHARS = size
        try:
            shape = corpus_shape([chunking.chunk_text(e.text, size) for e in events])
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("TRUNCATE vector_records")
                records = [r for ev in events for r in records_for_event(ev)]
                for i in range(0, len(records), 64):
                    batch = records[i:i + 64]
                    vectors = embedder.encode([r.text for r in batch])
                    for rec, vec in zip(batch, vectors, strict=True):
                        upsert_record(conn, rec, vec, getattr(embedder, "name", None))
                refresh_centroid(conn, getattr(embedder, "name", "") or "")
                clear_cache()
                scored, abstained = [], 0
                for entry in labels["labeled"]:
                    r = hybrid_search(conn, embedder, entry["query"], k=K)
                    abstained += bool(r.abstained)
                    scored.append(score_one(dedupe_events(r.hits), cause_event_ids(entry)))
            out[str(size)] = {"shape": shape, "hybrid": aggregate(scored),
                              "on_corpus_abstained": abstained}
            print(f"max_chars={size}: chunks={shape['n_chunks']} "
                  f"multi={shape['multi_chunk_frac']:.1%} "
                  f"hybrid={out[str(size)]['hybrid']} "
                  f"abstained={abstained}/{len(labels['labeled'])}", flush=True)
        finally:
            chunking.DEFAULT_MAX_CHARS = saved

    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(out, indent=2) + "\n")


if __name__ == "__main__":
    main()
