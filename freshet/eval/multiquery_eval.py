"""Key-gated multi-query vs single-query retrieval eval over the benchmark queries.
Skips cleanly (exit 0) without ANTHROPIC_API_KEY. Indicative + non-deterministic;
the committed JSON is one run.

Run (stack up, fresh DB): python -m freshet.eval.multiquery_eval
"""
from __future__ import annotations

import json
import os

RESULTS = "results/multiquery_eval.json"
_EVAL_TAU_S = 1e12


def aggregate(single: list[float], multi: list[float]) -> dict:
    n = len(single)
    s = sum(single) / n if n else 0.0
    m = sum(multi) / n if n else 0.0
    return {
        "single_recall@5": round(s, 3),
        "multi_recall@5": round(m, 3),
        "lift": round(m - s, 3),
        "n": n,
    }


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set — multi-query eval is key-gated, skipping.")
        return

    from freshet.api.multiquery import multi_query_event_ids
    from freshet.common.db import connect
    from freshet.eval import metrics, modes
    from freshet.eval.labeled import build_labeled_queries, relevant_event_ids
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()
    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    queries = build_labeled_queries(corpus, truths)
    index_corpus(conn, embedder, corpus)

    sample = queries[:20]  # cost control; first 20 labeled queries
    print(f"Multi-query eval: {len(sample)} queries (key-gated, indicative)")
    single_r, multi_r = [], []
    for q in sample:
        gt = relevant_event_ids(q, corpus)
        single_ids = modes.hybrid_event_ids(conn, embedder, q.text, 5, tau_s=_EVAL_TAU_S)
        multi_ids = multi_query_event_ids(conn, embedder, q.text, 5)
        single_r.append(metrics.recall_at_k(single_ids, gt, 5))
        multi_r.append(metrics.recall_at_k(multi_ids, gt, 5))

    result = {
        "config": aggregate(single_r, multi_r),
        "n_queries": len(sample),
        "note": "multi-query is indicative and non-deterministic",
    }
    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
