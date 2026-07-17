"""Evaluation harness entry point. Regenerates every committed headline number.

    make eval            # Postgres up; installs nothing — needs .[embed] .[eval]

Writes results/retrieval_metrics.json and two PNGs. Deterministic: a fixed-seed
corpus + the (deterministic) MiniLM embedder make retrieval metrics reproducible;
the staleness model is computed over a steady synthesized event stream at the
generator's cadence (see staleness_curves for why not the incident corpus)."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from freshet.common.db import connect
from freshet.eval import batch_baseline, metrics, modes
from freshet.eval.labeled import build_labeled_queries, relevant_event_ids
from freshet.generator.generator import build_benchmark
from freshet.pipeline.embedder import records_for_event, upsert_record
from freshet.pipeline.embedding import make_embedder

RESULTS_DIR = "results"
K = 5


def index_corpus(conn, embedder, corpus) -> None:
    conn.execute("DELETE FROM vector_records")
    now = datetime.now(timezone.utc)
    for ev in corpus:
        recs = records_for_event(ev, now=now)
        if not recs:
            continue
        vectors = embedder.encode([r.text for r in recs])
        for rec, vec in zip(recs, vectors):
            upsert_record(conn, rec, vec)


_EVAL_TAU_S = 1e12  # disable recency decay so the benchmark eval is deterministic


def score_modes(conn, embedder, corpus, queries) -> dict[str, dict[str, float]]:
    # ground truth is resolved against the SAME corpus that was indexed
    mode_fns = {
        "vector": lambda q: modes.vector_only_event_ids(conn, embedder, q, K),
        "keyword": lambda q: modes.keyword_only_event_ids(conn, q, K),
        "hybrid": lambda q: modes.hybrid_event_ids(conn, embedder, q, K, tau_s=_EVAL_TAU_S),
    }
    out: dict[str, dict[str, float]] = {}
    for mode, fn in mode_fns.items():
        agg = {f"recall@{K}": 0.0, f"precision@{K}": 0.0, "mrr": 0.0, f"ndcg@{K}": 0.0}
        for q in queries:
            gt = relevant_event_ids(q, corpus)
            ranked = fn(q.text)
            agg[f"recall@{K}"] += metrics.recall_at_k(ranked, gt, K)
            agg[f"precision@{K}"] += metrics.precision_at_k(ranked, gt, K)
            agg["mrr"] += metrics.reciprocal_rank(ranked, gt)
            agg[f"ndcg@{K}"] += metrics.ndcg_at_k(ranked, gt, K)
        out[mode] = {k: v / len(queries) for k, v in agg.items()}
    return out


def staleness_curves(
    streaming_freshness_s: float,
    batch_interval_s: float,
    spacing_s: float = 5.0,
    cycles: int = 4,
):
    """Data-staleness over time for streaming vs batch, over a steady event
    stream at the generator's real cadence (spacing_s) spanning `cycles` batch
    intervals. The comparison isolates ingestion cadence: the scripted incident
    corpus is unsuitable here because its postmortem event lands ~1h after the
    incident, an event-less gap that inflates *both* curves equally (nothing new
    to index) and obscures the streaming-vs-batch difference."""
    window = batch_interval_s * cycles
    event_ts = [i * spacing_s for i in range(int(window / spacing_s))]
    samples = [i * (window / 400) for i in range(401)]
    s_q = batch_baseline.streaming_queryable_at(event_ts, streaming_freshness_s)
    b_q = batch_baseline.batch_queryable_at(event_ts, batch_interval_s, t0=0.0)
    streaming = batch_baseline.staleness_series(event_ts, s_q, samples)
    batch = batch_baseline.staleness_series(event_ts, b_q, samples)
    return samples, streaming, batch


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet evaluation harness")
    p.add_argument("--embedder", choices=["stub", "bge"], default="bge")
    p.add_argument("--streaming-freshness-s", type=float, default=3.0,
                   help="measured streaming p50 freshness (see RESULTS.md)")
    p.add_argument("--batch-interval-s", type=float, default=3600.0,
                   help="modeled batch cadence (3600=hourly demo proxy for nightly)")
    p.add_argument("--out", default=RESULTS_DIR)
    a = p.parse_args()

    os.makedirs(a.out, exist_ok=True)
    embedder = make_embedder(a.embedder)
    conn = connect()
    try:
        corpus, truths = build_benchmark(seed=1, n_incidents=40)
        queries = build_labeled_queries(corpus, truths)
        index_corpus(conn, embedder, corpus)
        retrieval = score_modes(conn, embedder, corpus, queries)
    finally:
        conn.close()

    samples, streaming, batch = staleness_curves(
        a.streaming_freshness_s, a.batch_interval_s
    )
    streaming_clean = [s for s in streaming if s is not None]
    batch_clean = [b for b in batch if b is not None]
    summary = {
        "retrieval": retrieval,
        "k": K,
        "n_queries": len(queries),
        "streaming_freshness_s": a.streaming_freshness_s,
        "batch_interval_s": a.batch_interval_s,
        "mean_staleness_streaming_s": sum(streaming_clean) / len(streaming_clean),
        "mean_staleness_batch_s": sum(batch_clean) / len(batch_clean),
    }
    summary["staleness_ratio_batch_over_streaming"] = (
        summary["mean_staleness_batch_s"] / summary["mean_staleness_streaming_s"]
    )

    with open(os.path.join(a.out, "retrieval_metrics.json"), "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    # lazy: matplotlib is the .[eval] extra; importing run_eval for its helpers
    # (index_corpus, build_benchmark) must not require it
    from freshet.eval import plots
    plots.plot_retrieval_quality(retrieval, os.path.join(a.out, "retrieval_quality.png"))
    plots.plot_streaming_vs_batch(
        samples, streaming, batch,
        os.path.join(a.out, "streaming_vs_batch.png"),
        batch_interval_s=a.batch_interval_s,
    )

    best = max(retrieval, key=lambda m: retrieval[m][f"recall@{K}"])
    print(f"retrieval recall@{K}: " + ", ".join(
        f"{m}={retrieval[m][f'recall@{K}']:.3f}" for m in retrieval))
    print(f"best mode by recall@{K}: {best}")
    print(f"staleness batch/streaming ratio: "
          f"{summary['staleness_ratio_batch_over_streaming']:.0f}x")
    print(f"wrote {a.out}/retrieval_metrics.json + 2 PNGs")


if __name__ == "__main__":
    main()
