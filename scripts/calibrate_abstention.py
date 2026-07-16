"""Calibrate the abstention floor (min_similarity) for an embedder.

Measures the max top-k cosine similarity that hybrid_search sees for three
query populations against the committed benchmark corpus:

  on-corpus     — the eval's root-cause question for every benchmark incident;
                  retrieval must NOT abstain on these.
  ops-flavored  — on-domain questions about services/phenomena absent from the
                  corpus (hard negatives; abstaining is correct).
  unrelated     — off-domain questions (easy negatives; abstaining is correct).

Prints the distributions and a recommended floor: the midpoint of the gap
between the hardest negative and the weakest positive, if they separate.

Run (stack up):
    python scripts/calibrate_abstention.py [--embedder bge]
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from freshet.api.retrieval import hybrid_search  # noqa: E402
from freshet.common.db import connect  # noqa: E402
from freshet.eval.run_eval import index_corpus  # noqa: E402
from freshet.generator.generator import build_benchmark  # noqa: E402
from freshet.pipeline.embedding import make_embedder  # noqa: E402

# Same as the eval arms — recency-neutral so ranking is reproducible.
_EVAL_TAU_S = 1e12

# Hard negatives: ops vocabulary, but services/phenomena the corpus never saw.
OPS_FLAVORED = [
    "what caused the search-indexer incident and how was it resolved?",
    "what caused the image-cdn incident and how was it resolved?",
    "why is the ml-training-cluster running out of GPU memory?",
    "who rotated the TLS certificates on the edge proxy last week?",
    "why did the kafka consumer group for fraud-detection stall?",
    "is the postgres replica for analytics lagging behind primary?",
    "did the terraform apply for the vpc peering change succeed?",
    "why are DNS lookups for the payments gateway timing out?",
]

# Easy negatives: different domain entirely.
UNRELATED = [
    "how long should I roast a chicken per pound?",
    "what is the capital of Australia?",
    "who won the world cup in 2022?",
    "recommend a good science fiction novel",
    "how do I improve my tennis backhand?",
    "what are the health benefits of green tea?",
    "translate hello world into French",
    "what time is sunset in Reykjavik in June?",
]


def max_topk_similarity(conn, embedder, query: str, k: int = 5) -> float:
    res = hybrid_search(conn, embedder, query, k=k, service=None,
                        min_similarity=0.0, tau_s=_EVAL_TAU_S)
    return max((h.similarity for h in res.hits), default=0.0)


def describe(name: str, values: list[float]) -> None:
    values = sorted(values)
    print(f"  {name:<14} n={len(values):<3} min={values[0]:.3f} "
          f"p25={values[len(values) // 4]:.3f} median={statistics.median(values):.3f} "
          f"max={values[-1]:.3f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--embedder", default=os.environ.get("FRESHET_EMBEDDER", "bge"))
    ap.add_argument("--k", type=int, default=5)
    args = ap.parse_args()

    embedder = make_embedder(args.embedder)
    conn = connect()
    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    index_corpus(conn, embedder, corpus)

    on_corpus = [
        max_topk_similarity(
            conn, embedder,
            f"what caused the {t.service} incident and how was it resolved?",
            k=args.k)
        for t in truths
    ]
    ops = [max_topk_similarity(conn, embedder, q, k=args.k) for q in OPS_FLAVORED]
    unrelated = [max_topk_similarity(conn, embedder, q, k=args.k) for q in UNRELATED]

    current = float(getattr(embedder, "min_similarity", 0.0))
    print(f"\nEmbedder: {args.embedder} (current floor {current:.2f}), "
          f"max top-{args.k} similarity per query:")
    describe("on-corpus", on_corpus)
    describe("ops-flavored", ops)
    describe("unrelated", unrelated)

    weakest_pos = min(on_corpus)
    hardest_neg = max(ops + unrelated)
    print(f"\n  weakest positive {weakest_pos:.3f} vs hardest negative {hardest_neg:.3f}", end="")
    if hardest_neg < weakest_pos:
        mid = (weakest_pos + hardest_neg) / 2
        print(f" — separable; recommended floor ≈ {mid:.2f} (gap midpoint)")
    else:
        overlap = sum(1 for v in on_corpus if v <= hardest_neg)
        print(f" — OVERLAP: {overlap}/{len(on_corpus)} positives at or below the "
              f"hardest negative; no clean floor, pick by which error costs more")

    fn = sum(1 for v in on_corpus if v < current)
    tn = sum(1 for v in ops + unrelated if v < current)
    print(f"  at the current floor {current:.2f}: would abstain on {fn}/{len(on_corpus)} "
          f"on-corpus (want 0) and {tn}/{len(ops) + len(unrelated)} negatives "
          f"(want {len(ops) + len(unrelated)})")
    conn.close()


if __name__ == "__main__":
    main()
