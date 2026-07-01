"""Key-gated agent vs single-shot baseline eval over a 12-incident sample.

Run (stack up, corpus indexed):
    python -m freshet.eval.agent_eval
Skips cleanly when ANTHROPIC_API_KEY is unset (exit 0).
"""
from __future__ import annotations

import json
import os

from freshet.api.retrieval import hybrid_search
from freshet.api.synthesis import build_timeline

RESULTS = "results/agent_eval.json"

# Same as rootcause._EVAL_TAU_S — disables recency decay for reproducible ranking
_EVAL_TAU_S = 1e12


def sample_incidents(truths: list, n_per_archetype: int = 2) -> list:
    """Return the first n_per_archetype incidents for each archetype, in order."""
    seen: dict[str, list] = {}
    for t in truths:
        bucket = seen.setdefault(t.archetype, [])
        if len(bucket) < n_per_archetype:
            bucket.append(t)
    result = []
    for bucket in seen.values():
        result.extend(bucket)
    return result


def aggregate(records: list[dict]) -> dict:
    """Compute cause_recall and fix_recall from per-incident hit records."""
    n = len(records)
    if n == 0:
        return {"cause_recall": 0.0, "fix_recall": 0.0, "n": 0}
    cause_recall = sum(1 for r in records if r.get("cause_hit")) / n
    fix_recall = sum(1 for r in records if r.get("fix_hit")) / n
    return {
        "cause_recall": round(cause_recall, 3),
        "fix_recall": round(fix_recall, 3),
        "n": n,
    }


def _single_shot(conn, embedder, truth) -> dict:
    """Whole-corpus single-shot baseline: hybrid search + extractive timeline."""
    q = f"what caused the {truth.service} incident and how was it resolved?"
    res = hybrid_search(conn, embedder, q, k=12, service=None,
                        min_similarity=0.0, reranker=None, tau_s=_EVAL_TAU_S)
    tl = build_timeline(res.hits)
    return {
        "cause_id": tl.cause.event_id if tl.cause else None,
        "fix_id": tl.fix.event_id if tl.fix else None,
    }


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set — agent eval is key-gated, skipping.")
        return

    from freshet.api.agent import investigate
    from freshet.common.db import connect
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    sample = sample_incidents(truths, n_per_archetype=2)

    index_corpus(conn, embedder, corpus)

    n = len(sample)
    print(f"Eval sample: {n} incidents, 2 per archetype (6 archetypes)")
    print(f"Estimated API calls: ~6 tool rounds × {n} incidents ≈ {6 * n} calls")

    ss_records: list[dict] = []
    agent_records: list[dict] = []

    for truth in sample:
        ss = _single_shot(conn, embedder, truth)
        ss_records.append({
            "cause_hit": ss["cause_id"] == truth.cause_id,
            "fix_hit": ss["fix_id"] == truth.fix_id,
        })

        inv = investigate(conn, embedder, truth.service)
        agent_records.append({
            "cause_hit": inv.cause_id == truth.cause_id,
            "fix_hit": inv.fix_id == truth.fix_id,
        })
        print(
            f"  {truth.incident_id} ({truth.archetype}): "
            f"ss=({ss['cause_id'] == truth.cause_id}/{ss['fix_id'] == truth.fix_id}) "
            f"agent=({inv.cause_id == truth.cause_id}/{inv.fix_id == truth.fix_id})"
        )

    ss_agg = aggregate(ss_records)
    ag_agg = aggregate(agent_records)
    result = {
        "configs": {
            "single-shot": ss_agg,
            "agent": ag_agg,
        },
        "lift": {
            "cause_recall": round(ag_agg["cause_recall"] - ss_agg["cause_recall"], 3),
            "fix_recall": round(ag_agg["fix_recall"] - ss_agg["fix_recall"], 3),
        },
        "n_incidents": n,
        "note": "agent runs are indicative and non-deterministic",
    }

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
