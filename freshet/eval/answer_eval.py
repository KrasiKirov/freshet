"""Key-gated answer-quality eval: extractive timeline vs LLM narrative, scored by
the hand-rolled judge on faithfulness + answer-relevance over the M10a corpus.
Reproducing it needs a key + spend; keyless clones read the committed JSON. Without
a key it skips cleanly (exit 0)."""

from __future__ import annotations

import json
import os
from statistics import fmean

RESULTS = "results/answer_eval.json"
_CONFIGS = ("extractive", "narrative")


def aggregate(records: list[dict]) -> dict:
    """records: [{"config", "faithfulness", "answer_relevance"}]; per-config means."""
    configs = {}
    for cfg in _CONFIGS:
        rows = [r for r in records if r["config"] == cfg]
        if not rows:
            continue
        configs[cfg] = {
            "faithfulness": round(fmean(r["faithfulness"] for r in rows), 3),
            "answer_relevance": round(fmean(r["answer_relevance"] for r in rows), 3),
            "incidents": len(rows),
        }
    return {"configs": configs,
            "note": "LLM-judge scores are indicative and non-deterministic"}


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("skipped — set ANTHROPIC_API_KEY to run the answer-quality judge")
        return

    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline, synthesize_narrative
    from freshet.common.db import connect
    from freshet.eval import judge
    from freshet.eval.rootcause import _EVAL_TAU_S, _index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()
    events, truths = build_benchmark(seed=1, n_incidents=30)
    _index_corpus(conn, embedder, events)
    # sample one incident from each of the first 5 distinct archetypes (cost control)
    sample, seen = [], set()
    for t in truths:
        if t.archetype not in seen:
            seen.add(t.archetype)
            sample.append(t)
        if len(sample) == 5:
            break
    services = {t.incident_id: t.service for t in sample}

    records: list[dict] = []
    for service in services.values():
        q = f"what caused the {service} incident and how was it resolved?"
        res = hybrid_search(conn, embedder, q, k=8, service=service,
                            min_similarity=0.0, tau_s=_EVAL_TAU_S)
        tl = build_timeline(res.hits)
        answers = {"extractive": tl.render(), "narrative": synthesize_narrative(tl)}
        for cfg, answer in answers.items():
            try:
                records.append({
                    "config": cfg,
                    "faithfulness": judge.judge_faithfulness(answer, res.hits),
                    "answer_relevance": judge.judge_answer_relevance(answer, q),
                })
            except ValueError as exc:
                print(f"  (skipped {cfg} for {service}: {exc})")

    result = aggregate(records)
    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
