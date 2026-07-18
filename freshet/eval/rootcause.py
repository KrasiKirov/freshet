"""Keyless completeness eval for root-cause synthesis over the benchmark corpus.
Each of the 40 incidents spans six archetypes, so its true cause and fix are not
always a deploy/rollback — they range over config changes, dependency failures,
memory leaks, cert expiries and migrations (CHANGE_TYPES / REMEDIATION_TYPES). We
measure whether the generalized timeline (built from service-scoped retrieved
hits, mirroring the product's root-cause path) recovered them, for hybrid vs
hybrid+rerank. This isolates synthesis quality ('given the incident in scope, did
we surface its true cause/fix across every archetype?'); run_eval carries the
hard whole-corpus retrieval number.

Run (stack up, corpus indexed): python -m freshet.eval.rootcause
"""

from __future__ import annotations

import json
import os
from typing import Any

from freshet.api.retrieval import RetrievedHit, keyword_sql

RESULTS = "results/rootcause_eval.json"
PLOT = "results/rootcause_completeness.png"

# Disable recency decay for the eval: the corpus uses static historical timestamps,
# so the default decay would underflow every score to 0.0 and the top-k order would
# fall back to non-deterministic SQL tie-order. The eval ranks by retrieval
# relevance, not freshness, so a ~infinite tau makes recency_weight ~= 1 and the
# RRF-driven ranking reproducible run-to-run.
_EVAL_TAU_S = 1e12


def completeness(ground_truth: dict[str, tuple[str, str]],
                 captured: dict[str, set[str]]) -> dict[str, float]:
    """ground_truth[incident] = (cause_id, fix_id); captured[incident] = surfaced ids."""
    n = len(ground_truth)
    if n == 0:
        return {"cause_recall": 0.0, "fix_recall": 0.0, "key_event_recall": 0.0, "incidents": 0}
    cause = sum(1 for iid, (c, _f) in ground_truth.items() if c in captured.get(iid, set()))
    fix = sum(1 for iid, (_c, f) in ground_truth.items() if f in captured.get(iid, set()))
    cause_recall, fix_recall = cause / n, fix / n
    return {
        "cause_recall": round(cause_recall, 3),
        "fix_recall": round(fix_recall, 3),
        "key_event_recall": round((cause_recall + fix_recall) / 2, 3),
        "incidents": n,
    }


def cause_accuracy(ground_truth: dict[str, str], selected: dict[str, str | None]) -> float:
    n = len(ground_truth)
    if n == 0:
        return 0.0
    hit = sum(1 for iid, cid in ground_truth.items() if selected.get(iid) == cid)
    return round(hit / n, 3)


def mrr(ground_truth: dict[str, str], ranked: dict[str, list[str]]) -> float:
    n = len(ground_truth)
    if n == 0:
        return 0.0
    total = 0.0
    for iid, cid in ground_truth.items():
        ids = ranked.get(iid, [])
        if cid in ids:
            total += 1.0 / (ids.index(cid) + 1)
    return round(total / n, 3)


def recall_at_k(ground_truth: dict[str, str], retrieved: dict[str, set[str]]) -> float:
    n = len(ground_truth)
    if n == 0:
        return 0.0
    hit = sum(1 for iid, cid in ground_truth.items() if cid in retrieved.get(iid, set()))
    return round(hit / n, 3)


def keyword_only_hits(conn, question: str, k: int, service=None) -> list[RetrievedHit]:
    """Keyword-only retrieval arm as RetrievedHits (reusing the production keyword_sql),
    ranked by full-text rank. similarity=0.0 (no vector arm); score is rank-based so
    list order reflects keyword relevance."""
    params = {"q": question, "k": 30}
    if service is not None:
        params["service"] = service
    rows = conn.execute(keyword_sql(service, None), params).fetchall()
    hits: list[RetrievedHit] = []
    seen: set[str] = set()
    for r in rows:
        if r[1] in seen:
            continue
        seen.add(r[1])
        hits.append(RetrievedHit(chunk_id=r[0], event_id=r[1], service=r[2], ts=r[3],
                                 indexed_at=r[4], source=r[5], text=r[6], type=r[7],
                                 similarity=0.0, score=1.0 / (len(hits) + 1)))
        if len(hits) >= k:
            break
    return hits


def _index_corpus(conn, embedder, events) -> None:
    """Self-contained: (re)index the in-memory corpus into vector_records so the
    eval reproduces from a clean `make up` without a separate streaming step."""
    from freshet.pipeline.embedder import records_for_event, upsert_record

    conn.execute("DELETE FROM vector_records")
    for ev in events:
        for rec in records_for_event(ev):
            [vec] = embedder.encode([rec.text])
            upsert_record(conn, rec, vec)


def _hits_for_arm(conn, embedder, service, arm, reranker):
    q = f"what caused the {service} incident and how was it resolved?"
    if arm == "keyword":
        return keyword_only_hits(conn, q, k=12, service=service)
    from freshet.api.retrieval import hybrid_search
    res = hybrid_search(conn, embedder, q, k=12, service=service,
                        min_similarity=0.0, reranker=reranker, tau_s=_EVAL_TAU_S)
    return res.hits


def _naive_cause(hits):
    """Baseline selector: last change (by ts) at/before the first spike."""
    from freshet.api.synthesis import _CAUSE_TYPES, _role
    focus = sorted(hits, key=lambda h: h.ts)
    spike_ts = next((h.ts for h in focus if _role(h) == "spike"), None)
    changes = [h for h in focus if h.type in _CAUSE_TYPES
               and (spike_ts is None or h.ts <= spike_ts)]
    return changes[-1].event_id if changes else None


def _ranked_change_ids(hits):
    """Candidate change ids ordered by the score-aware selector's preference (for MRR)."""
    from freshet.api.synthesis import _CAUSE_TYPES, _role, _select_cause
    focus = sorted(hits, key=lambda h: h.ts)
    spike_ts = next((h.ts for h in focus if _role(h) == "spike"), None)
    changes = [h for h in focus if h.type in _CAUSE_TYPES
               and (spike_ts is None or h.ts <= spike_ts)]
    ordered = []
    pool = list(changes)
    while pool:
        pick = _select_cause(pool, hits, spike_ts)
        ordered.append(pick.event_id)
        pool = [h for h in pool if h.event_id != pick.event_id]
    return ordered


def main() -> None:
    from freshet.api.rerank import CrossEncoderReranker
    from freshet.api.synthesis import build_timeline
    from freshet.common.db import connect
    from freshet.generator.generator import build_hard_benchmark
    from freshet.pipeline.embedding import make_embedder

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()
    events, truths = build_hard_benchmark(seed=1, n_incidents=40)
    _index_corpus(conn, embedder, events)
    gt_cause = {t.incident_id: t.cause_id for t in truths}
    services = {t.incident_id: t.service for t in truths}

    arms = {"keyword": None, "hybrid": None, "hybrid+rerank": CrossEncoderReranker()}
    result: dict[str, Any] = {"tier": "hard", "n_incidents": len(truths), "ladder": {}}
    for arm, reranker in arms.items():
        hits_by_inc = {iid: _hits_for_arm(conn, embedder, svc, arm, reranker)
                       for iid, svc in services.items()}
        retrieved = {iid: {h.event_id for h in hs} for iid, hs in hits_by_inc.items()}
        naive_sel = {iid: _naive_cause(hs) for iid, hs in hits_by_inc.items()}
        aware_sel = {iid: (tl.cause.event_id if (tl := build_timeline(hs)).cause
                           else None) for iid, hs in hits_by_inc.items()}
        ranked = {iid: _ranked_change_ids(hs) for iid, hs in hits_by_inc.items()}
        result["ladder"][arm] = {
            "recall_at_k": recall_at_k(gt_cause, retrieved),
            "accuracy_naive": cause_accuracy(gt_cause, naive_sel),
            "accuracy_score_aware": cause_accuracy(gt_cause, aware_sel),
            "mrr_score_aware": mrr(gt_cause, ranked),
        }

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    _plot_ladder(result["ladder"])
    conn.close()


def _plot_ladder(ladder) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("(matplotlib not installed; skipping plot — pip install -e '.[eval]')")
        return
    arms = list(ladder)
    x = np.arange(len(arms))
    width = 0.38
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(x - width / 2, [ladder[a]["accuracy_naive"] for a in arms], width,
           label="naive selector")
    ax.bar(x + width / 2, [ladder[a]["accuracy_score_aware"] for a in arms], width,
           label="score-aware selector")
    ax.set_xticks(x)
    ax.set_xticklabels(arms, rotation=10)
    ax.set_ylim(0, 1)
    ax.set_ylabel("cause accuracy@1")
    ax.set_title("Root-cause (hard tier): naive vs score-aware, per arm")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOT, dpi=120)
    print(f"wrote {PLOT}")


if __name__ == "__main__":
    main()
