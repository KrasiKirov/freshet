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


def _captured_for_config(conn, embedder, ground_truth, services_by_incident, reranker):
    """For each incident, retrieve + build a timeline and collect the surfaced ids."""
    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline

    captured: dict[str, set[str]] = {}
    for iid, service in services_by_incident.items():
        q = f"what caused the {service} incident and how was it resolved?"
        # Service-scoped (k=12), mirroring the product's root-cause path
        # (run_rootcause_demo). This isolates the *synthesis* question — given an
        # incident in scope, does the generalized timeline recover its true cause
        # and fix across all six archetypes? — while run_eval carries the hard
        # whole-corpus retrieval number.
        res = hybrid_search(conn, embedder, q, k=12, service=service,
                            min_similarity=0.0, reranker=reranker, tau_s=_EVAL_TAU_S)
        tl = build_timeline(res.hits)
        ids = {e.hit.event_id for e in tl.entries}
        if tl.cause:
            ids.add(tl.cause.event_id)
        if tl.fix:
            ids.add(tl.fix.event_id)
        captured[iid] = ids
    return captured


def _index_corpus(conn, embedder, events) -> None:
    """Self-contained: (re)index the in-memory corpus into vector_records so the
    eval reproduces from a clean `make up` without a separate streaming step."""
    from freshet.pipeline.embedder import records_for_event, upsert_record

    conn.execute("DELETE FROM vector_records")
    for ev in events:
        for rec in records_for_event(ev):
            [vec] = embedder.encode([rec.text])
            upsert_record(conn, rec, vec)


def main() -> None:
    from freshet.common.db import connect
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder
    from freshet.api.rerank import CrossEncoderReranker

    # minilm (real semantic retrieval) is the meaningful base for a root-cause task;
    # still keyless (model downloaded once). Override with FRESHET_EMBEDDER=stub.
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()
    events, truths = build_benchmark(seed=1, n_incidents=40)
    _index_corpus(conn, embedder, events)
    gt = {t.incident_id: (t.cause_id, t.fix_id) for t in truths}
    services = {t.incident_id: t.service for t in truths}

    configs = {"hybrid": None, "hybrid+rerank": CrossEncoderReranker()}
    result = {"configs": {}}
    for label, reranker in configs.items():
        captured = _captured_for_config(conn, embedder, gt, services, reranker)
        result["configs"][label] = completeness(gt, captured)

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    _plot(result["configs"])
    conn.close()


def _plot(configs) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("(matplotlib not installed; skipping plot — pip install -e '.[eval]')")
        return
    labels = list(configs)
    metrics = ["cause_recall", "fix_recall", "key_event_recall"]
    import numpy as np
    x = np.arange(len(metrics))
    width = 0.8 / max(len(labels), 1)
    fig, ax = plt.subplots(figsize=(6, 4))
    for i, label in enumerate(labels):
        ax.bar(x + i * width, [configs[label][m] for m in metrics], width, label=label)
    ax.set_xticks(x + width * (len(labels) - 1) / 2)
    ax.set_xticklabels(metrics, rotation=15)
    ax.set_ylim(0, 1)
    ax.set_ylabel("recall")
    ax.set_title("Root-cause completeness: hybrid vs hybrid+rerank")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOT, dpi=120)
    print(f"wrote {PLOT}")


if __name__ == "__main__":
    main()
