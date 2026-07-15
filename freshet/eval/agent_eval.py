"""Agent vs single-shot vs fixed-two-step eval over a 12-incident sample.

Three arms, all whole-corpus (no service hint):
  1. single-shot  — one hybrid search + extractive timeline (keyless, deterministic)
  2. fixed-two-step — the ABLATION: the same temporal lookup the agent uses
     (`events_around`), driven by a deterministic two-step pipeline with no LLM.
     If this matches the agent, the win is the retrieval capability, not agency.
  3. agent — the tool-calling LLM loop (key-gated, non-deterministic)

Run (stack up, corpus indexed):
    python -m freshet.eval.agent_eval
Keyless runs score arms 1–2 only; with ANTHROPIC_API_KEY set, all three.
"""
from __future__ import annotations

import json
import os

from freshet.api.retrieval import events_around, hybrid_search
from freshet.api.synthesis import _CAUSE_TYPES, _ROLE_BY_TYPE, build_timeline
from freshet.common.schemas import REMEDIATION_TYPES

RESULTS = "results/agent_eval.json"

# Event types synthesis treats as the incident's symptom ("spike" role)
_SPIKE_TYPES = frozenset(t for t, r in _ROLE_BY_TYPE.items() if r == "spike")

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


def _fixed_two_step(conn, embedder, truth) -> dict:
    """Ablation: same temporal tool as the agent, zero LLM. Step 1 is the
    identical whole-corpus search the single-shot baseline runs; step 2 anchors
    on the top spike-role hit and calls the non-semantic temporal lookup
    (`events_around`), then picks cause/fix by event type — the deterministic
    version of exactly what the agent does with `get_events_around`."""
    q = f"what caused the {truth.service} incident and how was it resolved?"
    res = hybrid_search(conn, embedder, q, k=12, service=None,
                        min_similarity=0.0, reranker=None, tau_s=_EVAL_TAU_S)
    spike = next((h for h in res.hits if h.type in _SPIKE_TYPES), None)
    if spike is None:
        return {"cause_id": None, "fix_id": None}
    neighbors = events_around(conn, spike.service, spike.ts, window_s=1800.0)
    cause = max((n for n in neighbors
                 if n.type in _CAUSE_TYPES and n.ts <= spike.ts),
                key=lambda n: n.ts, default=None)
    fix = next((n for n in neighbors
                if n.type in REMEDIATION_TYPES and n.ts >= spike.ts), None)
    return {
        "cause_id": cause.event_id if cause else None,
        "fix_id": fix.event_id if fix else None,
    }


def main() -> None:
    keyed = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if not keyed:
        print("ANTHROPIC_API_KEY not set — running the keyless arms only "
              "(single-shot + fixed-two-step ablation); the agent arm is skipped.")

    from freshet.common.db import connect
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder
    if keyed:
        from freshet.api.agent import investigate

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    sample = sample_incidents(truths, n_per_archetype=2)

    index_corpus(conn, embedder, corpus)

    n = len(sample)
    print(f"Eval sample: {n} incidents, 2 per archetype (6 archetypes)")
    if keyed:
        print(f"Estimated API calls: ~6 tool rounds × {n} incidents ≈ {6 * n} calls")

    ss_records: list[dict] = []
    fx_records: list[dict] = []
    agent_records: list[dict] = []

    for truth in sample:
        ss = _single_shot(conn, embedder, truth)
        ss_records.append({
            "cause_hit": ss["cause_id"] == truth.cause_id,
            "fix_hit": ss["fix_id"] == truth.fix_id,
        })
        fx = _fixed_two_step(conn, embedder, truth)
        fx_records.append({
            "cause_hit": fx["cause_id"] == truth.cause_id,
            "fix_hit": fx["fix_id"] == truth.fix_id,
        })
        line = (
            f"  {truth.incident_id} ({truth.archetype}): "
            f"ss=({ss['cause_id'] == truth.cause_id}/{ss['fix_id'] == truth.fix_id}) "
            f"fixed=({fx['cause_id'] == truth.cause_id}/{fx['fix_id'] == truth.fix_id})"
        )
        if keyed:
            inv = investigate(conn, embedder, truth.service)
            agent_records.append({
                "cause_hit": inv.cause_id == truth.cause_id,
                "fix_hit": inv.fix_id == truth.fix_id,
            })
            line += f" agent=({inv.cause_id == truth.cause_id}/{inv.fix_id == truth.fix_id})"
        print(line)

    ss_agg = aggregate(ss_records)
    fx_agg = aggregate(fx_records)
    configs = {
        "single-shot": ss_agg,
        "fixed-two-step": fx_agg,
    }
    lift = {
        "fixed_vs_single_cause_recall": round(fx_agg["cause_recall"] - ss_agg["cause_recall"], 3),
        "fixed_vs_single_fix_recall": round(fx_agg["fix_recall"] - ss_agg["fix_recall"], 3),
    }
    note = ("single-shot and fixed-two-step are keyless and deterministic; "
            "fixed-two-step is the ablation for the agent's temporal-lookup win")
    if keyed:
        ag_agg = aggregate(agent_records)
        configs["agent"] = ag_agg
        lift["agent_vs_fixed_cause_recall"] = round(ag_agg["cause_recall"] - fx_agg["cause_recall"], 3)
        lift["agent_vs_fixed_fix_recall"] = round(ag_agg["fix_recall"] - fx_agg["fix_recall"], 3)
        note += "; agent runs are indicative and non-deterministic"
    result = {
        "configs": configs,
        "lift": lift,
        "n_incidents": n,
        "note": note,
    }

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
