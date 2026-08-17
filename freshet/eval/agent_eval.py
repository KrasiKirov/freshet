"""Agent vs single-shot vs fixed-two-step eval over a 12-incident sample.

Runs on the HARD benchmark tier (`build_hard_benchmark`): every incident carries
a benign decoy change interposed between the true cause and the spike — the LAST
change before the spike, engineered to trap a naive "latest change wins"
heuristic. This makes the ablation discriminate instead of saturating: on the
easy tier a single in-window cause makes step 2 near-tautological (it scores
1.0/1.0 mechanically); the decoy removes that guarantee, so the number now
reflects whether an arm can tell the real cause from a plausible bystander.

Three arms, all whole-corpus (no service hint):
  1. single-shot  — one hybrid search + extractive timeline (keyless, deterministic)
  2. fixed-two-step — the ABLATION: the same temporal lookup the agent uses
     (`events_around`), driven by a deterministic two-step pipeline with no LLM.
     Its heuristic picks the latest change before the spike, so the decoy is
     designed to fool it — the gap to the agent measures what reasoning buys.
  3. agent — the tool-calling LLM loop (key-gated, non-deterministic), which can
     read the corroborating chat/postmortem to reject the benign decoy.

Run (stack up, corpus indexed):
    python -m freshet.eval.agent_eval
Keyless runs score arms 1–2 only; with ANTHROPIC_API_KEY set, all three.
"""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from math import comb

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
    out = {
        "cause_recall": round(cause_recall, 3),
        "fix_recall": round(fix_recall, 3),
        "n": n,
    }
    # Abstention is reported separately from a wrong answer: naming the decoy and
    # returning nothing are very different failures for an on-call tool.
    if any("cause_pred" in r for r in records):
        out["abstained_cause"] = sum(
            1 for r in records if "cause_pred" in r and r["cause_pred"] is None)
        out["abstained_fix"] = sum(
            1 for r in records if "fix_pred" in r and r["fix_pred"] is None)
        # False positives: named something, and it was wrong. This is what the
        # out-of-window incidents exist to measure — without it, an arm that always
        # guesses is never penalised for confidently naming a bystander.
        out["cause_false_positives"] = sum(
            1 for r in records if r.get("cause_pred") is not None and not r["cause_hit"])
        unrec = [r for r in records if r.get("cause_recoverable") is False]
        rec = [r for r in records if r.get("cause_recoverable") is True]
        if rec:
            # headline recall is diluted by the incidents where abstaining is the
            # correct answer, so report the in-window subset on its own
            out["cause_recall_recoverable"] = round(
                sum(1 for r in rec if r["cause_hit"]) / len(rec), 3)
        if unrec:
            out["unrecoverable_n"] = len(unrec)
            out["unrecoverable_correctly_abstained"] = sum(
                1 for r in unrec if r["cause_pred"] is None)
    return out


def positional_rules(neighbors, spike) -> dict[str, dict]:
    """GAMEABILITY GUARD — reported on every run, never used as a result.

    A suite of blind index rules that encode no understanding whatsoever: they just
    pick the Nth change/remediation around the spike. If ANY of them scores above
    chance, the benchmark has a fixed layout and is measuring knowledge of the
    generator rather than reasoning — which is exactly the flaw that made the
    earlier fixed-position corpus worthless. All of these must sit near chance.
    """
    changes = sorted((n for n in neighbors
                      if n.type in _CAUSE_TYPES and n.ts <= spike.ts
                      and n.event_id != spike.event_id), key=lambda n: n.ts)
    rems = sorted((n for n in neighbors
                   if n.type in REMEDIATION_TYPES and n.ts >= spike.ts),
                  key=lambda n: n.ts)

    def at(seq, i):
        return seq[i].event_id if -len(seq) <= i < len(seq) else None

    return {
        "last-change/first-remediation": {
            "cause_id": at(changes, -1), "fix_id": at(rems, 0)},
        "2nd-to-last-change/2nd-remediation": {
            "cause_id": at(changes, -2), "fix_id": at(rems, 1)},
        "first-change/last-remediation": {
            "cause_id": at(changes, 0), "fix_id": at(rems, -1)},
    }


def _hardened_heuristic(conn, embedder, truth) -> dict:
    """The STRONGEST reasonable keyless baseline — the bar the agent must clear.

    Unlike the naive rules it uses evidence rather than position: it skips changes
    whose text is mechanistically unrelated to the spike's symptom (token overlap
    with the symptom text), and it takes the last remediation *before* the recovery
    event rather than the first after the spike. If the agent cannot beat this, the
    LLM is not buying reasoning — it is buying a heuristic someone could have
    written by hand."""
    spike, neighbors = anchor(conn, embedder, truth, scoped=True)
    if spike is None:
        return {"cause_id": None, "fix_id": None}

    stop = {"the", "on", "to", "of", "a", "for", "and", "is", "in", "at", "by"}
    def toks(s: str) -> set[str]:
        return {w.strip(".,;:()").lower() for w in s.split()} - stop
    symptom = toks(spike.text)

    # cause: among changes before the spike, prefer the one whose text shares the
    # most vocabulary with the symptom; abstain if nothing overlaps at all
    cands = [n for n in neighbors if n.type in _CAUSE_TYPES and n.ts <= spike.ts
             and n.event_id != spike.event_id]
    scored = sorted(cands, key=lambda n: (len(toks(n.text) & symptom), n.ts))
    cause = scored[-1] if scored and (toks(scored[-1].text) & symptom) else None

    # fix: the last remediation at or before the recovery event
    recovery = next((n for n in neighbors
                     if n.type == "healthy" and n.ts >= spike.ts), None)
    rems = [n for n in neighbors if n.type in REMEDIATION_TYPES and n.ts >= spike.ts
            and (recovery is None or n.ts <= recovery.ts)]
    fix = rems[-1] if rems else None
    return {"cause_id": cause.event_id if cause else None,
            "fix_id": fix.event_id if fix else None}


def mcnemar(base_records: list[dict], other_records: list[dict], key: str) -> dict:
    """Exact two-sided McNemar test on paired per-incident outcomes.

    The arms score the same incidents, so the comparison is paired and only the
    discordant pairs carry signal. At n=12 an unpaired proportion test would be
    hopeless; the paired test can still resolve a one-sided difference."""
    b = sum(1 for x, y in zip(base_records, other_records, strict=True)
            if y.get(key) and not x.get(key))      # other correct, base wrong
    c = sum(1 for x, y in zip(base_records, other_records, strict=True)
            if x.get(key) and not y.get(key))      # base correct, other wrong
    n = b + c
    p = 1.0 if n == 0 else min(
        1.0, 2 * sum(comb(n, k) for k in range(min(b, c) + 1)) / 2 ** n)
    return {"discordant_other_only": b, "discordant_base_only": c,
            "p_value": round(p, 4)}


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


def anchor(conn, embedder, truth, scoped: bool = False):
    """Step 1 shared by every keyless arm: find the spike, then pull its temporal
    neighbourhood. Returns (spike, neighbors) or (None, []) when no spike is found."""
    if scoped:
        # The scoped arm exists to isolate *rule* failure from *retrieval* failure,
        # so its anchor must be as reliable as possible: symptom vocabulary (the
        # spike texts are mechanism-specific, so the natural-language question
        # misses them) plus a deeper k. Anchoring is a tool step, not the thing
        # under test.
        q = (f"{truth.service} alert: error spike, latency, timeouts, saturation, "
             "handshake failures, OOM, locks")
        k = 25
    else:
        q = f"what caused the {truth.service} incident and how was it resolved?"
        k = 12
    res = hybrid_search(conn, embedder, q, k=k,
                        service=truth.service if scoped else None,
                        min_similarity=0.0, reranker=None, tau_s=_EVAL_TAU_S)
    spike = next((h for h in res.hits if h.type in _SPIKE_TYPES), None)
    if spike is None:
        return None, []
    return spike, events_around(conn, spike.service, spike.ts, window_s=1800.0)


def _pick_from_neighbors(neighbors, spike) -> dict:
    """The naive deterministic rules: latest change before the spike is the cause,
    first remediation at/after the spike is the fix. The anchor is excluded from
    the cause candidates — an event cannot be its own cause. (Without that guard,
    types that are both spike-role and cause-typed — `dependency_down`,
    `cert_expired` — let the anchor select itself and score a spurious hit.)"""
    cause = max((n for n in neighbors
                 if n.type in _CAUSE_TYPES and n.ts <= spike.ts
                 and n.event_id != spike.event_id),
                key=lambda n: n.ts, default=None)
    fix = next((n for n in neighbors
                if n.type in REMEDIATION_TYPES and n.ts >= spike.ts), None)
    return {
        "cause_id": cause.event_id if cause else None,
        "fix_id": fix.event_id if fix else None,
    }


def _fixed_two_step(conn, embedder, truth, scoped: bool = False) -> dict:
    """Ablation: same temporal tool as the agent, zero LLM. Step 1 is the
    identical whole-corpus search the single-shot baseline runs; step 2 anchors
    on the top spike-role hit and calls the non-semantic temporal lookup
    (`events_around`), then applies the naive type rules — the deterministic
    version of exactly what the agent does with `get_events_around`.

    `scoped=True` runs the same pipeline with the service filter the agent is
    also free to use. That separates the two ways this arm can fail: a whole-corpus
    step 1 sometimes retrieves no spike at all (a *retrieval* failure), whereas the
    scoped arm essentially always anchors correctly, so whatever it still gets wrong
    is purely the naive rule being fooled by the planted decoys."""
    spike, neighbors = anchor(conn, embedder, truth, scoped=scoped)
    if spike is None:
        return {"cause_id": None, "fix_id": None}
    return _pick_from_neighbors(neighbors, spike)


def main() -> None:
    keyed = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if not keyed:
        print("ANTHROPIC_API_KEY not set — running the keyless arms only "
              "(single-shot + fixed-two-step ablation); the agent arm is skipped.")

    from freshet.common.db import connect
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_hard_benchmark
    from freshet.pipeline.embedding import make_embedder
    if keyed:
        from freshet.api.agent import investigate

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()

    # Hard tier: each incident plants a benign decoy as the last change before the
    # spike, so the naive temporal heuristic can pick wrong (see module docstring).
    corpus, truths = build_hard_benchmark(seed=1, n_incidents=40)
    # Keyless arms are free, so they score the whole 40-incident tier by default.
    # Lower this (e.g. 2 → 12 incidents) to bound spend when the agent arm runs.
    per_arch = int(os.environ.get("FRESHET_EVAL_PER_ARCHETYPE", "7"))
    sample = sample_incidents(truths, n_per_archetype=per_arch)

    index_corpus(conn, embedder, corpus)

    n = len(sample)
    print(f"Eval sample: {n} incidents, 2 per archetype (6 archetypes)")
    if keyed:
        print(f"Estimated API calls: ~6 tool rounds × {n} incidents ≈ {6 * n} calls")

    ss_records: list[dict] = []
    fx_records: list[dict] = []
    sc_records: list[dict] = []
    agent_records: list[dict] = []

    hd_records: list[dict] = []
    guard_records: dict[str, list[dict]] = {}

    def _rec(pred: dict, truth) -> dict:
        return {
            "cause_hit": pred["cause_id"] == truth.cause_id,
            "fix_hit": pred["fix_id"] == truth.fix_id,
            "cause_pred": pred["cause_id"],
            "fix_pred": pred["fix_id"],
            "cause_recoverable": truth.cause_recoverable,
        }

    for truth in sample:
        ss = _single_shot(conn, embedder, truth)
        ss_records.append(_rec(ss, truth))
        fx = _fixed_two_step(conn, embedder, truth)
        fx_records.append(_rec(fx, truth))
        sc = _fixed_two_step(conn, embedder, truth, scoped=True)
        sc_records.append(_rec(sc, truth))
        hd = _hardened_heuristic(conn, embedder, truth)
        hd_records.append(_rec(hd, truth))
        # gameability guard: blind index rules, scored on the same anchors
        g_spike, g_neighbors = anchor(conn, embedder, truth, scoped=True)
        if g_spike is not None:
            for name, pred in positional_rules(g_neighbors, g_spike).items():
                guard_records.setdefault(name, []).append(_rec(pred, truth))
        line = (
            f"  {truth.incident_id} ({truth.archetype}): "
            f"ss=({ss['cause_id'] == truth.cause_id}/{ss['fix_id'] == truth.fix_id}) "
            f"scoped=({sc['cause_id'] == truth.cause_id}/{sc['fix_id'] == truth.fix_id}) "
            f"hardened=({hd['cause_id'] == truth.cause_id}/{hd['fix_id'] == truth.fix_id})"
        )
        if keyed:
            inv = investigate(conn, embedder, truth.service)
            agent_records.append(_rec(
                {"cause_id": inv.cause_id, "fix_id": inv.fix_id}, truth))
            line += f" agent=({inv.cause_id == truth.cause_id}/{inv.fix_id == truth.fix_id})"
        print(line)

    ss_agg = aggregate(ss_records)
    fx_agg = aggregate(fx_records)
    sc_agg = aggregate(sc_records)
    configs = {
        "single-shot": ss_agg,
        "fixed-two-step": fx_agg,
        "fixed-two-step-scoped": sc_agg,
        "hardened-heuristic": aggregate(hd_records),
    }
    # The guard is reported alongside, never as a result: every entry must sit near
    # chance, otherwise the benchmark is gameable and the arms above are void.
    guard = {name: aggregate(recs) for name, recs in guard_records.items()}
    # A blind index rule can only be right when the randomised trap count happens to
    # equal its offset, so its expected score is 1/(number of possible counts):
    # k_after ∈ 0..3 → 0.25 for cause, m_failed ∈ 0..2 → 0.333 for fix. Anything
    # meaningfully above this means the layout still leaks position.
    guard["_chance_ceiling"] = {"cause_recall": 0.25, "fix_recall": 0.333}
    lift = {
        "fixed_vs_single_cause_recall": round(fx_agg["cause_recall"] - ss_agg["cause_recall"], 3),
        "fixed_vs_single_fix_recall": round(fx_agg["fix_recall"] - ss_agg["fix_recall"], 3),
    }
    note = ("hard tier (build_hard_benchmark): trap COUNTS are randomised per "
            "incident (0-3 benign changes before the spike, 0-2 failed remediations "
            "after it), so no fixed index rule can recover the answer — see "
            "`gameability_guard`, every entry of which must sit near chance for the "
            "arms above to mean anything. ~1 in 4 incidents place the cause outside "
            "the lookup window, where abstaining is correct; `cause_false_positives` "
            "is what penalises guessing. All arms except `agent` are keyless and "
            "deterministic; `hardened-heuristic` is the strongest hand-written "
            "baseline (symptom-vocabulary match + recovery-anchored fix)")
    if keyed:
        ag_agg = aggregate(agent_records)
        configs["agent"] = ag_agg
        lift["agent_vs_scoped_cause_recall"] = round(ag_agg["cause_recall"] - sc_agg["cause_recall"], 3)
        lift["agent_vs_scoped_fix_recall"] = round(ag_agg["fix_recall"] - sc_agg["fix_recall"], 3)
        note += "; agent runs are indicative and non-deterministic (single run)"
    result = {
        "configs": configs,
        "gameability_guard": guard,
        "lift": lift,
        "n_incidents": n,
        "tier": "hard",
        "provenance": {
            "seed": 1,
            "embedder": os.environ.get("FRESHET_EMBEDDER", "bge"),
            "agent_model": (os.environ.get("FRESHET_AGENT_MODEL", "claude-sonnet-4-6")
                            if keyed else None),
            "run_date": datetime.now(UTC).date().isoformat(),
        },
        "note": note,
    }
    if keyed:
        result["paired_test"] = {
            "comparison": "agent vs fixed-two-step-scoped (cause_hit)",
            **mcnemar(sc_records, agent_records, "cause_hit"),
        }

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2)
    print(json.dumps(result, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
