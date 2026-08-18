"""Scoring statistics and benchmark-integrity checks, kept separate from the
arms that use them so both can be tested on their own.

`mcnemar` is the paired significance test the arms are compared with; the arms
score the same incidents, so only discordant pairs carry signal and an unpaired
proportion test would be hopeless at n=40.

`positional_rules` is the GAMEABILITY GUARD. It exists because an earlier version
of this benchmark planted its traps at fixed offsets, and a blind index rule
("second-to-last change, second remediation") scored 1.000 and beat the LLM while
understanding nothing. Running these every time turns "the benchmark measures
capability, not layout" into a number in the artifact instead of a claim in prose.
"""
from __future__ import annotations

from math import comb

from freshet.api.synthesis import _CAUSE_TYPES
from freshet.common.schemas import REMEDIATION_TYPES


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

    # recovery-anchored variants: "last remediation before the alert cleared" used
    # to score a saturating 1.000, so it is now measured every run rather than
    # trusted. `first_healthy` is the trap on false-recovery incidents.
    healthy = sorted((n for n in neighbors
                      if n.type == "healthy" and n.ts >= spike.ts),
                     key=lambda n: n.ts)
    def last_rem_before(h):
        if h is None:
            return None
        prior = [r for r in rems if r.ts <= h.ts]
        return prior[-1].event_id if prior else None

    return {
        # BLIND rules use position only and must stay at the chance ceiling —
        # anything above it means the layout leaks and the benchmark is void.
        "blind: last-change/first-remediation": {
            "cause_id": at(changes, -1), "fix_id": at(rems, 0)},
        "blind: 2nd-to-last-change/2nd-remediation": {
            "cause_id": at(changes, -2), "fix_id": at(rems, 1)},
        "blind: first-change/last-remediation": {
            "cause_id": at(changes, 0), "fix_id": at(rems, -1)},
        # EVIDENCE rules read the recovery signal, so beating chance is legitimate.
        # They are reported as reference baselines, not gameability indicators —
        # "last remediation before the alert cleared" scored a saturating 1.000
        # before false recoveries and post-fix cleanups were introduced.
        "evidence: last-remediation-before-FIRST-recovery": {
            "cause_id": None,
            "fix_id": last_rem_before(healthy[0] if healthy else None)},
        "evidence: last-remediation-before-LAST-recovery": {
            "cause_id": None,
            "fix_id": last_rem_before(healthy[-1] if healthy else None)},
    }


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
