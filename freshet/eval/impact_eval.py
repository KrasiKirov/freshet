"""Measure the impact heuristic against the authored impact benchmark. Pure and
keyless: derives each incident's observable signals (services, timestamps, its own
event texts) from build_impact_benchmark and compares classify_impact's label to the
authored ImpactTruth. Reports exact + adjacent agreement (Low<->High is a worse miss
than Low<->Medium). Honest framing: this measures how well observable proxies recover
an authored severity-driven label on synthetic data — NOT real user impact."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from freshet.autopilot.impact import classify_impact
from freshet.generator.impact_scenarios import build_impact_benchmark

_ORDER = {"Low": 0, "Medium": 1, "High": 2}
_OUT = Path("results/impact_metrics.json")


def evaluate(seed: int = 1) -> dict:
    events, truths = build_impact_benchmark(seed)
    by_incident: dict[str, list] = defaultdict(list)
    for e in events:
        by_incident[e.incident_id].append(e)

    exact = adjacent = 0
    confusion = []
    for tr in truths:
        evs = by_incident[tr.incident_id]
        services = sorted({e.service for e in evs})
        opened = min(e.ts for e in evs)
        resolved = max(e.ts for e in evs)
        texts = [e.text for e in evs]
        pred = classify_impact(services, opened, resolved, texts)
        if pred == tr.label:
            exact += 1
        if abs(_ORDER[pred] - _ORDER[tr.label]) <= 1:
            adjacent += 1
        if pred != tr.label:
            confusion.append({"incident": tr.incident_id, "truth": tr.label, "pred": pred})

    n = len(truths)
    return {
        "n": n,
        "exact_agreement": round(exact / n, 3),
        "adjacent_agreement": round(adjacent / n, 3),
        "confusion": confusion,
    }


def main() -> None:
    result = evaluate()
    _OUT.parent.mkdir(exist_ok=True)
    _OUT.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
