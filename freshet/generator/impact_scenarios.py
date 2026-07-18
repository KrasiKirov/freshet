"""A dedicated, isolated impact benchmark: incidents authored to span Low/Med/High
by varying severity, stated error-rate, duration, and breadth. The authored label
is derived from the (unobservable-at-runtime) severity; the runtime heuristic must
recover it from observable proxies, so agreement < 100% is expected and honest.

Kept entirely separate from build_benchmark/scenarios.py so the committed
retrieval/cause-fix/multiquery numbers cannot move."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from freshet.common.schemas import Event, EventSource, EventType, Severity


@dataclass(frozen=True)
class ImpactTruth:
    incident_id: str
    label: str  # "Low" | "Medium" | "High"


# (authored_label, stated_pct, n_services, duration_min, spike_severity)
_SPECS = [
    ("High", 40, 3, 90, Severity.SEV1),
    ("High", 30, 1, 20, Severity.SEV1),
    ("High", 8, 1, 15, Severity.SEV1),   # severe but quiet observable signals (honest miss)
    ("High", 12, 4, 10, Severity.SEV1),
    ("Medium", 11, 1, 30, Severity.SEV2),
    ("Medium", 20, 1, 45, Severity.SEV2),
    ("Medium", 3, 1, 5, Severity.SEV2),  # quiet (honest miss)
    ("Medium", 15, 2, 8, Severity.SEV2),
    ("Low", 2, 1, 5, Severity.SEV4),
    ("Low", 4, 1, 3, Severity.SEV4),
    ("Low", 30, 1, 2, Severity.SEV4),    # small-but-loud stated spike (honest over-estimate)
    ("Low", 1, 1, 8, Severity.SEV4),
]


def _event(eid, ts, iid, svc, source, type_, text, severity=None) -> Event:
    ev = Event(ts=ts, incident_id=iid, service=svc, source=source, type=type_,
               severity=severity, text=text)
    ev.event_id = eid
    return ev


def build_impact_benchmark(seed: int = 1) -> tuple[list[Event], list[ImpactTruth]]:
    rng = random.Random(seed)
    start = datetime(2026, 7, 1, 8, 0, 0, tzinfo=UTC)

    def mint() -> str:
        return f"imp_{rng.getrandbits(48):012x}"

    events: list[Event] = []
    truths: list[ImpactTruth] = []
    t = start
    for i, (label, pct, n_services, dur_min, sev) in enumerate(_SPECS):
        iid = f"IMP-{i + 1:04d}"
        svcs = [f"svc{i:02d}-{j}" for j in range(n_services)]
        primary = svcs[0]
        events.append(_event(mint(), t, iid, primary, EventSource.DEPLOY,
                             EventType.DEPLOY_STARTED, f"Deploy of {primary} started"))
        spike_t = t + timedelta(seconds=90)
        for j, svc in enumerate(svcs):
            text = (f"5xx error rate on {svc} crossed 5% (now {pct}%)" if j == 0
                    else f"{svc} degraded — elevated errors")
            events.append(_event(mint(), spike_t, iid, svc, EventSource.ALERT,
                                 EventType.ERROR_SPIKE, text, severity=sev))
        heal_t = t + timedelta(minutes=dur_min)
        events.append(_event(mint(), heal_t, iid, primary, EventSource.ALERT,
                             EventType.HEALTHY, f"{primary} recovered"))
        truths.append(ImpactTruth(iid, label))
        t = heal_t + timedelta(hours=1)
    return events, truths
