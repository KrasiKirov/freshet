"""A scripted, coherent incident used for demos and as eval ground truth.

The scenario tells one story on ``scheduler-api``: a deploy goes out, error rate
spikes, on-call investigates in chat, rolls back, the incident resolves, and a
postmortem lands later. Because we author it, we know exactly which events are
relevant to which questions — that is the ground truth the eval harness uses.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

from freshet.common.schemas import Event, EventSource, EventType, Severity

# Each step: (offset_seconds_from_incident_start, factory(start_ts, incident_id) -> Event)
ScenarioStep = tuple[float, Callable[[datetime, str], Event]]

SERVICE = "scheduler-api"
BAD_VERSION = "v2.15.0"
GOOD_VERSION = "v2.14.1"


def _ev(offset: float, **kw) -> ScenarioStep:
    def factory(start: datetime, incident_id: str) -> Event:
        return Event(ts=start + timedelta(seconds=offset), incident_id=incident_id, **kw)

    return offset, factory


SCENARIO: list[ScenarioStep] = [
    _ev(
        0,
        service=SERVICE,
        source=EventSource.DEPLOY,
        type=EventType.DEPLOY_STARTED,
        text=f"Deploy {BAD_VERSION} of {SERVICE} started by ci-bot",
        structured={"version": BAD_VERSION, "previous": GOOD_VERSION, "by": "ci-bot"},
    ),
    _ev(
        45,
        service=SERVICE,
        source=EventSource.DEPLOY,
        type=EventType.DEPLOY_FINISHED,
        text=f"Deploy {BAD_VERSION} of {SERVICE} finished",
        structured={"version": BAD_VERSION},
    ),
    _ev(
        90,
        service=SERVICE,
        source=EventSource.ALERT,
        type=EventType.ERROR_SPIKE,
        severity=Severity.SEV2,
        text=f"5xx error rate on {SERVICE} crossed 5% (now 11%)",
        structured={"metric": "error_rate", "value": 0.11, "threshold": 0.05},
    ),
    _ev(
        120,
        service=SERVICE,
        source=EventSource.CHAT,
        type=EventType.MESSAGE,
        text=f"alice: errors on {SERVICE} just spiked — anything deploy recently?",
        structured={"author": "alice"},
    ),
    _ev(
        150,
        service=SERVICE,
        source=EventSource.CHAT,
        type=EventType.MESSAGE,
        text=f"bob: yeah {BAD_VERSION} went out ~2m before the spike. correlated.",
        structured={"author": "bob"},
    ),
    _ev(
        180,
        service=SERVICE,
        source=EventSource.METRIC,
        type=EventType.LATENCY_SPIKE,
        text=f"p99 latency on {SERVICE} up 4x since {BAD_VERSION}",
        structured={"metric": "p99_latency_ms", "value": 1840, "baseline": 460},
    ),
    _ev(
        240,
        service=SERVICE,
        source=EventSource.DEPLOY,
        type=EventType.ROLLBACK,
        text=f"Rolling back {SERVICE} from {BAD_VERSION} to {GOOD_VERSION}",
        structured={"from": BAD_VERSION, "to": GOOD_VERSION, "by": "bob"},
    ),
    _ev(
        330,
        service=SERVICE,
        source=EventSource.ALERT,
        type=EventType.HEALTHY,
        text=f"5xx error rate on {SERVICE} back below threshold after rollback",
        structured={"metric": "error_rate", "value": 0.004},
    ),
    _ev(
        3600,
        service=SERVICE,
        source=EventSource.POSTMORTEM,
        type=EventType.RCA,
        text=(
            f"Postmortem: {BAD_VERSION} introduced a regression in the {SERVICE} "
            f"connection pool causing 5xx under load. Resolved by rolling back to "
            f"{GOOD_VERSION}. Action item: add pool-saturation canary check."
        ),
        structured={"root_cause": "connection_pool_regression", "fix": "rollback"},
    ),
]


def build_scenario(start: datetime, incident_id: str) -> list[Event]:
    """Materialize the scripted incident as a list of Events."""
    return [factory(start, incident_id) for _, factory in SCENARIO]
