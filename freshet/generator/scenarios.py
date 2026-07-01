"""A scripted, coherent incident used for demos and as eval ground truth.

The scenario tells one story on ``scheduler-api``: a deploy goes out, error rate
spikes, on-call investigates in chat, rolls back, the incident resolves, and a
postmortem lands later. Because we author it, we know exactly which events are
relevant to which questions — that is the ground truth the eval harness uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from freshet.common.schemas import Event, EventSource, EventType, Severity

SERVICE = "scheduler-api"
BAD_VERSION = "v2.15.0"
GOOD_VERSION = "v2.14.1"


def build_scenario(start: datetime, incident_id: str, service: str = SERVICE) -> list[Event]:
    """Materialize one coherent incident arc on `service`:
    deploy -> error spike -> chat -> latency -> rollback -> healthy -> postmortem.
    Because we author it, the causing deploy (DEPLOY_STARTED) and the fix (ROLLBACK)
    are the known ground truth for the completeness eval."""
    bad, good = BAD_VERSION, GOOD_VERSION

    def at(offset: float) -> datetime:
        return start + timedelta(seconds=offset)

    return [
        Event(ts=at(0), incident_id=incident_id, service=service,
              source=EventSource.DEPLOY, type=EventType.DEPLOY_STARTED,
              text=f"Deploy {bad} of {service} started by ci-bot",
              structured={"version": bad, "previous": good, "by": "ci-bot"}),
        Event(ts=at(45), incident_id=incident_id, service=service,
              source=EventSource.DEPLOY, type=EventType.DEPLOY_FINISHED,
              text=f"Deploy {bad} of {service} finished", structured={"version": bad}),
        Event(ts=at(90), incident_id=incident_id, service=service,
              source=EventSource.ALERT, type=EventType.ERROR_SPIKE, severity=Severity.SEV2,
              text=f"5xx error rate on {service} crossed 5% (now 11%)",
              structured={"metric": "error_rate", "value": 0.11, "threshold": 0.05}),
        Event(ts=at(120), incident_id=incident_id, service=service,
              source=EventSource.CHAT, type=EventType.MESSAGE,
              text=f"alice: errors on {service} just spiked — anything deploy recently?",
              structured={"author": "alice"}),
        Event(ts=at(150), incident_id=incident_id, service=service,
              source=EventSource.CHAT, type=EventType.MESSAGE,
              text=f"bob: yeah {bad} went out ~2m before the spike. correlated.",
              structured={"author": "bob"}),
        Event(ts=at(180), incident_id=incident_id, service=service,
              source=EventSource.METRIC, type=EventType.LATENCY_SPIKE,
              text=f"p99 latency on {service} up 4x since {bad}",
              structured={"metric": "p99_latency_ms", "value": 1840, "baseline": 460}),
        Event(ts=at(240), incident_id=incident_id, service=service,
              source=EventSource.DEPLOY, type=EventType.ROLLBACK,
              text=f"Rolling back {service} from {bad} to {good}",
              structured={"from": bad, "to": good, "by": "bob"}),
        Event(ts=at(330), incident_id=incident_id, service=service,
              source=EventSource.ALERT, type=EventType.HEALTHY,
              text=f"5xx error rate on {service} back below threshold after rollback",
              structured={"metric": "error_rate", "value": 0.004}),
        Event(ts=at(3600), incident_id=incident_id, service=service,
              source=EventSource.POSTMORTEM, type=EventType.RCA,
              text=(f"Postmortem: {bad} introduced a regression in the {service} "
                    f"connection pool causing 5xx under load. Resolved by rolling back "
                    f"to {good}. Action item: add pool-saturation canary check."),
              structured={"root_cause": "connection_pool_regression", "fix": "rollback"}),
    ]


def build_runbooks(start: datetime, services: list[str]) -> list[Event]:
    """One static reference doc per service, ingested like any event so it is
    indexed and citable. Not time-bound; stamped at corpus start."""
    return [
        Event(
            ts=start,
            service=svc,
            source=EventSource.RUNBOOK,
            type="runbook",
            text=(f"{svc} runbook: on elevated 5xx or latency, check the most "
                  f"recent deploy first and roll back the latest version if it "
                  f"correlates; escalate to on-call if rollback does not recover."),
            structured={"doc": "runbook"},
        )
        for svc in services
    ]


@dataclass(frozen=True)
class Step:
    offset_s: float
    source: EventSource
    type: str
    role: str
    text: str
    severity: Optional[Severity] = None


@dataclass(frozen=True)
class Archetype:
    name: str
    steps: list[Step]
    queries: list[tuple[str, frozenset[str]]]


def _archetype(name, change, fix, queries) -> Archetype:
    """Build an archetype from its distinguishing change/fix (each a
    (source, type, text) tuple). Shared steps (spike, chat, recovery, postmortem)
    are identical across archetypes so retrieval is tested on the cause/fix vocab."""
    c_src, c_type, c_text = change
    f_src, f_type, f_text = fix
    return Archetype(name=name, queries=queries, steps=[
        Step(0,    c_src,                 c_type,         "change",      c_text),
        Step(90,   EventSource.ALERT,     "error_spike",  "spike",
             "5xx error rate on {service} crossed 5% (now 11%)", Severity.SEV2),
        Step(120,  EventSource.CHAT,      "message",      "chat",
             "alice: errors on {service} just spiked — investigating"),
        Step(150,  EventSource.CHAT,      "message",      "chat",
             "bob: looks correlated with the recent change to {service}"),
        Step(240,  f_src,                 f_type,         "remediation", f_text),
        Step(330,  EventSource.ALERT,     "healthy",      "recovery",
             "5xx error rate on {service} back below threshold"),
        Step(3600, EventSource.POSTMORTEM, "rca",         "postmortem",
             "Postmortem: the {service} incident was caused by the change above and "
             "resolved by the remediation above. Action item: add a guard."),
    ])


_Q = lambda *pairs: [(t, frozenset(types)) for t, types in pairs]

ARCHETYPES: list[Archetype] = [
    _archetype("deploy_regression",
               (EventSource.DEPLOY, "deploy_started", "Deploy v2.15.0 of {service} started by ci-bot"),
               (EventSource.DEPLOY, "rollback", "Rolling back {service} to the previous version"),
               _Q(("what deploy caused the {service} incident?", {"deploy_started", "error_spike"}),
                  ("how was the {service} outage resolved?", {"rollback", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
    _archetype("config_change",
               (EventSource.DEPLOY, "config_changed", "Config change applied to {service}: pool size 8 -> 64"),
               (EventSource.DEPLOY, "config_reverted", "Reverted the {service} config change"),
               _Q(("what config change caused the {service} incident?", {"config_changed", "error_spike"}),
                  ("how was the {service} outage resolved?", {"config_reverted", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
    _archetype("dependency_outage",
               (EventSource.ALERT, "dependency_down", "Upstream dependency for {service} is down (timeouts)"),
               (EventSource.DEPLOY, "dependency_failover", "Failed {service} over to the standby dependency"),
               _Q(("what dependency failure caused the {service} incident?", {"dependency_down", "error_spike"}),
                  ("how was the {service} outage resolved?", {"dependency_failover", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
    _archetype("resource_exhaustion",
               (EventSource.DEPLOY, "memory_leak_shipped", "Deploy shipped a memory leak to {service} (RSS climbing)"),
               (EventSource.DEPLOY, "scaled_up", "Scaled {service} up and restarted the leaking pods"),
               _Q(("what caused the {service} memory/resource incident?", {"memory_leak_shipped", "error_spike"}),
                  ("how was the {service} outage resolved?", {"scaled_up", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
    _archetype("cert_expiry",
               (EventSource.ALERT, "cert_expired", "TLS certificate for {service} expired; handshakes failing"),
               (EventSource.DEPLOY, "cert_renewed", "Renewed and deployed the {service} TLS certificate"),
               _Q(("what caused the {service} TLS/auth incident?", {"cert_expired", "error_spike"}),
                  ("how was the {service} outage resolved?", {"cert_renewed", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
    _archetype("bad_migration",
               (EventSource.DEPLOY, "migration_applied", "Schema migration applied to {service} (locking writes)"),
               (EventSource.DEPLOY, "migration_reverted", "Reverted the {service} schema migration"),
               _Q(("what migration caused the {service} incident?", {"migration_applied", "error_spike"}),
                  ("how was the {service} outage resolved?", {"migration_reverted", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"}))),
]
