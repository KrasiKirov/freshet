"""A scripted, coherent incident used for demos and as eval ground truth.

The scenario tells one story on ``scheduler-api``: a deploy goes out, error rate
spikes, on-call investigates in chat, rolls back, the incident resolves, and a
postmortem lands later. Because we author it, we know exactly which events are
relevant to which questions — that is the ground truth the eval harness uses.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

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
    severity: Severity | None = None


@dataclass(frozen=True)
class Archetype:
    name: str
    steps: list[Step]
    queries: list[tuple[str, frozenset[str]]]
    # --- hard-tier fields ---------------------------------------------------
    # These describe how this archetype is made *adversarial*, and they live here
    # rather than in parallel name-keyed dicts so that adding an archetype cannot
    # silently omit one: the constructor requires them. (They previously lived in
    # four separate module-level dicts, where a missing entry surfaced as a
    # KeyError at corpus-generation time with no test to catch it.)
    #
    # symptom:          spike text whose mechanism matches THIS cause (pool
    #                   exhaustion follows a pool resize, OOM follows a leak), so
    #                   the cause is recoverable by reasoning rather than position.
    # cause_signature:  how the postmortem names the cause.
    # benign_decoy:     (source, type, text) of a plausible unrelated change.
    # ineffective_fix:  (source, type, text) of a remediation that does not work.
    #                   Its type is deliberately never this archetype's real fix
    #                   type, so ground truth stays unambiguous.
    # Hard tier only — the easy tier keeps its generic shared spike text, so
    # `build_benchmark` output (and every published M12/M14 number) is unchanged.
    symptom: str
    cause_signature: str
    benign_decoy: tuple[EventSource, str, str]
    ineffective_fix: tuple[EventSource, str, str]


def _archetype(name, change, fix, queries, *, symptom, cause_signature,
               benign_decoy, ineffective_fix) -> Archetype:
    """Build an archetype from its distinguishing change/fix (each a
    (source, type, text) tuple). Shared steps (spike, chat, recovery, postmortem)
    are identical across archetypes so retrieval is tested on the cause/fix vocab.
    The keyword-only hard-tier fields are required: a new archetype cannot be
    declared without saying how it is made adversarial."""
    c_src, c_type, c_text = change
    f_src, f_type, f_text = fix
    return Archetype(name=name, queries=queries, symptom=symptom,
                     cause_signature=cause_signature, benign_decoy=benign_decoy,
                     ineffective_fix=ineffective_fix, steps=[
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


def _Q(*pairs):
    return [(t, frozenset(types)) for t, types in pairs]

ARCHETYPES: list[Archetype] = [
    _archetype("deploy_regression",
               (EventSource.DEPLOY, "deploy_started", "Deploy v2.15.0 of {service} started by ci-bot"),
               (EventSource.DEPLOY, "rollback", "Rolling back {service} to the previous version"),
               _Q(("what deploy caused the {service} incident?", {"deploy_started", "error_spike"}),
                  ("how was the {service} outage resolved?", {"rollback", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="5xx error rate on {service} crossed 5% (now 11%)",
               cause_signature="the v2.15.0 deploy",
               benign_decoy=(EventSource.DEPLOY, "deploy_started",
                                  "Deploy v2.14.2 of {service} started by ci-bot"),
               ineffective_fix=(EventSource.DEPLOY, "scaled_up",
                                     "Scaled {service} up to 8 replicas")),
    _archetype("config_change",
               (EventSource.DEPLOY, "config_changed", "Config change applied to {service}: pool size 8 -> 64"),
               (EventSource.DEPLOY, "config_reverted", "Reverted the {service} config change"),
               _Q(("what config change caused the {service} incident?", {"config_changed", "error_spike"}),
                  ("how was the {service} outage resolved?", {"config_reverted", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="{service} connection pool saturated; requests queueing and timing out",
               cause_signature="the connection pool size change (8 -> 64)",
               benign_decoy=(EventSource.DEPLOY, "config_changed",
                                  "Config change applied to {service}: log level info -> debug"),
               ineffective_fix=(EventSource.DEPLOY, "scaled_up",
                                     "Scaled {service} up to 8 replicas")),
    _archetype("dependency_outage",
               (EventSource.ALERT, "dependency_down", "Upstream dependency for {service} is down (timeouts)"),
               (EventSource.DEPLOY, "dependency_failover", "Failed {service} over to the standby dependency"),
               _Q(("what dependency failure caused the {service} incident?", {"dependency_down", "error_spike"}),
                  ("how was the {service} outage resolved?", {"dependency_failover", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="upstream dependency timeouts on {service}: 40% of calls failing",
               cause_signature="the upstream dependency outage",
               benign_decoy=(EventSource.DEPLOY, "config_changed",
                                  "Config change applied to {service}: enable request tracing"),
               ineffective_fix=(EventSource.DEPLOY, "scaled_up",
                                     "Scaled {service} up to 8 replicas")),
    _archetype("resource_exhaustion",
               (EventSource.DEPLOY, "memory_leak_shipped", "Deploy shipped a memory leak to {service} (RSS climbing)"),
               (EventSource.DEPLOY, "scaled_up", "Scaled {service} up and restarted the leaking pods"),
               _Q(("what caused the {service} memory/resource incident?", {"memory_leak_shipped", "error_spike"}),
                  ("how was the {service} outage resolved?", {"scaled_up", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="{service} RSS at 98% of limit; pods OOMKilled repeatedly",
               cause_signature="the deploy that shipped the memory leak",
               benign_decoy=(EventSource.DEPLOY, "deploy_started",
                                  "Deploy v3.01.1 of {service} started by ci-bot"),
               ineffective_fix=(EventSource.DEPLOY, "config_reverted",
                                     "Reverted a recent {service} logging config toggle")),
    _archetype("cert_expiry",
               (EventSource.ALERT, "cert_expired", "TLS certificate for {service} expired; handshakes failing"),
               (EventSource.DEPLOY, "cert_renewed", "Renewed and deployed the {service} TLS certificate"),
               _Q(("what caused the {service} TLS/auth incident?", {"cert_expired", "error_spike"}),
                  ("how was the {service} outage resolved?", {"cert_renewed", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="TLS handshake failures on {service}: certificate verify failed",
               cause_signature="the expired TLS certificate",
               benign_decoy=(EventSource.DEPLOY, "config_changed",
                                  "Config change applied to {service}: rotate non-TLS API key"),
               ineffective_fix=(EventSource.DEPLOY, "scaled_up",
                                     "Scaled {service} up to 8 replicas")),
    _archetype("bad_migration",
               (EventSource.DEPLOY, "migration_applied", "Schema migration applied to {service} (locking writes)"),
               (EventSource.DEPLOY, "migration_reverted", "Reverted the {service} schema migration"),
               _Q(("what migration caused the {service} incident?", {"migration_applied", "error_spike"}),
                  ("how was the {service} outage resolved?", {"migration_reverted", "healthy"}),
                  ("root cause of the {service} incident", {"rca"}),
                  ("{service} error rate spike", {"error_spike"})),
               symptom="{service} write latency spiked; queries blocked on table locks",
               cause_signature="the schema migration that locked writes",
               benign_decoy=(EventSource.DEPLOY, "migration_applied",
                                  "Schema migration applied to {service} (add nullable column)"),
               ineffective_fix=(EventSource.DEPLOY, "scaled_up",
                                     "Scaled {service} up to 8 replicas")),
]





# Generic same-service benign changes for retrieval volume (before the bad change).
_VOLUME_CHANGES = [
    (EventSource.DEPLOY, "config_changed", "Config change applied to {service}: bump request timeout to 30s"),
    (EventSource.DEPLOY, "deploy_started", "Deploy of {service} started by ci-bot (dependency bump)"),
    (EventSource.DEPLOY, "config_changed", "Config change applied to {service}: add readiness probe"),
    (EventSource.DEPLOY, "deploy_started", "Deploy of {service} started by ci-bot (translation strings)"),
    (EventSource.DEPLOY, "config_changed", "Config change applied to {service}: raise log retention to 14d"),
]


def hard_incident_events(archetype: Archetype, service: str, start: datetime,
                         incident_id: str, mint: Callable[[], str],
                         n_volume: int = 10, rng: random.Random | None = None,
                         recoverable: bool = True,
                         ) -> tuple[list[Event], str, str, str, bool]:
    """One hardened incident on `service`, with the *positional* signal randomised
    away so that no fixed index rule can recover the answer.

    Per incident (seeded): `k_after` ∈ 0..3 benign changes sit between the true
    cause and the spike, and `m_failed` ∈ 0..2 ineffective remediations sit between
    the spike and the real fix. Because both can be **zero**, the naive
    "last-change / first-remediation" rule is sometimes exactly right — which also
    defeats any inverse rule that blindly skips one. The cause's index from the end
    varies, so "second-to-last change" is no better than chance.

    On ~1 in 4 incidents the true cause is placed *outside* the ±30-min lookup
    window, where the calibrated answer is to abstain rather than name a bystander;
    those are reported via the returned `recoverable` flag.

    What stays recoverable is the evidence: the spike text describes a symptom
    mechanistically coherent with the true cause and not with the decoys, and the
    postmortem names the cause's signature. The real fix is identifiable only by the
    `healthy` recovery event that follows it — the ineffective attempts are worded
    neutrally on purpose. Returns (events, cause_id, fix_id, spike_id, recoverable).
    """
    rng = rng or random.Random(0)
    c_step = archetype.steps[0]                 # role == "change" (the BAD change)
    f_step = next(s for s in archetype.steps if s.role == "remediation")
    b_src, b_type, b_text = archetype.benign_decoy
    i_src, i_type, i_text = archetype.ineffective_fix

    # Draw the layout up front so the rng consumption is fixed per incident.
    k_after = rng.randint(0, 3)        # benign changes between cause and spike
    m_failed = rng.randint(0, 2)       # failed remediations between spike and fix
    m_post = rng.randint(0, 2)         # cleanup remediations AFTER recovery
    # How the resolution is shaped. Without this the real fix is ALWAYS the last
    # remediation at or before the recovery event, and that single rule scores
    # 1.000 — saturating the fix task so it cannot distinguish an agent from a
    # heuristic. These three patterns move the fix's position *relative to the
    # recovery signal*, so recovery-anchoring alone is no longer sufficient.
    #   0 clean                     — fix, then recovery
    #   1 false_recovery            — a failed attempt briefly clears the alert,
    #                                 errors return, then the real fix
    #   2 cleanup_before_recovery   — a cleanup lands between the fix and recovery
    pattern = rng.randint(0, 2)
    if pattern == 1:
        m_failed = max(1, m_failed)    # a false recovery needs an attempt to follow
    # `recoverable` is assigned deterministically by the caller (every 4th incident)
    # rather than drawn here: a benchmark's composition should be exact, not subject
    # to sampling luck — a random draw landed on 42.5% out-of-window at seed 1.

    def ev(offset_s, source, type_, text, severity=None, benign=False,
           ineffective=False):
        meta: dict = {}
        if benign:
            meta["benign"] = True
        if ineffective:
            meta["ineffective"] = True
        e = Event(ts=start + timedelta(seconds=offset_s), incident_id=incident_id,
                  service=service, source=source, type=type_, severity=severity,
                  text=text.format(service=service), structured=meta)
        e.event_id = mint()
        return e

    events: list[Event] = []
    # 1) benign volume changes BEFORE the bad change (retrieval distractors)
    for i in range(n_volume):
        src, typ, txt = _VOLUME_CHANGES[i % len(_VOLUME_CHANGES)]
        events.append(ev(-600 - i * 30, src, typ, txt, benign=True))
    # 2) the BAD change (ground-truth cause). When not `recoverable` it is placed
    #    well outside the ±1800s window around the spike, so the calibrated answer
    #    becomes "abstain" and a guessing arm is penalised by false positives.
    bad = ev(0 if recoverable else -2400, c_step.source, c_step.type, c_step.text)
    events.append(bad)
    # 3) k_after benign changes between the cause and the spike (0..3 — at zero the
    #    naive last-change rule is correct, which is what kills inverse heuristics)
    for i in range(k_after):
        events.append(ev(15 * (i + 1), b_src, b_type, b_text, benign=True))
    # 4) spike / chat / remediation / recovery / postmortem
    spike = ev(90, EventSource.ALERT, "error_spike",
               archetype.symptom, Severity.SEV2)
    events.append(spike)
    events.append(ev(120, EventSource.CHAT, "message",
                     "alice: errors on {service} just spiked — investigating"))
    events.append(ev(150, EventSource.CHAT, "message",
                     "bob: looks correlated with the recent change to {service}"))
    # 5) m_failed ineffective remediations before the real fix (0..2 — at zero the
    #    naive first-remediation rule is correct)
    for i in range(m_failed):
        events.append(ev(160 + i * 20, i_src, i_type, i_text, ineffective=True))
    # a failed attempt clears the alert briefly; the errors then return. The alert
    # that follows the REAL fix is the one that sticks — that is the distinction an
    # arm has to make, and no fixed offset encodes it.
    if pattern == 1:
        events.append(ev(200, EventSource.ALERT, "healthy",
                         "{service} back below alert threshold; error rate normal",
                         ineffective=True))
        events.append(ev(210, EventSource.CHAT, "message",
                         "alice: {service} errors are back, that didn't hold"))
    fix = ev(240, f_step.source, f_step.type, f_step.text)
    events.append(fix)
    # a cleanup lands between the fix and the recovery, so "the last remediation
    # before recovery" is the cleanup rather than the fix
    if pattern == 2:
        events.append(ev(280, i_src, i_type, i_text, ineffective=True))
    # the recovery event is the ONLY signal separating the real fix from the
    # neutrally-worded attempts that preceded it
    events.append(ev(330, EventSource.ALERT, "healthy",
                     "{service} back below alert threshold; error rate normal"))
    # 6) cleanup remediations AFTER recovery (0..2). Without these the real fix is
    #    always the LAST remediation in the window, and a blind "last remediation"
    #    rule scores ~0.93 while understanding nothing. With them, the fix is only
    #    identifiable relative to the recovery event — i.e. from evidence.
    for i in range(m_post):
        events.append(ev(400 + i * 30, i_src, i_type, i_text, ineffective=True))
    # the postmortem names the CAUSE's signature (evidence the cause task needs)
    # but deliberately not the fix's, so fix identification stays a reasoning task
    events.append(ev(3600, EventSource.POSTMORTEM, "rca",
                     f"Postmortem: the {{service}} incident was caused by "
                     f"{archetype.cause_signature}, and was resolved once the "
                     "responsible change was undone. Action item: add a guard."))
    return events, bad.event_id, fix.event_id, spike.event_id, recoverable
