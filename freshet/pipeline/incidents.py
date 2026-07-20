"""Incident correlation: attach events to incidents and persist state.

M4 scope: rule-based correlation suited to the synthetic stream. An event with
an explicit incident_id is recorded against it. A severe event (error/latency
spike, rollback, or SEV1/SEV2) without one joins the open incident on its
service, or opens a new one. A healthy event — or a status-feed terminal
status ("resolved"/"postmortem", see RESOLUTION_TYPES) — resolves its
incident; an RCA event records the resolution summary. All writes are
idempotent (ON CONFLICT DO NOTHING joins into incident_services/incident_events,
COALESCE on resolution fields) so at-least-once redelivery cannot duplicate or
regress state.

Find-or-create is atomic: correlator-opened incidents claim a partial unique
index (one open auto incident per service) via INSERT ... ON CONFLICT, so
concurrent normalizer instances can race safely — the loser of the claim finds
the winner's incident and joins it. Explicit-id incidents (generator, status
feeds) are exempt from the constraint and go through the idempotent upsert.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from freshet.common.schemas import Event, EventType, Severity

SEVERE_TYPES = {
    EventType.ERROR_SPIKE.value,
    EventType.LATENCY_SPIKE.value,
    EventType.ROLLBACK.value,
}

# Event types that resolve an incident. "healthy" is the synthetic generator's
# recovery event; "resolved"/"postmortem" are Statuspage terminal statuses that
# the status poller passes through as the event type — without them, real
# status-feed incidents would stay open forever (and FIND_OPEN_SQL would glue
# every future severe event onto the oldest stale incident for the service).
RESOLUTION_TYPES = frozenset({
    EventType.HEALTHY.value,
    "resolved",
    "postmortem",
})


@dataclass
class CorrelationResult:
    incident_id: str | None
    transition: str | None  # "opened" | "resolved" | None


def is_severe(ev: Event) -> bool:
    return ev.type in SEVERE_TYPES or ev.severity in (Severity.SEV1, Severity.SEV2)


def incident_title(ev: Event) -> str:
    return f"{ev.service}: {ev.type}"


def _new_incident_id() -> str:
    return f"INC_{uuid.uuid4().hex[:12]}"


UPSERT_INCIDENT_SQL = """
INSERT INTO incidents (incident_id, title, opened_at)
VALUES (%(id)s, %(title)s, %(ts)s)
ON CONFLICT (incident_id) DO UPDATE SET
    opened_at = LEAST(incidents.opened_at, EXCLUDED.opened_at)
RETURNING (xmax = 0) AS inserted
"""

# Idempotent appends into the join tables — same "already there? do nothing"
# semantics the old array_append CASE gave, so redelivery is still safe.
LINK_SERVICE_SQL = (
    "INSERT INTO incident_services (incident_id, service)"
    " VALUES (%(id)s, %(service)s) ON CONFLICT DO NOTHING"
)
LINK_EVENT_SQL = (
    "INSERT INTO incident_events (incident_id, event_id)"
    " VALUES (%(id)s, %(event_id)s) ON CONFLICT DO NOTHING"
)

RESOLVE_SQL = (
    "UPDATE incidents SET resolved_at = %(ts)s"
    " WHERE incident_id = %(id)s AND resolved_at IS NULL"
    " RETURNING incident_id"
)
SUMMARY_SQL = (
    "UPDATE incidents SET resolution_summary = COALESCE(resolution_summary, %(text)s)"
    " WHERE incident_id = %(id)s"
)
FIND_OPEN_SQL = (
    "SELECT i.incident_id FROM incidents i"
    " JOIN incident_services s ON s.incident_id = i.incident_id"
    " WHERE i.resolved_at IS NULL AND s.service = %(service)s"
    " ORDER BY i.opened_at LIMIT 1"
)

# Atomic open-incident claim for stray severe events (no explicit incident_id).
# The partial unique index (one open auto incident per primary_service) makes
# this race-safe under concurrent normalizers: exactly one INSERT wins, losers
# get no row back and re-run FIND_OPEN to join the winner. The service link
# itself is written by LINK_SERVICE_SQL in correlate(), same as the upsert path.
CLAIM_OPEN_SQL = """
INSERT INTO incidents (incident_id, title, primary_service, auto_opened, opened_at)
VALUES (%(id)s, %(title)s, %(service)s, TRUE, %(ts)s)
ON CONFLICT (primary_service) WHERE resolved_at IS NULL AND auto_opened DO NOTHING
RETURNING incident_id
"""


def _find_or_open(conn, ev: Event) -> tuple[str, bool]:
    """Return (incident_id, opened): join the open incident on ev's service, or
    atomically open a new one. Retries the find once after a lost claim race."""
    params = {
        "id": _new_incident_id(),
        "title": incident_title(ev),
        "service": ev.service,
        "ts": ev.ts,
        "event_id": ev.event_id,
    }
    for _ in range(2):
        row = conn.execute(FIND_OPEN_SQL, {"service": ev.service}).fetchone()
        if row:
            return row[0], False
        claimed = conn.execute(CLAIM_OPEN_SQL, params).fetchone()
        if claimed:
            return claimed[0], True
        # lost the claim race — loop once to find the winner's incident
    # winner resolved in the window between claim and find: fall back to a
    # plain create through the idempotent upsert (transition comes from it)
    return _new_incident_id(), False


def correlate(conn, ev: Event) -> CorrelationResult:
    """Record ev against its incident; report the incident_id and transition.

    Resolution rules apply only to events explicitly carrying an incident_id —
    a routine 'healthy' noise event must not close an open incident.
    """
    incident_id = ev.incident_id
    opened_by_claim = False
    if incident_id is None and is_severe(ev):
        incident_id, opened_by_claim = _find_or_open(conn, ev)
    if incident_id is None:
        return CorrelationResult(None, None)
    inserted = conn.execute(
        UPSERT_INCIDENT_SQL,
        {"id": incident_id, "title": incident_title(ev), "ts": ev.ts},
    ).fetchone()[0]
    conn.execute(LINK_SERVICE_SQL, {"id": incident_id, "service": ev.service})
    conn.execute(LINK_EVENT_SQL, {"id": incident_id, "event_id": ev.event_id})
    # a claim-created row makes the follow-up upsert a no-op (inserted=False),
    # so the claim itself carries the "opened" transition
    transition = "opened" if (inserted or opened_by_claim) else None
    if ev.incident_id is not None and ev.type in RESOLUTION_TYPES:
        if conn.execute(RESOLVE_SQL, {"ts": ev.ts, "id": incident_id}).fetchone():
            transition = "resolved"
    elif ev.incident_id is not None and ev.type == EventType.RCA.value:
        conn.execute(SUMMARY_SQL, {"text": ev.text, "id": incident_id})
    return CorrelationResult(incident_id, transition)
