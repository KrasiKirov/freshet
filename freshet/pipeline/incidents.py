"""Incident correlation: attach events to incidents and persist state.

M4 scope: rule-based correlation suited to the synthetic stream. An event with
an explicit incident_id is recorded against it. A severe event (error/latency
spike, rollback, or SEV1/SEV2) without one joins the open incident on its
service, or opens a new one. A healthy event resolves its incident; an RCA
event records the resolution summary. All writes are idempotent (guarded array
appends, COALESCE on resolution fields) so at-least-once redelivery cannot
duplicate or regress state.

Single-writer assumption: the find-or-create in correlate() is not atomic, so
correctness requires exactly one normalizer instance (or a partial unique
index on open incidents per service) — scale embedders, not normalizers,
until that guard exists.
"""

from __future__ import annotations

import uuid
from typing import Optional

from freshet.common.schemas import Event, EventType, Severity

SEVERE_TYPES = {
    EventType.ERROR_SPIKE.value,
    EventType.LATENCY_SPIKE.value,
    EventType.ROLLBACK.value,
}


def is_severe(ev: Event) -> bool:
    return ev.type in SEVERE_TYPES or ev.severity in (Severity.SEV1, Severity.SEV2)


def incident_title(ev: Event) -> str:
    return f"{ev.service}: {ev.type}"


def _new_incident_id() -> str:
    return f"INC_{uuid.uuid4().hex[:12]}"


UPSERT_INCIDENT_SQL = """
INSERT INTO incidents (incident_id, title, services, opened_at, event_ids)
VALUES (%(id)s, %(title)s, ARRAY[%(service)s], %(ts)s, ARRAY[%(event_id)s])
ON CONFLICT (incident_id) DO UPDATE SET
    services = CASE WHEN %(service)s = ANY(incidents.services)
                    THEN incidents.services
                    ELSE array_append(incidents.services, %(service)s) END,
    event_ids = CASE WHEN %(event_id)s = ANY(incidents.event_ids)
                     THEN incidents.event_ids
                     ELSE array_append(incidents.event_ids, %(event_id)s) END,
    opened_at = LEAST(incidents.opened_at, EXCLUDED.opened_at)
"""

RESOLVE_SQL = (
    "UPDATE incidents SET resolved_at = COALESCE(resolved_at, %(ts)s)"
    " WHERE incident_id = %(id)s"
)
SUMMARY_SQL = (
    "UPDATE incidents SET resolution_summary = COALESCE(resolution_summary, %(text)s)"
    " WHERE incident_id = %(id)s"
)
FIND_OPEN_SQL = (
    "SELECT incident_id FROM incidents"
    " WHERE resolved_at IS NULL AND %(service)s = ANY(services)"
    " ORDER BY opened_at LIMIT 1"
)


def correlate(conn, ev: Event) -> Optional[str]:
    """Record ev against its incident; return the incident_id or None.

    Resolution rules apply only to events explicitly carrying an incident_id —
    a routine 'healthy' noise event must not close an open incident.
    """
    incident_id = ev.incident_id
    if incident_id is None and is_severe(ev):
        row = conn.execute(FIND_OPEN_SQL, {"service": ev.service}).fetchone()
        incident_id = row[0] if row else _new_incident_id()
    if incident_id is None:
        return None
    conn.execute(
        UPSERT_INCIDENT_SQL,
        {
            "id": incident_id,
            "title": incident_title(ev),
            "service": ev.service,
            "ts": ev.ts,
            "event_id": ev.event_id,
        },
    )
    if ev.incident_id is not None and ev.type == EventType.HEALTHY.value:
        conn.execute(RESOLVE_SQL, {"ts": ev.ts, "id": incident_id})
    elif ev.incident_id is not None and ev.type == EventType.RCA.value:
        conn.execute(SUMMARY_SQL, {"text": ev.text, "id": incident_id})
    return incident_id
