"""Gather incident findings for the brief via the keyless extractive timeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

from freshet.autopilot.brief import (
    Findings,
    cause_from_updates,
    findings_from_timeline,
    findings_from_updates,
)
from freshet.autopilot.impact import estimate_impact

_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_INCIDENT_META_SQL = "SELECT opened_at, resolved_at FROM incidents WHERE incident_id = %s"
_INCIDENT_SERVICES_SQL = "SELECT service FROM incident_services WHERE incident_id = %s"
# The brief's update timeline is a DIRECT lookup, not a similarity search:
# an incident's updates are a known, complete set, and retrieval filters only by
# service — so a search would happily cite the provider's OTHER incidents.
_INCIDENT_UPDATES_SQL = (
    "SELECT DISTINCT ON (event_id) event_id, ts, text FROM vector_records"
    " WHERE incident_id = %s ORDER BY event_id, ts DESC")


@dataclass(frozen=True)
class _Update:
    """Minimal shape `cite_hit` and `findings_from_updates` need."""

    event_id: str
    ts: datetime
    text: str


def fetch_incident_updates(conn, incident_id: str) -> list[_Update]:
    """Every indexed update belonging to one incident. Deduplicated by event_id
    because a long update chunks into several rows."""
    rows = conn.execute(_INCIDENT_UPDATES_SQL, (incident_id,)).fetchall()
    return [_Update(event_id=r[0], ts=r[1], text=r[2]) for r in rows]


def fetch_runbook(conn, service: str) -> str | None:
    row = conn.execute(_RUNBOOK_SQL, (service,)).fetchone()
    return row[0] if row else None


def _impact_for(conn, incident_id: str, service: str, hits) -> str:
    row = conn.execute(_INCIDENT_META_SQL, (incident_id,)).fetchone()
    opened_at, resolved_at = row if row else (None, None)
    services = [r[0] for r in conn.execute(_INCIDENT_SERVICES_SQL, (incident_id,)).fetchall()]
    if not services:
        services = [service]
    return estimate_impact(services, opened_at, resolved_at, [h.text for h in hits])


def gather_findings(conn, embedder, service: str, incident_id: str, status: str) -> Findings:
    runbook = fetch_runbook(conn, service)
    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline
    q = f"what caused the {service} incident and how was it resolved?"
    res = hybrid_search(conn, embedder, q, k=12, service=service)
    tl = build_timeline(res.hits)
    f = findings_from_timeline(tl, status, runbook)
    # Cause/fix is kept for corpora that contain change events; the update
    # timeline is ADDED, not substituted, because status feeds have none. It is
    # sourced by direct lookup so the brief cannot cite a different incident.
    own = fetch_incident_updates(conn, incident_id)
    f.updates = findings_from_updates(service, status, own, runbook).updates
    # Change events give the strongest cause, but status feeds have none. Fall
    # back to the provider's own words IF an update actually states a cause.
    if not f.cause_text:
        stated = cause_from_updates(own)
        if stated:
            f.cause_text, f.cause_cite = stated
    f.impact = _impact_for(conn, incident_id, service, res.hits)
    return f


_INCIDENT_ROW_SQL = ("SELECT opened_at, resolved_at, resolution_summary"
                     " FROM incidents WHERE incident_id = %s")


def _format_duration(opened_at, resolved_at) -> str | None:
    if not opened_at or not resolved_at:
        return None
    secs = int((resolved_at - opened_at).total_seconds())
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    return f"{mins // 60}h {mins % 60}m"


def gather_postmortem(conn, embedder, service: str, incident_id: str, *, client=None) -> Findings:
    row = conn.execute(_INCIDENT_ROW_SQL, (incident_id,)).fetchone()
    opened_at, resolved_at, resolution_summary = row if row else (None, None, None)
    duration = _format_duration(opened_at, resolved_at)

    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline, synthesize_narrative
    q = f"what caused the {service} incident and how was it resolved?"
    res = hybrid_search(conn, embedder, q, k=12, service=service)
    tl = build_timeline(res.hits)
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            narrative = synthesize_narrative(tl, client=client)
        except Exception as exc:  # degrade to the extractive timeline, never crash
            print(f"[autopilot] narrative synthesis failed ({exc!r}); using extractive timeline")
            narrative = tl.render()
    else:
        narrative = tl.render()

    runbook = fetch_runbook(conn, service)
    summary = resolution_summary or "resolved"
    meta = f"Duration {duration} · {summary}" if duration else summary
    f = Findings(service=service, status="resolved", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=runbook, narrative=narrative, meta=meta)
    f.impact = _impact_for(conn, incident_id, service, res.hits)
    return f
