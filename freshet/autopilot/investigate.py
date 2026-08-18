"""Gather incident findings for the brief via the keyless extractive timeline."""

from __future__ import annotations

import os

from freshet.autopilot.brief import Findings, findings_from_timeline
from freshet.autopilot.impact import estimate_impact

_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_INCIDENT_META_SQL = "SELECT opened_at, resolved_at FROM incidents WHERE incident_id = %s"
_INCIDENT_SERVICES_SQL = "SELECT service FROM incident_services WHERE incident_id = %s"


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
