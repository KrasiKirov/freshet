"""Gather incident findings for the brief. Mirrors the composer's `auto` pattern:
run the full tool-using agent when a key is present, else the keyless extractive
timeline — so the loop always runs and CI stays green."""

from __future__ import annotations

import os
from collections import namedtuple
from typing import Optional

from freshet.autopilot.brief import (
    Findings, findings_from_investigation, findings_from_timeline,
)
from freshet.autopilot.impact import estimate_impact

# Tiny stand-in carrying only what cite_hit needs (event_id, ts, text) — avoids
# constructing the 10-field RetrievedHit just to cite an event by id.
_Hit = namedtuple("_Hit", ["event_id", "ts", "text"])

_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_LOOKUP_SQL = "SELECT event_id, ts, text FROM vector_records WHERE event_id = %s LIMIT 1"
_INCIDENT_IMPACT_SQL = ("SELECT services, opened_at, resolved_at"
                        " FROM incidents WHERE incident_id = %s")


def fetch_runbook(conn, service: str) -> Optional[str]:
    row = conn.execute(_RUNBOOK_SQL, (service,)).fetchone()
    return row[0] if row else None


def lookup_hit(conn, event_id: str) -> Optional[_Hit]:
    row = conn.execute(_LOOKUP_SQL, (event_id,)).fetchone()
    return _Hit(event_id=row[0], ts=row[1], text=row[2]) if row else None


def _impact_for(conn, incident_id: str, service: str, hits) -> str:
    row = conn.execute(_INCIDENT_IMPACT_SQL, (incident_id,)).fetchone()
    services, opened_at, resolved_at = row if row else (None, None, None)
    services = list(services) if services else [service]
    return estimate_impact(services, opened_at, resolved_at, [h.text for h in hits])


def gather_findings(conn, embedder, service: str, incident_id: str, status: str,
                    *, client=None) -> Findings:
    runbook = fetch_runbook(conn, service)
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            from freshet.api.agent import investigate
            inv = investigate(conn, embedder, service, client=client)
            cause_hit = lookup_hit(conn, inv.cause_id) if inv.cause_id else None
            fix_hit = lookup_hit(conn, inv.fix_id) if inv.fix_id else None
            f = findings_from_investigation(inv, service, status, cause_hit, fix_hit, runbook)
            hits = [h for h in (cause_hit, fix_hit) if h]
            f.impact = _impact_for(conn, incident_id, service, hits)
            return f
        except Exception as exc:  # degrade, never crash the loop
            print(f"[autopilot] agent failed ({exc!r}); falling back to keyless timeline")
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


def _format_duration(opened_at, resolved_at) -> Optional[str]:
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
