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

# Tiny stand-in carrying only what cite_hit needs (event_id, ts, text) — avoids
# constructing the 10-field RetrievedHit just to cite an event by id.
_Hit = namedtuple("_Hit", ["event_id", "ts", "text"])

_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_LOOKUP_SQL = "SELECT event_id, ts, text FROM vector_records WHERE event_id = %s LIMIT 1"


def fetch_runbook(conn, service: str) -> Optional[str]:
    row = conn.execute(_RUNBOOK_SQL, (service,)).fetchone()
    return row[0] if row else None


def lookup_hit(conn, event_id: str) -> Optional[_Hit]:
    row = conn.execute(_LOOKUP_SQL, (event_id,)).fetchone()
    return _Hit(event_id=row[0], ts=row[1], text=row[2]) if row else None


def gather_findings(conn, embedder, service: str, status: str, *, client=None) -> Findings:
    runbook = fetch_runbook(conn, service)
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            from freshet.api.agent import investigate
            inv = investigate(conn, embedder, service, client=client)
            cause_hit = lookup_hit(conn, inv.cause_id) if inv.cause_id else None
            fix_hit = lookup_hit(conn, inv.fix_id) if inv.fix_id else None
            return findings_from_investigation(inv, service, status, cause_hit, fix_hit, runbook)
        except Exception as exc:  # degrade, never crash the loop
            print(f"[autopilot] agent failed ({exc!r}); falling back to keyless timeline")
    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline
    q = f"what caused the {service} incident and how was it resolved?"
    res = hybrid_search(conn, embedder, q, k=12, service=service)
    tl = build_timeline(res.hits)
    return findings_from_timeline(tl, status, runbook)
