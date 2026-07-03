"""Autopilot consumer: read incident.lifecycle, and on 'opened' debounce → claim
→ brief exactly once. 'resolved' is a logged stub reserved for sub-project ③.

Handling is sequential; the blocking debounce wait is acceptable at demo incident
volumes and keeps offset handling trivial (no timer bookkeeping)."""

from __future__ import annotations

import time

from freshet.autopilot.investigate import gather_findings
from freshet.pipeline.lifecycle import LifecycleEvent

_CLAIM_SQL = ("UPDATE incidents SET briefed_at = now()"
              " WHERE incident_id = %s AND briefed_at IS NULL RETURNING incident_id")


def claim_incident(conn, incident_id: str) -> bool:
    return conn.execute(_CLAIM_SQL, (incident_id,)).fetchone() is not None


def handle_lifecycle(conn, embedder, raw_json: str, *, window_s: float, sink,
                     sleep=time.sleep, client=None) -> None:
    ev = LifecycleEvent.from_json(raw_json)
    if ev.type != "opened":
        print(f"[autopilot] {ev.type} {ev.incident_id} — no action (sub-project ③)")
        return
    sleep(window_s)  # debounce: let the incident accrue evidence
    if not claim_incident(conn, ev.incident_id):
        print(f"[autopilot] {ev.incident_id} already briefed — skipping")
        return
    findings = gather_findings(conn, embedder, ev.service, status="open", client=client)
    sink.deliver(findings)
