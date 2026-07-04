"""Autopilot consumer: read incident.lifecycle, and on 'opened' debounce → claim
→ brief exactly once. On 'resolved', claim the postmortem slot and post a threaded
postmortem under the original brief's Slack message.

Handling is sequential; the blocking debounce wait is acceptable at demo incident
volumes and keeps offset handling trivial (no timer bookkeeping)."""

from __future__ import annotations

import time

from freshet.autopilot.investigate import gather_findings, gather_postmortem
from freshet.autopilot.sinks.base import Sink
from freshet.pipeline.lifecycle import LifecycleEvent

_CLAIM_SQL = ("UPDATE incidents SET briefed_at = now()"
              " WHERE incident_id = %s AND briefed_at IS NULL RETURNING incident_id")
_POSTMORTEM_CLAIM_SQL = ("UPDATE incidents SET postmortem_at = now()"
                         " WHERE incident_id = %s AND postmortem_at IS NULL RETURNING incident_id")
_SET_SLACK_TS_SQL = "UPDATE incidents SET slack_ts = %s WHERE incident_id = %s"
_GET_SLACK_TS_SQL = "SELECT slack_ts FROM incidents WHERE incident_id = %s"


def claim_incident(conn, incident_id: str) -> bool:
    return conn.execute(_CLAIM_SQL, (incident_id,)).fetchone() is not None


def claim_postmortem(conn, incident_id: str) -> bool:
    return conn.execute(_POSTMORTEM_CLAIM_SQL, (incident_id,)).fetchone() is not None


def handle_lifecycle(conn, embedder, raw_json: str, *, window_s: float, sink: Sink,
                     sleep=time.sleep, client=None) -> None:
    ev = LifecycleEvent.from_json(raw_json)

    if ev.type == "opened":
        sleep(window_s)  # debounce: let the incident accrue evidence
        if not claim_incident(conn, ev.incident_id):
            print(f"[autopilot] {ev.incident_id} already briefed — skipping")
            return
        findings = gather_findings(conn, embedder, ev.service, ev.incident_id, "open", client=client)
        ts = sink.deliver(findings)
        if ts:
            conn.execute(_SET_SLACK_TS_SQL, (ts, ev.incident_id))
        return

    if ev.type == "resolved":
        if not claim_postmortem(conn, ev.incident_id):
            print(f"[autopilot] {ev.incident_id} postmortem already posted — skipping")
            return
        row = conn.execute(_GET_SLACK_TS_SQL, (ev.incident_id,)).fetchone()
        slack_ts = row[0] if row else None
        pm = gather_postmortem(conn, embedder, ev.service, ev.incident_id, client=client)
        sink.deliver(pm, thread=slack_ts)
        return

    print(f"[autopilot] {ev.type} {ev.incident_id} — no action")
