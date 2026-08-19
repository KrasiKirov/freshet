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

# The claim is a LEASE, not a tombstone. Kafka redelivers (auto_commit=False), so
# a crashed brief comes back — but a permanent claim would make the redelivered
# event skip it forever, silently downgrading at-least-once to at-most-once.
# A lease expires, so a hard kill self-heals with no reaper process: the predicate
# IS the reaper. `brief_delivered_at` is what stops an expired lease re-posting a
# brief that actually landed.
LEASE_MINUTES = 15   # must exceed the debounce window plus worst-case LLM latency
_CLAIM_SQL = (
    "UPDATE incidents SET briefed_at = now()"
    " WHERE incident_id = %s"
    "   AND brief_delivered_at IS NULL"
    f"  AND (briefed_at IS NULL OR briefed_at < now() - interval '{LEASE_MINUTES} minutes')"
    " RETURNING incident_id")
# A postmortem is only posted for an incident we actually briefed (briefed_at
# set). Without this guard, the first live-demo poll — which replays every
# historical, already-resolved status-feed incident — would flood the sink with
# postmortems for incidents that never got a brief.
_POSTMORTEM_CLAIM_SQL = (
    "UPDATE incidents SET postmortem_at = now()"
    " WHERE incident_id = %s"
    "   AND postmortem_delivered_at IS NULL"
    "   AND brief_delivered_at IS NOT NULL"     # only postmortem what we briefed
    f"  AND (postmortem_at IS NULL OR postmortem_at < now() - interval '{LEASE_MINUTES} minutes')"
    " RETURNING incident_id")
_MARK_BRIEF_SQL = "UPDATE incidents SET brief_delivered_at = now() WHERE incident_id = %s"
_MARK_POSTMORTEM_SQL = ("UPDATE incidents SET postmortem_delivered_at = now()"
                        " WHERE incident_id = %s")
_SET_SLACK_TS_SQL = "UPDATE incidents SET slack_ts = %s WHERE incident_id = %s"
_GET_SLACK_TS_SQL = "SELECT slack_ts FROM incidents WHERE incident_id = %s"
# A claim is a promise to deliver, not a record that we did. If the work after it
# fails, the claim MUST be released: Kafka will redeliver the lifecycle event, but
# a consumer that finds the slot already claimed skips it, so a transient Slack or
# LLM error would suppress that incident's brief permanently.
_RELEASE_SQL = "UPDATE incidents SET briefed_at = NULL WHERE incident_id = %s"
_RELEASE_POSTMORTEM_SQL = "UPDATE incidents SET postmortem_at = NULL WHERE incident_id = %s"


def claim_incident(conn, incident_id: str) -> bool:
    return conn.execute(_CLAIM_SQL, (incident_id,)).fetchone() is not None


def claim_postmortem(conn, incident_id: str) -> bool:
    return conn.execute(_POSTMORTEM_CLAIM_SQL, (incident_id,)).fetchone() is not None


def mark_brief_delivered(conn, incident_id: str) -> None:
    """Record that the sink accepted the brief. Delivery is final: no expired
    lease may re-post it."""
    conn.execute(_MARK_BRIEF_SQL, (incident_id,))


def mark_postmortem_delivered(conn, incident_id: str) -> None:
    conn.execute(_MARK_POSTMORTEM_SQL, (incident_id,))


def release_incident(conn, incident_id: str) -> None:
    """Undo a brief claim so a redelivery can retry it."""
    conn.execute(_RELEASE_SQL, (incident_id,))


def release_postmortem(conn, incident_id: str) -> None:
    """Undo a postmortem claim so a redelivery can retry it."""
    conn.execute(_RELEASE_POSTMORTEM_SQL, (incident_id,))


def handle_lifecycle(conn, embedder, raw_json: str, *, window_s: float, sink: Sink,
                     sleep=time.sleep, composer=None) -> None:
    ev = LifecycleEvent.from_json(raw_json)

    if ev.type == "opened":
        sleep(window_s)  # debounce: let the incident accrue evidence
        if not claim_incident(conn, ev.incident_id):
            print(f"[autopilot] {ev.incident_id} already briefed — skipping")
            return
        try:
            findings = gather_findings(conn, embedder, ev.service, ev.incident_id, "open")
            ts = sink.deliver(findings)
        except Exception:
            release_incident(conn, ev.incident_id)
            raise
        mark_brief_delivered(conn, ev.incident_id)
        if ts:
            conn.execute(_SET_SLACK_TS_SQL, (ts, ev.incident_id))
        return

    if ev.type == "resolved":
        if not claim_postmortem(conn, ev.incident_id):
            print(f"[autopilot] {ev.incident_id} postmortem already posted or never briefed — skipping")
            return
        try:
            row = conn.execute(_GET_SLACK_TS_SQL, (ev.incident_id,)).fetchone()
            slack_ts = row[0] if row else None
            pm = gather_postmortem(conn, embedder, ev.service, ev.incident_id, composer=composer)
            sink.deliver(pm, thread=slack_ts)
            mark_postmortem_delivered(conn, ev.incident_id)
        except Exception:
            release_postmortem(conn, ev.incident_id)
            raise
        return

    print(f"[autopilot] {ev.type} {ev.incident_id} — no action")
