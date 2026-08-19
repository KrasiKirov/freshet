"""Autopilot consumer: read incident.lifecycle, and on 'opened' debounce → claim
→ brief exactly once. On 'resolved', claim the postmortem slot and post a threaded
postmortem under the original brief's Slack message.

Handling is sequential; the blocking debounce wait is acceptable at demo incident
volumes and keeps offset handling trivial (no timer bookkeeping)."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from freshet.autopilot.investigate import gather_findings, gather_postmortem
from freshet.autopilot.sinks.base import Sink
from freshet.common.incidents import ensure_incident
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
# Delivery and the Slack thread id are recorded in ONE statement (the connection
# is autocommit, so two would be two commits): a crash between them would leave a
# delivered brief with no slack_ts, and the postmortem would post unthreaded.
_MARK_BRIEF_SQL = ("UPDATE incidents SET brief_delivered_at = now(),"
                   " brief_due_at = NULL,"
                   " slack_ts = coalesce(%s, slack_ts) WHERE incident_id = %s")
_MARK_POSTMORTEM_SQL = ("UPDATE incidents SET postmortem_delivered_at = now()"
                        " WHERE incident_id = %s")
_GET_SLACK_TS_SQL = "SELECT slack_ts FROM incidents WHERE incident_id = %s"
# A claim is a promise to deliver, not a record that we did. If the work after it
# fails, the claim MUST be released: Kafka will redeliver the lifecycle event, but
# a consumer that finds the slot already claimed skips it, so a transient Slack or
# LLM error would suppress that incident's brief permanently.
_RELEASE_SQL = "UPDATE incidents SET briefed_at = NULL WHERE incident_id = %s"
# The debounce, scheduled rather than slept. Only set when no brief has been
# delivered and none is already scheduled, so a redelivered lifecycle event does
# not keep pushing the due time further out (which would starve the incident).
_SCHEDULE_SQL = (
    "UPDATE incidents SET brief_due_at = now() + (%s * interval '1 second')"
    " WHERE incident_id = %s AND brief_delivered_at IS NULL AND brief_due_at IS NULL")
_DUE_SQL = (
    "SELECT incident_id, coalesce(primary_service, '') FROM incidents"
    " WHERE brief_due_at IS NOT NULL AND brief_due_at <= now()"
    "   AND brief_delivered_at IS NULL"
    " ORDER BY brief_due_at LIMIT %s")
# Cleared only on delivery. A failed attempt leaves it set so the next idle tick
# retries; the lease predicate is what stops two workers racing on it.
_CLEAR_DUE_SQL = "UPDATE incidents SET brief_due_at = NULL WHERE incident_id = %s"
_INDEXED_COUNT_SQL = (
    "SELECT count(*) FROM vector_records WHERE incident_id = %s")
_RELEASE_POSTMORTEM_SQL = "UPDATE incidents SET postmortem_at = NULL WHERE incident_id = %s"


def claim_incident(conn, incident_id: str) -> bool:
    """Claim the brief slot. The caller must have ensured the row exists — a
    claim against a missing row silently matches nothing and the incident is
    never briefed (which is exactly what happened to 956 of 1,182 incidents)."""
    return conn.execute(_CLAIM_SQL, (incident_id,)).fetchone() is not None


def claim_postmortem(conn, incident_id: str) -> bool:
    return conn.execute(_POSTMORTEM_CLAIM_SQL, (incident_id,)).fetchone() is not None


def schedule_brief(conn, incident_id: str, window_s: float) -> None:
    """Mark when this incident's brief becomes due, and return immediately."""
    conn.execute(_SCHEDULE_SQL, (window_s, incident_id))


def due_incidents(conn, limit: int = 10) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in conn.execute(_DUE_SQL, (limit,)).fetchall()]


def wait_for_index(conn, incident_id: str, timeout_s: float = 10.0,
                   sleep=time.sleep, now=time.monotonic) -> int:
    """Give the embedder a moment to index this incident's updates.

    A brief assembled before its evidence lands cites nothing. An empty timeline
    is still allowed after the timeout — status feeds are genuinely sparse, and a
    brief that says little beats no brief at all.
    """
    deadline = now() + timeout_s
    while True:
        n = conn.execute(_INDEXED_COUNT_SQL, (incident_id,)).fetchone()[0]
        if n or now() >= deadline:
            return n
        sleep(0.5)


def mark_brief_delivered(conn, incident_id: str, slack_ts: str | None = None) -> None:
    """Record that the sink accepted the brief, and the thread id it returned.
    Delivery is final: no expired lease may re-post it. Only ever called after
    deliver() RETURNS — a sink that fails raises, and the claim is released."""
    conn.execute(_MARK_BRIEF_SQL, (slack_ts, incident_id))


def mark_postmortem_delivered(conn, incident_id: str) -> None:
    conn.execute(_MARK_POSTMORTEM_SQL, (incident_id,))


def release_incident(conn, incident_id: str) -> None:
    """Undo a brief claim so a redelivery can retry it."""
    conn.execute(_RELEASE_SQL, (incident_id,))


def release_postmortem(conn, incident_id: str) -> None:
    """Undo a postmortem claim so a redelivery can retry it."""
    conn.execute(_RELEASE_POSTMORTEM_SQL, (incident_id,))


def _opened_at(ev: LifecycleEvent) -> datetime:
    """The lifecycle ts, or now if it is unparseable — never block a brief on it."""
    try:
        return datetime.fromisoformat(ev.ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(UTC)


def handle_lifecycle(conn, raw_json: str, *, window_s: float, sink: Sink,
                     sleep=time.sleep, composer=None) -> None:
    ev = LifecycleEvent.from_json(raw_json)

    if ev.type == "opened":
        # Both writers create the row: the embedder as it indexes, and here, so a
        # lifecycle event that beats the embedder can still brief.
        ensure_incident(conn, ev.incident_id, ev.service, _opened_at(ev), ev.title)
        # Schedule and return. Sleeping here held the Kafka partition for the whole
        # debounce window, delaying every offset behind it. drain_due_briefs
        # delivers on an idle tick instead, so the offset commits immediately.
        schedule_brief(conn, ev.incident_id, window_s)
        return

    if ev.type == "resolved":
        if not claim_postmortem(conn, ev.incident_id):
            print(f"[autopilot] {ev.incident_id} postmortem already posted or never briefed — skipping")
            return
        try:
            row = conn.execute(_GET_SLACK_TS_SQL, (ev.incident_id,)).fetchone()
            slack_ts = row[0] if row else None
            pm = gather_postmortem(conn, ev.service, ev.incident_id, composer=composer)
            sink.deliver(pm, thread=slack_ts)
            mark_postmortem_delivered(conn, ev.incident_id)
        except Exception:
            release_postmortem(conn, ev.incident_id)
            raise
        return

    print(f"[autopilot] {ev.type} {ev.incident_id} — no action")


def drain_due_briefs(conn, *, sink: Sink, limit: int = 10,
                     index_timeout_s: float = 10.0, composer=None) -> int:
    """Deliver every brief whose debounce window has elapsed. Returns how many.

    Runs on the consumer's idle tick, off the message path. Each incident is
    still claimed under the lease, so a second worker draining concurrently
    cannot double-deliver; a failure releases the claim and leaves `brief_due_at`
    set, so the next tick retries rather than dropping the incident.
    """
    delivered = 0
    for incident_id, service in due_incidents(conn, limit):
        if not claim_incident(conn, incident_id):
            continue                      # another worker holds the lease
        try:
            n = wait_for_index(conn, incident_id, index_timeout_s)
            if not n:
                print(f"[autopilot] {incident_id}: no indexed updates yet — "
                      f"briefing on what exists")
            findings = gather_findings(conn, service, incident_id, "open",
                                       composer=composer)
            ts = sink.deliver(findings)
        except Exception:
            # due_at stays set: the next idle tick retries this incident.
            release_incident(conn, incident_id)
            raise
        mark_brief_delivered(conn, incident_id, ts)
        delivered += 1
    return delivered
