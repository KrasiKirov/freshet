"""Autopilot consumer: read incident.lifecycle, and on 'opened' debounce → claim
→ brief exactly once. On 'resolved', claim the postmortem slot and post a threaded
postmortem under the original brief's Slack message.

Handling is sequential; the blocking debounce wait is acceptable at demo incident
volumes and keeps offset handling trivial (no timer bookkeeping)."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

from freshet.autopilot.investigate import gather_findings, gather_postmortem
from freshet.autopilot.sinks.base import Sink
from freshet.common.incidents import ensure_incident
from freshet.pipeline.lifecycle import LifecycleEvent
from freshet.rag.budget import BudgetExhausted

log = logging.getLogger(__name__)

DRAIN_INTERVAL_S = 5.0

LEASE_MINUTES = 15   # must exceed the debounce window plus worst-case LLM latency
MAX_BRIEF_AGE_S = 24 * 3600
_CLAIM_SQL = (
    "UPDATE incidents SET briefed_at = now()"
    " WHERE incident_id = %s"
    "   AND brief_delivered_at IS NULL"
    f"  AND (briefed_at IS NULL OR briefed_at < now() - interval '{LEASE_MINUTES} minutes')"
    " RETURNING incident_id")
_POSTMORTEM_CLAIM_SQL = (
    "UPDATE incidents SET postmortem_at = now()"
    " WHERE incident_id = %s"
    "   AND postmortem_delivered_at IS NULL"
    "   AND brief_delivered_at IS NOT NULL"     # only postmortem what we briefed
    f"  AND (postmortem_at IS NULL OR postmortem_at < now() - interval '{LEASE_MINUTES} minutes')"
    " RETURNING incident_id")
_MARK_BRIEF_SQL = ("UPDATE incidents SET brief_delivered_at = now(),"
                   " brief_due_at = NULL,"
                   " slack_ts = coalesce(%s, slack_ts),"
                   " slack_channel_id = coalesce(%s, slack_channel_id)"
                   " WHERE incident_id = %s")
_MARK_POSTMORTEM_SQL = ("UPDATE incidents SET postmortem_delivered_at = now()"
                        " WHERE incident_id = %s")
_GET_SLACK_TS_SQL = "SELECT slack_ts FROM incidents WHERE incident_id = %s"
_RELEASE_SQL = "UPDATE incidents SET briefed_at = NULL WHERE incident_id = %s"
_SCHEDULE_SQL = (
    "UPDATE incidents SET brief_due_at = now() + (%s * interval '1 second')"
    " WHERE incident_id = %s AND brief_delivered_at IS NULL AND brief_due_at IS NULL")
_DUE_SQL = (
    "SELECT incident_id, coalesce(primary_service, '') FROM incidents"
    " WHERE brief_due_at IS NOT NULL AND brief_due_at <= now()"
    "   AND brief_delivered_at IS NULL"
    " ORDER BY brief_due_at LIMIT %s")
_CLEAR_DUE_SQL = "UPDATE incidents SET brief_due_at = NULL WHERE incident_id = %s"
_DEFER_POSTMORTEM_SQL = (
    "UPDATE incidents SET postmortem_needed = true"
    " WHERE incident_id = %s AND postmortem_delivered_at IS NULL"
    " RETURNING incident_id")
_CLAIM_DEFERRED_POSTMORTEM_SQL = (
    "UPDATE incidents SET postmortem_at = now(), postmortem_needed = false"
    " WHERE incident_id = %s AND postmortem_needed"
    "   AND postmortem_delivered_at IS NULL AND brief_delivered_at IS NOT NULL"
    " RETURNING incident_id, coalesce(primary_service, ''), slack_ts")
_INDEXED_COUNT_SQL = (
    "SELECT count(*) FROM vector_records WHERE incident_id = %s")
_RELEASE_POSTMORTEM_SQL = "UPDATE incidents SET postmortem_at = NULL WHERE incident_id = %s"

_MARK_RESOLVED_SQL = ("UPDATE incidents SET resolved_at = coalesce(resolved_at, %s)"
                      " WHERE incident_id = %s")


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


def mark_brief_delivered(conn, incident_id: str, slack_ts: str | None = None,
                         channel_id: str | None = None) -> None:
    """Record that the sink accepted the brief, and the thread id it returned.
    Delivery is final: no expired lease may re-post it. Only ever called after
    deliver() RETURNS — a sink that fails raises, and the claim is released."""
    conn.execute(_MARK_BRIEF_SQL, (slack_ts, channel_id, incident_id))


def mark_postmortem_delivered(conn, incident_id: str) -> None:
    conn.execute(_MARK_POSTMORTEM_SQL, (incident_id,))


def release_incident(conn, incident_id: str) -> None:
    """Undo a brief claim so a redelivery can retry it."""
    conn.execute(_RELEASE_SQL, (incident_id,))


def release_postmortem(conn, incident_id: str) -> None:
    """Undo a postmortem claim so a redelivery can retry it."""
    conn.execute(_RELEASE_POSTMORTEM_SQL, (incident_id,))


def _event_ts(ev: LifecycleEvent) -> datetime:
    """The lifecycle ts, or now if it is unparseable — never block a brief on it.

    Coerced to UTC: Postgres reads a naive value into `timestamptz` using the
    SESSION time zone, which would shift every incident duration derived from it
    by that offset. Same guard freshet.common.schemas applies to Event.
    """
    try:
        parsed = datetime.fromisoformat(ev.ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(UTC)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def handle_lifecycle(conn, raw_json: str, *, window_s: float, sink: Sink,
                     sleep=time.sleep, composer=None) -> None:
    """Record what a lifecycle event implies; deliver nothing on this path.

    `sleep` is retained only for callers that still pass it — the debounce is a
    due-time in Postgres now, not a blocking wait.
    """
    ev = LifecycleEvent.from_json(raw_json)

    if ev.type == "opened":
        age = (datetime.now(UTC) - _event_ts(ev)).total_seconds()
        if age > MAX_BRIEF_AGE_S:
            print(f"[autopilot] {ev.incident_id} opened {age / 3600:.0f}h ago — "
                  f"too old to brief")
            return
        ensure_incident(conn, ev.incident_id, ev.service, _event_ts(ev), ev.title)
        schedule_brief(conn, ev.incident_id, window_s)
        return

    if ev.type == "resolved":
        conn.execute(_MARK_RESOLVED_SQL, (_event_ts(ev), ev.incident_id))
        if not claim_postmortem(conn, ev.incident_id):
            conn.execute(_DEFER_POSTMORTEM_SQL, (ev.incident_id,))
            print(f"[autopilot] {ev.incident_id} postmortem already posted or never briefed — skipping")
            return
        try:
            row = conn.execute(_GET_SLACK_TS_SQL, (ev.incident_id,)).fetchone()
            slack_ts = row[0] if row else None
            pm = gather_postmortem(conn, ev.service, ev.incident_id, composer=composer)
            sink.deliver(pm, thread=slack_ts)
            mark_postmortem_delivered(conn, ev.incident_id)
            print(f"[autopilot] {ev.incident_id}: postmortem delivered"
                  + (f" (slack_ts={slack_ts})" if slack_ts else ""), flush=True)
        except Exception:
            release_postmortem(conn, ev.incident_id)
            raise
        return

    print(f"[autopilot] {ev.type} {ev.incident_id} — no action")


def drain_due_briefs(conn, *, sink: Sink, limit: int = 10,
                     index_timeout_s: float = 10.0, composer=None,
                     embedder=None) -> int:
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
                                       composer=composer, embedder=embedder)
            ts = sink.deliver(findings)
        except BudgetExhausted as exc:
            release_incident(conn, incident_id)
            log.warning("%s: %s", incident_id, exc)
            break
        except Exception:
            release_incident(conn, incident_id)
            raise
        mark_brief_delivered(conn, incident_id, ts,
                             getattr(sink, "last_channel_id", None))
        print(f"[autopilot] {incident_id}: brief delivered"
              + (f" (slack_ts={ts})" if ts else ""), flush=True)
        delivered += 1
        deliver_deferred_postmortem(conn, incident_id, sink=sink, composer=composer)
    return delivered


def deliver_deferred_postmortem(conn, incident_id: str, *, sink: Sink,
                                composer=None) -> bool:
    """Post a postmortem that was owed but unclaimable while the brief was pending."""
    row = conn.execute(_CLAIM_DEFERRED_POSTMORTEM_SQL, (incident_id,)).fetchone()
    if row is None:
        return False
    _, service, slack_ts = row
    try:
        pm = gather_postmortem(conn, service, incident_id, composer=composer)
        sink.deliver(pm, thread=slack_ts)
    except Exception:
        release_postmortem(conn, incident_id)
        raise
    mark_postmortem_delivered(conn, incident_id)
    print(f"[autopilot] {incident_id}: deferred postmortem delivered"
          + (f" (slack_ts={slack_ts})" if slack_ts else ""), flush=True)
    return True


def handle_and_drain(conn, raw_json: str, *, window_s: float, sink: Sink,
                     sleep=time.sleep, composer=None, embedder=None) -> int:
    """Handle one lifecycle message, then deliver anything already due.

    Draining ONLY on an idle poll meant a busy partition could hold briefs
    indefinitely: `poll` never returns None while messages keep arriving, so the
    debounce elapsed and nothing delivered it. Draining here too bounds delivery
    by message arrival as well as by idleness.
    """
    handle_lifecycle(conn, raw_json, window_s=window_s, sink=sink, sleep=sleep,
                     composer=composer)
    return drain_due_briefs(conn, sink=sink, composer=composer,
                            embedder=embedder)


class DrainThrottle:
    """Rate-limits the idle-tick drain without changing its semantics."""

    def __init__(self, interval_s: float = DRAIN_INTERVAL_S, now=time.monotonic) -> None:
        self._interval, self._now, self._next = interval_s, now, -1e9

    def __call__(self, conn, *, sink: Sink, embedder=None, composer=None) -> int:
        if self._now() < self._next:
            return 0
        self._next = self._now() + self._interval
        return drain_due_briefs(conn, sink=sink, embedder=embedder, composer=composer)
