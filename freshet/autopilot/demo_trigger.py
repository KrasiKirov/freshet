"""Fire a lifecycle event for an incident that is indexed but not yet briefed.

Live incidents arrive at roughly two an hour, which is the right rate for an
alerting agent and the wrong rate for taking screenshots. This picks a real
incident already in the index — real provider text, real citations, real
recurrence — and emits the `opened` event the Autopilot would have received when
it happened, so a brief lands in Slack on demand.

Nothing here fabricates data: the brief is assembled from the provider's own
updates by the same code path a live incident takes.

    make demo-brief              # best available candidate
    make demo-brief ARGS='--service cloudflare --count 2'
    make demo-brief ARGS='--repeat --service zoom'
    make demo-brief ARGS='--progress --service zoom'
    make demo-brief ARGS='--resolve --service zoom'
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from freshet.common.db import connect
from freshet.common.incidents import ensure_incident
from freshet.common.kafka_io import make_producer, produce_sync
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC, LifecycleEvent

# A screenshot wants a current unresolved incident with several updates (not one
# line), ideally one whose provider stated a cause, and earlier siblings for the
# recurrence line.
_CANDIDATES_SQL = """
SELECT v.incident_id,
       v.service,
       max(v.title)                         AS title,
       count(DISTINCT v.event_id)           AS updates,
       bool_or(v.text ILIKE '%%caused by%%'
            OR v.text ILIKE '%%root cause%%'
            OR v.text ILIKE '%%due to%%')   AS states_cause
FROM vector_records v
LEFT JOIN incidents i ON i.incident_id = v.incident_id
WHERE v.incident_id IS NOT NULL
  AND v.title IS NOT NULL
  AND v.ts > now() - interval '24 hours'
  AND (i.brief_delivered_at IS NULL)
  AND (i.resolved_at IS NULL)
  AND (%(service)s::text IS NULL OR v.service = %(service)s::text)
GROUP BY v.incident_id, v.service
HAVING count(DISTINCT v.event_id) >= 2
   AND bool_and(v.text !~* '(resolved|recovered|operational now|returned to normal|fix has been implemented|fully processed)')
ORDER BY states_cause DESC, count(DISTINCT v.event_id) DESC, max(v.ts) DESC
LIMIT %(limit)s
"""
_REPEAT_CANDIDATES_SQL = _CANDIDATES_SQL.replace(
    "  AND (i.brief_delivered_at IS NULL)\n", "").replace(
    "  AND (i.resolved_at IS NULL)\n", "")
_RESOLVE_CANDIDATES_SQL = _CANDIDATES_SQL.replace(
    "  AND (i.brief_delivered_at IS NULL)\n",
    "  AND (i.brief_delivered_at IS NOT NULL)\n")
_PROGRESS_CANDIDATES_SQL = _CANDIDATES_SQL.replace(
    "  AND (i.brief_delivered_at IS NULL)\n",
    "  AND (i.brief_delivered_at IS NOT NULL)\n")

_RESET_DEMO_SQL = """
UPDATE incidents
SET opened_at = %s,
    brief_as_of = (SELECT min(v.ts) FROM vector_records v
                   WHERE v.incident_id = incidents.incident_id),
    resolved_at = NULL,
    resolution_summary = NULL,
    briefed_at = NULL,
    brief_delivered_at = NULL,
    brief_due_at = NULL,
    postmortem_at = NULL,
    postmortem_delivered_at = NULL,
    progress_at = NULL,
    progress_delivered_at = NULL,
    progress_needed = false,
    postmortem_needed = false,
    slack_ts = NULL,
    slack_channel_id = NULL
WHERE incident_id = %s
"""


def candidates(conn, service: str | None = None, limit: int = 5,
               repeat: bool = False, progress: bool = False,
               resolve: bool = False) -> list[dict]:
    sql = (_RESOLVE_CANDIDATES_SQL if resolve else
           _PROGRESS_CANDIDATES_SQL if progress else
           _REPEAT_CANDIDATES_SQL if repeat else _CANDIDATES_SQL)
    rows = conn.execute(sql, {"service": service, "limit": limit}).fetchall()
    return [{"incident_id": r[0], "service": r[1], "title": r[2],
             "updates": r[3], "states_cause": r[4]} for r in rows]


def fire(conn, producer, pick: dict, *, repeat: bool = False) -> None:
    """Emit the `opened` event this incident would have produced when it opened."""
    now = datetime.now(UTC)
    if repeat:
        conn.execute(_RESET_DEMO_SQL, (now, pick["incident_id"]))
    ensure_incident(conn, pick["incident_id"], pick["service"], now, pick["title"] or "")
    # stamped now (autopilot refuses briefs older than MAX_BRIEF_AGE_S);
    # cited evidence keeps the provider's original timestamps
    ev = LifecycleEvent(type="opened", incident_id=pick["incident_id"],
                        service=pick["service"], ts=now.isoformat().replace("+00:00", "Z"),
                        title=pick["title"] or "")
    produce_sync(producer, LIFECYCLE_TOPIC, ev.to_json(), key=pick["incident_id"])


def resolve(producer, pick: dict) -> None:
    """Emit a controlled `resolved` event for a brief already posted by the demo."""
    ev = LifecycleEvent(type="resolved", incident_id=pick["incident_id"],
                        service=pick["service"],
                        ts=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        title=pick["title"] or "")
    produce_sync(producer, LIFECYCLE_TOPIC, ev.to_json(), key=pick["incident_id"])


def progress(producer, pick: dict) -> None:
    """Emit the provider's in-progress transition for the demo thread."""
    ev = LifecycleEvent(type="in_progress", incident_id=pick["incident_id"],
                        service=pick["service"],
                        ts=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        title=pick["title"] or "")
    produce_sync(producer, LIFECYCLE_TOPIC, ev.to_json(), key=pick["incident_id"])


def main() -> None:
    p = argparse.ArgumentParser(description="Trigger a demo brief from real indexed data")
    p.add_argument("--brokers", default="127.0.0.1:9092")
    p.add_argument("--service", default=None, help="restrict to one provider")
    p.add_argument("--count", type=int, default=1, help="how many briefs to trigger")
    p.add_argument("--dry-run", action="store_true", help="show candidates, fire nothing")
    modes = p.add_mutually_exclusive_group()
    modes.add_argument("--repeat", action="store_true",
                       help="reuse a current incident and reset its demo state")
    modes.add_argument("--resolve", action="store_true",
                       help="emit a resolved event for a brief already delivered by the demo")
    modes.add_argument("--progress", action="store_true",
                       help="emit an in-progress update for a brief already delivered by the demo")
    args = p.parse_args()

    conn = connect()
    picks = candidates(conn, args.service, limit=max(args.count, 5),
                       repeat=args.repeat, progress=args.progress, resolve=args.resolve)
    if not picks:
        raise SystemExit("no matching current incident — check the service, or run "
                         "the opening demo first")

    for pick in picks[:args.count if not args.dry_run else len(picks)]:
        mark = "cause stated" if pick["states_cause"] else "no stated cause"
        print(f"  {pick['service']:12} {pick['updates']:>3} updates  {mark:16} "
              f"{(pick['title'] or '')[:52]}")

    if args.dry_run:
        return
    if args.repeat:
        print("repeating a current incident for the demo; Slack will receive a new post")
    producer = make_producer(args.brokers)
    for pick in picks[:args.count]:
        if args.resolve:
            resolve(producer, pick)
        elif args.progress:
            progress(producer, pick)
        else:
            fire(conn, producer, pick, repeat=args.repeat)
    producer.flush()
    kind = "resolved" if args.resolve else "in_progress" if args.progress else "opened"
    print(f"\nfired {min(args.count, len(picks))} '{kind}' event(s)")


if __name__ == "__main__":
    main()
