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
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from freshet.common.db import connect
from freshet.common.incidents import ensure_incident
from freshet.common.kafka_io import make_producer, produce_sync
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC, LifecycleEvent

# A screenshot wants an incident with several updates (so the timeline is not one
# line), ideally one whose provider stated a cause, and ideally one with earlier
# siblings so the recurrence line appears.
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
  AND (i.brief_delivered_at IS NULL)
  AND (%(service)s::text IS NULL OR v.service = %(service)s::text)
GROUP BY v.incident_id, v.service
HAVING count(DISTINCT v.event_id) >= 3
ORDER BY states_cause DESC, count(DISTINCT v.event_id) DESC, max(v.ts) DESC
LIMIT %(limit)s
"""


def candidates(conn, service: str | None = None, limit: int = 5) -> list[dict]:
    rows = conn.execute(_CANDIDATES_SQL, {"service": service, "limit": limit}).fetchall()
    return [{"incident_id": r[0], "service": r[1], "title": r[2],
             "updates": r[3], "states_cause": r[4]} for r in rows]


def fire(conn, producer, pick: dict) -> None:
    """Emit the `opened` event this incident would have produced when it opened."""
    now = datetime.now(UTC)
    ensure_incident(conn, pick["incident_id"], pick["service"], now, pick["title"] or "")
    # The autopilot refuses briefs for incidents older than MAX_BRIEF_AGE_S, and
    # rightly so — a replayed topic must not page anyone about 2022. The demo
    # event is stamped NOW because the brief is being requested now; the CITED
    # evidence keeps the provider's own original timestamps.
    ev = LifecycleEvent(type="opened", incident_id=pick["incident_id"],
                        service=pick["service"], ts=now.isoformat().replace("+00:00", "Z"),
                        title=pick["title"] or "")
    produce_sync(producer, LIFECYCLE_TOPIC, ev.to_json(), key=pick["incident_id"])


def main() -> None:
    p = argparse.ArgumentParser(description="Trigger a demo brief from real indexed data")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--service", default=None, help="restrict to one provider")
    p.add_argument("--count", type=int, default=1, help="how many briefs to trigger")
    p.add_argument("--dry-run", action="store_true", help="show candidates, fire nothing")
    args = p.parse_args()

    conn = connect()
    picks = candidates(conn, args.service, limit=max(args.count, 5))
    if not picks:
        raise SystemExit("no unbriefed incident with >= 3 updates — every good "
                         "candidate has already been briefed; try --service")

    for pick in picks[:args.count if not args.dry_run else len(picks)]:
        mark = "cause stated" if pick["states_cause"] else "no stated cause"
        print(f"  {pick['service']:12} {pick['updates']:>3} updates  {mark:16} "
              f"{(pick['title'] or '')[:52]}")

    if args.dry_run:
        return
    producer = make_producer(args.brokers)
    for pick in picks[:args.count]:
        fire(conn, producer, pick)
    producer.flush()
    print(f"\nfired {min(args.count, len(picks))} 'opened' event(s) — the autopilot "
          f"briefs after its debounce window")


if __name__ == "__main__":
    main()
