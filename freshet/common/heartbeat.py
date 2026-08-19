"""Record that a pipeline component is alive, so freshness can prove uptime.

Freshness measures how long an update took to become queryable. The old filter
(`ts >= min(indexed_at)`) only excluded backfilled history — it could not tell a
SLOW pipeline from a STOPPED one. After a 14-hour outage the catch-up burst
scored 9.8-hour staleness and reported streaming as 14x slower than hourly batch.

A heartbeat row makes uptime explicit: score only updates posted inside a window
where the pipeline was demonstrably running.
"""

from __future__ import annotations

import time

# Written no more often than this. The freshness reader treats a gap longer than
# GAP_TOLERANCE_S as the pipeline having been down.
BEAT_INTERVAL_S = 30.0
GAP_TOLERANCE_S = 300.0          # 10x the beat: tolerates a slow batch, not an outage

_UPSERT = (
    "INSERT INTO pipeline_heartbeat (component, beat_at) VALUES (%s, now())"
    " ON CONFLICT (component) DO UPDATE SET beat_at = now()")
_LOG = ("INSERT INTO pipeline_heartbeat_log (component, beat_at)"
        " VALUES (%s, date_trunc('minute', now())) ON CONFLICT DO NOTHING")


class Heartbeat:
    """Throttled writer: one row per component, plus a per-minute log for gaps."""

    def __init__(self, component: str, interval_s: float = BEAT_INTERVAL_S,
                 now=time.monotonic) -> None:
        self.component = component
        self._interval = interval_s
        self._now = now
        self._last = -1e9

    def beat(self, conn) -> bool:
        """Write a heartbeat if one is due. Returns whether it wrote."""
        if self._now() - self._last < self._interval:
            return False
        self._last = self._now()
        conn.execute(_UPSERT, (self.component,))
        conn.execute(_LOG, (self.component,))
        return True


def continuous_run_start(conn, component: str = "embedder",
                         gap_tolerance_s: float = GAP_TOLERANCE_S):
    """Start of the CURRENT unbroken run, or None if the component never beat.

    Walks the per-minute log backwards from the newest beat and stops at the
    first gap wider than the tolerance. Everything before that gap belongs to an
    earlier run and must not be scored against this one.
    """
    rows = conn.execute(
        "SELECT beat_at FROM pipeline_heartbeat_log WHERE component = %s"
        " ORDER BY beat_at DESC", (component,)).fetchall()
    if not rows:
        return None
    start = rows[0][0]
    for (earlier,) in rows[1:]:
        if (start - earlier).total_seconds() > gap_tolerance_s:
            break
        start = earlier
    return start
