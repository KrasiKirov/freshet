"""Periodic housekeeping on the autopilot's idle tick.

Two tables accumulate rows nothing ever removes: `llm_budget` (one row per hour)
and `pipeline_heartbeat_log` (one per minute per component). `BudgetedComposer`
already had a `prune()` — it simply had no caller anywhere in the codebase, so
the 7-day retention its docstring advertises had never once run.

Housekeeping is best-effort by construction: a failure here must never stop the
consumer, because nothing it does is on the delivery path.
"""

from __future__ import annotations

import logging
import time

from freshet.common.heartbeat import prune_log

log = logging.getLogger(__name__)

MAINTENANCE_INTERVAL_S = 3600.0     # both retention windows are days; hourly is ample


class Maintenance:
    """Throttled housekeeping. Returns whether a pass actually ran."""

    def __init__(self, interval_s: float = MAINTENANCE_INTERVAL_S,
                 now=time.monotonic) -> None:
        self._interval = interval_s
        self._now = now
        self._next_at = -1e9

    def __call__(self, conn, composer=None) -> bool:
        if self._now() < self._next_at:
            return False
        self._next_at = self._now() + self._interval
        try:
            prune = getattr(composer, "prune", None)
            if prune is not None:
                prune()
            prune_log(conn)
        except Exception as exc:
            log.warning("maintenance pass failed: %r", exc)
            return False
        return True
