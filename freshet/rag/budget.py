"""A hard cap on LLM calls, so a runaway loop cannot run up a bill.

Normal load is small: roughly two incidents an hour, so a brief, a postmortem and
the occasional threaded question — call it a handful of calls per hour. The
danger is not steady state, it is a fault: a replayed topic, a crash-loop, or a
thread the bot somehow answers repeatedly. Any of those turn a cheap agent into
an expensive one while nobody is watching.

The counter lives in Postgres, not in memory, precisely because the fault case is
usually a restart loop — an in-process cap resets every time and caps nothing.

Over budget, callers DEFER rather than degrade: the brief keeps its due-time and
posts on the next window, and an unanswered thread question stays unanswered
rather than being marked as seen. A late alert is recoverable; silently answering
with something worse is not, and dropping the question loses it entirely.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

DEFAULT_HOURLY_CAP = 60          # ~10x normal load
DEFAULT_DAILY_CAP = 500

_SPEND_SQL = (
    "INSERT INTO llm_budget (window_start, calls)"
    " VALUES (date_trunc('hour', now()), 1)"
    " ON CONFLICT (window_start) DO UPDATE SET calls = llm_budget.calls + 1"
    " RETURNING calls")
_DAY_SQL = ("SELECT coalesce(sum(calls), 0) FROM llm_budget"
            " WHERE window_start > now() - interval '24 hours'")
_PRUNE_SQL = "DELETE FROM llm_budget WHERE window_start < now() - interval '7 days'"


class BudgetExhausted(RuntimeError):
    """The LLM call cap for this window is spent."""


def _cap(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


class BudgetedComposer:
    """Wraps any composer and refuses once the cap is reached.

    The spend is recorded BEFORE the call, so a request that fails or times out
    still counts — otherwise a persistently failing call would retry without
    limit and the cap would never bind, which is the exact runaway it exists to
    stop.
    """

    def __init__(self, inner, conn, hourly_cap: int | None = None,
                 daily_cap: int | None = None) -> None:
        self._inner = inner
        self._conn = conn
        self.hourly_cap = hourly_cap or _cap("FRESHET_LLM_HOURLY_CAP", DEFAULT_HOURLY_CAP)
        self.daily_cap = daily_cap or _cap("FRESHET_LLM_DAILY_CAP", DEFAULT_DAILY_CAP)

    def _spend(self) -> None:
        hour_calls = self._conn.execute(_SPEND_SQL).fetchone()[0]
        if hour_calls > self.hourly_cap:
            raise BudgetExhausted(
                f"LLM hourly cap reached ({hour_calls}/{self.hourly_cap}); "
                f"deferring until the next hour")
        day_calls = self._conn.execute(_DAY_SQL).fetchone()[0]
        if day_calls > self.daily_cap:
            raise BudgetExhausted(
                f"LLM daily cap reached ({day_calls}/{self.daily_cap}); deferring")

    def compose(self, question: str, hits) -> str:
        self._spend()
        return self._inner.compose(question, hits)

    def spent(self) -> tuple[int, int]:
        """(this hour, last 24h) — for logging and the metrics endpoint."""
        row = self._conn.execute(
            "SELECT coalesce((SELECT calls FROM llm_budget"
            "                 WHERE window_start = date_trunc('hour', now())), 0),"
            "       (SELECT coalesce(sum(calls), 0) FROM llm_budget"
            "        WHERE window_start > now() - interval '24 hours')").fetchone()
        return int(row[0]), int(row[1])

    def prune(self) -> None:
        self._conn.execute(_PRUNE_SQL)
