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

# The WHERE lives on the DO UPDATE branch, so the increment and the cap check
# are ONE atomic statement (two workers must not race it). When the guard
# fails, no row comes back and the counter is untouched — a refusal is free.
# It used to cost a call, so a crash loop that never reached the API still exhausted the daily cap.
_SPEND_SQL = (
    "INSERT INTO llm_budget (window_start, calls)"
    " VALUES (date_trunc('hour', now()), 1)"
    " ON CONFLICT (window_start) DO UPDATE SET calls = llm_budget.calls + 1"
    "   WHERE llm_budget.calls < %(hourly_cap)s"
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
    stop. A call REFUSED by the cap is a different thing: it was never
    attempted, so it costs nothing and leaves the counter where it was.
    """

    def __init__(self, inner, conn, hourly_cap: int | None = None,
                 daily_cap: int | None = None) -> None:
        self._inner = inner
        self._conn = conn
        # `or` would swallow an explicit 0: a caller asking for "spend nothing"
        # silently got the 60/hour default, so the cap could not be turned off.
        self.hourly_cap = (_cap("FRESHET_LLM_HOURLY_CAP", DEFAULT_HOURLY_CAP)
                           if hourly_cap is None else hourly_cap)
        self.daily_cap = (_cap("FRESHET_LLM_DAILY_CAP", DEFAULT_DAILY_CAP)
                          if daily_cap is None else daily_cap)

    def _spend(self) -> None:
        # A zero or negative cap means "disabled": the INSERT branch isn't
        # gated by the WHERE, so the first call of an hour would slip through a cap of 0.
        if self.hourly_cap <= 0 or self.daily_cap <= 0:
            raise BudgetExhausted("LLM budget is disabled (cap is zero); deferring")
        # Read the day BEFORE touching the hour, so a daily refusal is free too.
        day_calls = self._conn.execute(_DAY_SQL).fetchone()[0]
        if day_calls >= self.daily_cap:
            raise BudgetExhausted(
                f"LLM daily cap reached ({day_calls}/{self.daily_cap}); deferring")
        row = self._conn.execute(
            _SPEND_SQL, {"hourly_cap": self.hourly_cap}).fetchone()
        if row is None:
            raise BudgetExhausted(
                f"LLM hourly cap reached ({self.hourly_cap}/{self.hourly_cap}); "
                f"deferring until the next hour")

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
