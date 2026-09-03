"""Over budget must DEFER, never degrade — proven through the real code path.

`consumer.drain_due_briefs` has an `except BudgetExhausted` that releases the
claim and keeps `brief_due_at`, so the brief posts in the next window. It was
unreachable: `_summarise` caught bare `Exception`, so an exhausted budget
produced a delivered, narrative-less brief instead of a deferral. The existing
handler test monkeypatches `gather_findings` itself and cannot see that.
"""
from datetime import UTC, datetime

import pytest

from freshet.autopilot import investigate
from freshet.rag.budget import BudgetExhausted


class _OverBudget:
    def compose(self, question, hits):
        raise BudgetExhausted("LLM hourly cap reached")


class _Boom:
    def compose(self, question, hits):
        raise RuntimeError("the API is down")


class _ConnWithOneUpdate:
    """One indexed update for the incident; everything else empty."""

    def execute(self, sql, params=None):
        self._sql = sql
        return self

    def fetchall(self):
        if "GROUP BY event_id" in self._sql:
            return [("evt_1", datetime(2026, 8, 22, 10, 0, tzinfo=UTC),
                     "Elevated errors caused by a bad config push.",
                     "api", "status_update", "alert")]
        return []

    def fetchone(self):
        if "resolution_summary" in self._sql:
            return (datetime(2026, 8, 22, 9, 0, tzinfo=UTC),
                    datetime(2026, 8, 22, 10, 30, tzinfo=UTC), "resolved")
        if "FROM incidents" in self._sql:
            return (datetime(2026, 8, 22, 9, 0, tzinfo=UTC), None)
        return None


def test_an_exhausted_budget_propagates_out_of_gather_findings():
    with pytest.raises(BudgetExhausted):
        investigate.gather_findings(_ConnWithOneUpdate(), "api", "INC_1", "open",
                                    composer=_OverBudget())


def test_an_exhausted_budget_propagates_out_of_gather_postmortem():
    with pytest.raises(BudgetExhausted):
        investigate.gather_postmortem(_ConnWithOneUpdate(), "api", "INC_1",
                                      composer=_OverBudget())


def test_an_ordinary_generation_failure_still_degrades_gracefully():
    """Only the budget is a deferral. A failing API must not block the alert —
    the brief renders without its narrative, exactly as before."""
    f = investigate.gather_findings(_ConnWithOneUpdate(), "api", "INC_1", "open",
                                    composer=_Boom())
    assert f.narrative is None
    assert f.cause_text == "Elevated errors caused by a bad config push."
