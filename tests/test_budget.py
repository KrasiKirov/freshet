"""A hard cap on LLM spend that a restart cannot reset.

Normal load is a handful of calls an hour. The danger is a fault — a replayed
topic or a crash-loop — running up a bill while nobody watches, which is exactly
why the counter lives in Postgres rather than in the process.
"""
import pytest

from freshet.rag.budget import BudgetedComposer, BudgetExhausted


class _Conn:
    """Counts like the real SQL: one row per hour, incremented and returned."""
    def __init__(self):
        self.hour = 0
        self.day = 0

    def execute(self, sql, params=None):
        row = (0,)
        if "ON CONFLICT" in sql:
            cap = params["hourly_cap"]
            fresh = self.hour == 0
            if fresh or self.hour < cap:
                self.hour += 1
                self.day += 1
                row = (self.hour,)
            else:
                row = None                  # DO UPDATE ... WHERE matched nothing
        elif "sum(calls)" in sql and "SELECT calls FROM llm_budget" in sql:
            row = (self.hour, self.day)
        elif "sum(calls)" in sql:
            row = (self.day,)

        class _R:
            def fetchone(self_inner):
                return row
        return _R()


class _Inner:
    def __init__(self):
        self.calls = 0

    def compose(self, question, hits):
        self.calls += 1
        return "answer"


def test_calls_under_the_cap_pass_through():
    inner, conn = _Inner(), _Conn()
    c = BudgetedComposer(inner, conn, hourly_cap=3, daily_cap=100)
    assert c.compose("q", []) == "answer"
    assert inner.calls == 1


def test_the_hourly_cap_stops_further_calls():
    inner, conn = _Inner(), _Conn()
    c = BudgetedComposer(inner, conn, hourly_cap=2, daily_cap=100)
    c.compose("q", [])
    c.compose("q", [])
    with pytest.raises(BudgetExhausted, match="hourly"):
        c.compose("q", [])
    assert inner.calls == 2, "the third call must never reach the model"


def test_the_daily_cap_binds_even_when_the_hour_is_quiet():
    inner, conn = _Inner(), _Conn()
    conn.day = 500
    c = BudgetedComposer(inner, conn, hourly_cap=100, daily_cap=500)
    with pytest.raises(BudgetExhausted, match="daily"):
        c.compose("q", [])
    assert inner.calls == 0


def test_spend_is_recorded_before_the_call():
    """A failing call must still count, or a retry loop never hits the cap."""
    class _Boom:
        def compose(self, q, h):
            raise RuntimeError("api down")
    conn = _Conn()
    c = BudgetedComposer(_Boom(), conn, hourly_cap=2, daily_cap=100)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            c.compose("q", [])
    with pytest.raises(BudgetExhausted):
        c.compose("q", [])


def test_caps_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("FRESHET_LLM_HOURLY_CAP", "7")
    monkeypatch.setenv("FRESHET_LLM_DAILY_CAP", "9")
    c = BudgetedComposer(_Inner(), _Conn())
    assert (c.hourly_cap, c.daily_cap) == (7, 9)


def test_a_nonsense_cap_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("FRESHET_LLM_HOURLY_CAP", "not-a-number")
    assert BudgetedComposer(_Inner(), _Conn()).hourly_cap == 60


def test_a_refused_call_does_not_consume_budget():
    """The runaway this module exists to stop is a crash loop. If a refusal
    still increments, the loop exhausts the DAILY cap without ever reaching the
    API, and every legitimate brief defers for 24 hours."""
    inner, conn = _Inner(), _Conn()
    c = BudgetedComposer(inner, conn, hourly_cap=2, daily_cap=500)
    c.compose("q", [])
    c.compose("q", [])
    for _ in range(20):
        with pytest.raises(BudgetExhausted):
            c.compose("q", [])
    assert inner.calls == 2, "only admitted calls reach the inner composer"
    assert c.spent() == (2, 2), "refusals must leave the counters untouched"


def test_a_daily_refusal_does_not_consume_the_hourly_counter():
    inner, conn = _Inner(), _Conn()
    conn.day = 500
    c = BudgetedComposer(inner, conn, hourly_cap=60, daily_cap=500)
    with pytest.raises(BudgetExhausted, match="daily"):
        c.compose("q", [])
    assert conn.hour == 0


def test_a_zero_cap_refuses_without_writing():
    inner, conn = _Inner(), _Conn()
    c = BudgetedComposer(inner, conn, hourly_cap=0, daily_cap=0)
    with pytest.raises(BudgetExhausted):
        c.compose("q", [])
    assert conn.hour == 0 and inner.calls == 0
