from datetime import UTC, datetime, timedelta

import pytest

from freshet.autopilot import consumer
from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.pipeline.lifecycle import LifecycleEvent


class _FakeConn:
    """Routes by SQL: RETURNING → claim; slack_ts → stored ts; due → scheduled
    briefs; vector_records count → how much evidence is indexed."""
    def __init__(self, *, claim_ok=True, slack_ts=None, due=(("INC_1", "api"),),
                 indexed=3, postmortem_needed=False):
        self.claim_ok = claim_ok
        self.slack_ts = slack_ts
        self.due = list(due)
        self.indexed = indexed
        self.postmortem_needed = postmortem_needed
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        row, rows = None, []
        if "slack_ts IS NOT NULL" in sql:
            rows = []                       # thread polling is not under test here
        elif "postmortem_needed" in sql and "RETURNING" in sql:
            row = ("INC_1", "api", self.slack_ts) if self.postmortem_needed else None
        elif "RETURNING" in sql:
            row = ("INC_1",) if self.claim_ok else None
        elif "SELECT slack_ts" in sql:
            row = (self.slack_ts,)
        elif "count(*) FROM vector_records" in sql:
            row = (self.indexed,)
        elif "brief_due_at IS NOT NULL" in sql:
            rows = self.due

        class _R:
            def __init__(self, r, rs):
                self._r, self._rs = r, rs
            def fetchone(self):
                return self._r
            def fetchall(self):
                return self._rs
        return _R(row, rows)


class _RecordingSink:
    def __init__(self, handle=None):
        self.handle = handle
        self.calls = []

    def deliver(self, findings, *, thread=None):
        self.calls.append((findings, thread))
        return self.handle


def _pm():
    return Findings("api", "resolved", None, None, None, None, None, "narrative", "Duration 42m · resolved")


def _open_json(ts=None):
    stamp = ts or datetime.now(UTC).isoformat()
    return LifecycleEvent(type="opened", incident_id="INC_1", service="api", ts=stamp).to_json()


def _resolved_json():
    return LifecycleEvent(type="resolved", incident_id="INC_1", service="api",
                          ts="2026-07-01T00:00:00+00:00").to_json()


def test_resolved_posts_postmortem_threaded_under_slack_ts(monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    sink = _RecordingSink()
    consumer.handle_lifecycle(_FakeConn(slack_ts="9.9"), _resolved_json(),
                              window_s=0, sink=sink, sleep=lambda s: None)
    assert len(sink.calls) == 1
    findings, thread = sink.calls[0]
    assert findings.status == "resolved" and thread == "9.9"


def test_resolved_skips_on_redelivery(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run")))
    sink = _RecordingSink()
    consumer.handle_lifecycle(_FakeConn(claim_ok=False), _resolved_json(),
                              window_s=0, sink=sink, sleep=lambda s: None)
    assert not sink.calls
    assert "already" in capsys.readouterr().out.lower()


def _lifecycle(kind="opened", iid="INC-1"):
    import json
    return json.dumps({"type": kind, "incident_id": iid, "service": "github",
                       "ts": "2026-08-18T12:00:00+00:00", "title": "t"})


class _RecordingConn:
    """Tracks whether the claim was released after a failure."""

    def __init__(self):
        self.claimed = True
        self.released = False

    def execute(self, sql, params=None):
        if "briefed_at = now()" in sql or "postmortem_at = now()" in sql:
            row = ("INC-1",) if self.claimed else None
        elif "briefed_at = NULL" in sql or "postmortem_at = NULL" in sql:
            self.released = True
            row = None
        else:
            row = None

        class _Cur:
            def fetchone(self_inner):
                return row

            def fetchall(self_inner):
                return []          # no evidence needed; the sink is what fails
        return _Cur()


def test_failed_postmortem_releases_its_claim(monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    conn = _FakeConn(slack_ts="1.2")
    with pytest.raises(RuntimeError, match="slack down"):
        consumer.handle_lifecycle(conn, _resolved_json(), window_s=0,
                                  sink=_RaisingSink(), sleep=lambda s: None)
    sql = " ".join(q for q, _ in conn.executed)
    assert "postmortem_at = NULL" in sql
    assert "postmortem_delivered_at = now()" not in sql


class _RaisingSink:
    def deliver(self, findings, *, thread=None):
        raise RuntimeError("slack down")


# --- the debounce is scheduled, not slept -----------------------------------

def test_opened_schedules_a_brief_and_never_sleeps():
    """Sleeping in the handler held the Kafka partition for the whole window,
    delaying every offset behind it. The handler must return immediately."""
    conn = _FakeConn()
    def _boom(_):
        raise AssertionError("the handler must not sleep")
    consumer.handle_lifecycle(conn, _open_json(), window_s=45, sink=StdoutSink(), sleep=_boom)
    sql = " ".join(q for q, _ in conn.executed)
    assert "brief_due_at = now() +" in sql, "the brief must be scheduled"
    assert "brief_delivered_at = now()" not in sql, "nothing is delivered on this path"


def test_scheduling_does_not_push_an_already_scheduled_brief_further_out():
    """A redelivered lifecycle event must not starve the incident by resetting
    its due time on every retry."""
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, _open_json(), window_s=45, sink=StdoutSink())
    schedule = next(q for q, _ in conn.executed if "brief_due_at = now() +" in q)
    assert "brief_due_at IS NULL" in schedule
    assert "brief_delivered_at IS NULL" in schedule


# --- delivery happens on the idle tick ---------------------------------------

def test_drain_delivers_a_due_brief_once(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", "bad deploy",
                                                 "[ev1 @ 2026-07-01 00:00:00]",
                                                 None, None, None, None))
    conn, sink = _FakeConn(), _RecordingSink(handle="9.9")
    assert consumer.drain_due_briefs(conn, sink=sink) == 1
    assert len(sink.calls) == 1
    # params are (slack_ts, slack_channel_id, incident_id)
    assert any("brief_delivered_at = now()" in q and p == ("9.9", None, "INC_1")
               for q, p in conn.executed)


def test_drain_with_nothing_due_is_a_no_op(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not brief")))
    conn, sink = _FakeConn(due=()), _RecordingSink()
    assert consumer.drain_due_briefs(conn, sink=sink) == 0
    assert sink.calls == []


def test_drain_skips_an_incident_another_worker_holds(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not brief")))
    conn, sink = _FakeConn(claim_ok=False), _RecordingSink()
    assert consumer.drain_due_briefs(conn, sink=sink) == 0
    assert sink.calls == []


def test_a_failed_delivery_releases_the_claim_and_leaves_it_due(monkeypatch):
    """due_at must survive so the next idle tick retries; marking it delivered
    would suppress the incident forever."""
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, None))
    conn = _FakeConn()
    with pytest.raises(RuntimeError, match="slack down"):
        consumer.drain_due_briefs(conn, sink=_RaisingSink())
    sql = " ".join(q for q, _ in conn.executed)
    assert "briefed_at = NULL" in sql
    assert "brief_delivered_at = now()" not in sql
    assert "brief_due_at = NULL" not in sql, "the retry must stay scheduled"


def test_stale_opened_events_are_not_scheduled():
    """The first poller sweep replays years of Atom history. Without a recency
    cut, every historical incident gets a brief."""
    conn = _FakeConn()
    old = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    consumer.handle_lifecycle(conn, _open_json(old), window_s=45, sink=StdoutSink())
    sql = " ".join(q for q, _ in conn.executed)
    assert "brief_due_at = now() +" not in sql


def test_resolved_before_brief_marks_postmortem_needed(monkeypatch):
    """A resolve inside the debounce window used to skip the postmortem forever."""
    monkeypatch.setattr(consumer, "gather_postmortem",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("too early")))
    conn = _FakeConn(claim_ok=False)
    consumer.handle_lifecycle(conn, _resolved_json(), window_s=0, sink=_RecordingSink())
    sql = " ".join(q for q, _ in conn.executed)
    assert "postmortem_needed" in sql


def test_drain_posts_a_deferred_postmortem_after_the_brief(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    conn = _FakeConn(slack_ts="9.9", postmortem_needed=True)
    sink = _RecordingSink(handle="9.9")
    assert consumer.drain_due_briefs(conn, sink=sink) == 1
    assert len(sink.calls) == 2
    assert sink.calls[1][0].status == "resolved"
    assert sink.calls[1][1] == "9.9"


def test_handle_and_drain_runs_delivery_on_the_message_path(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    conn = _FakeConn(due=(("INC_1", "api"),))
    sink = _RecordingSink(handle="1")
    consumer.handle_and_drain(conn, _open_json(), window_s=0, sink=sink)
    assert sink.calls, "a due brief must not wait for an idle Kafka poll"


def test_drain_waits_for_the_embedder_then_briefs_anyway(monkeypatch):
    """Status feeds are genuinely sparse: an empty timeline after the timeout is
    allowed, but it must be logged rather than silently briefed."""
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    conn = _FakeConn(indexed=0)
    slept = []
    n = consumer.wait_for_index(conn, "INC_1", timeout_s=1.0,
                                sleep=slept.append, now=iter([0.0, 0.5, 1.5]).__next__)
    assert n == 0 and slept, "it should have waited before giving up"


# --- a replayed topic must not page anyone about 2022 ------------------------

def _open_json_aged(hours):
    from datetime import UTC, datetime, timedelta
    ts = (datetime.now(UTC) - timedelta(hours=hours)).isoformat()
    return LifecycleEvent(type="opened", incident_id="INC_OLD", service="api", ts=ts).to_json()


def test_a_stale_open_is_not_scheduled(capsys):
    """A sample of the live topic held 1,429 opens, 10 of them under a day old.
    Without this guard a resubmitted Flink job briefs every historical incident."""
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, _open_json_aged(72), window_s=45, sink=StdoutSink())
    sql = " ".join(q for q, _ in conn.executed)
    assert "brief_due_at = now() +" not in sql, "a 3-day-old incident must not brief"
    assert "too old" in capsys.readouterr().out.lower()


def test_a_recent_open_is_still_scheduled():
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, _open_json_aged(1), window_s=45, sink=StdoutSink())
    assert any("brief_due_at = now() +" in q for q, _ in conn.executed)


def test_the_age_boundary_is_the_documented_constant():
    assert consumer.MAX_BRIEF_AGE_S == 24 * 3600


def test_an_exhausted_budget_defers_the_brief_instead_of_dropping_it(monkeypatch):
    """Over budget is a pause, not a failure: due_at stays set so the brief posts
    in the next window rather than being lost or posted degraded."""
    from freshet.rag.budget import BudgetExhausted

    def _over(*a, **k):
        raise BudgetExhausted("LLM hourly cap reached")
    monkeypatch.setattr(consumer, "gather_findings", _over)
    conn, sink = _FakeConn(), _RecordingSink(handle="9.9")

    assert consumer.drain_due_briefs(conn, sink=sink) == 0
    sql = " ".join(q for q, _ in conn.executed)
    assert "briefed_at = NULL" in sql, "the claim must be released for the retry"
    assert "brief_delivered_at = now()" not in sql
    assert "brief_due_at = NULL" not in sql, "the due-time must survive"
    assert sink.calls == []


def test_the_brief_path_refuses_to_run_without_a_composer():
    """main() built a BudgetedComposer and gave it to the thread poller only.
    _idle and _handle forwarded none, so the brief path fell through to
    `composer or make_composer()` and built a bare, UNBUDGETED composer —
    meaning the dominant LLM consumer, billed twice per incident, was never
    counted or capped. A missing composer is now a programming error."""
    from freshet.autopilot import investigate

    class _Conn:
        def execute(self, sql, params=None):
            self._sql = sql
            return self

        def fetchall(self):
            if "GROUP BY event_id" in self._sql:
                return [("evt_1", datetime(2026, 8, 22, 10, 0, tzinfo=UTC),
                         "Errors are elevated.", "api", "status_update", "alert")]
            return []

        def fetchone(self):
            return None

    with pytest.raises(ValueError, match="composer"):
        investigate.gather_findings(_Conn(), "api", "INC_1", "open")


def test_the_autopilot_idle_tick_forwards_its_composer():
    """A regression guard on the wiring itself: the budget only binds if the
    budgeted composer actually reaches drain_due_briefs."""
    import inspect

    from freshet.autopilot import __main__ as entry

    assert "composer" in inspect.signature(entry._idle).parameters
    assert "composer" in inspect.signature(entry._handle).parameters
    assert "composer=composer" in inspect.getsource(entry._idle)
    assert "composer=composer" in inspect.getsource(entry._handle)


def test_a_resolved_event_records_when_the_incident_actually_resolved():
    """resolved_at was READ by _format_duration and estimate_impact and WRITTEN
    by nothing, so a postmortem for an incident that had just resolved reported
    it as 'ongoing' and carried no duration."""
    class _Composer:
        def compose(self, question, hits):
            return "a summary"

    conn = _FakeConn()
    consumer.handle_lifecycle(conn, _resolved_json(), window_s=45, sink=StdoutSink(),
                              composer=_Composer())
    marks = [(q, p) for q, p in conn.executed if "resolved_at" in q and q.startswith("UPDATE")]
    assert marks, "the resolved transition must be recorded on the incident row"
    sql, params = marks[0]
    assert "coalesce(resolved_at" in sql, "redelivery must not move the timestamp"
    assert params[0] == datetime(2026, 7, 1, 0, 0, tzinfo=UTC), \
        "must use the provider's own transition time, not now()"


def test_the_resolution_is_recorded_even_when_the_postmortem_cannot_be_claimed():
    """The incident resolving is a FACT; posting a postmortem is our ACTION. The
    claim legitimately fails (already posted, or never briefed) — recording the
    fact only on the happy path is how resolved_at stayed NULL for every
    incident while the postmortem path itself worked."""
    conn = _FakeConn(claim_ok=False)
    consumer.handle_lifecycle(conn, _resolved_json(), window_s=45, sink=StdoutSink())
    assert any("resolved_at" in q and q.startswith("UPDATE") for q, _ in conn.executed)


def test_a_lifecycle_ts_without_an_offset_is_read_as_utc():
    """timestamptz reads a naive value in the SESSION time zone, which would
    shift every reported incident duration by that offset."""
    ev = consumer.LifecycleEvent(type="resolved", incident_id="INC_1",
                                 service="api", ts="2026-07-01T00:00:00")
    assert consumer._event_ts(ev).tzinfo is not None


# --- the successful-delivery path must be visible in the log ----------------

def test_a_delivered_brief_is_logged_by_incident_id(monkeypatch, capsys):
    """A nine-hour live run delivered 8 cited briefs to Slack and
    logs/autopilot.log recorded none of them — lsof showed the live process
    wrote zero bytes to that file. Only the database knew delivery happened.
    The module already logs every path where it DECLINES to brief; it must
    also log the one where it actually posts the flagship action."""
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", "bad deploy",
                                                 "[ev1 @ 2026-07-01 00:00:00]",
                                                 None, None, None, None))
    conn, sink = _FakeConn(), _RecordingSink(handle="9.9")
    assert consumer.drain_due_briefs(conn, sink=sink) == 1
    out = capsys.readouterr().out
    assert "INC_1" in out and "brief" in out.lower(), \
        f"a delivered brief must name the incident in the log, got: {out!r}"


def test_a_delivered_postmortem_is_logged_by_incident_id(monkeypatch, capsys):
    """The postmortem success path is the same silent gap as the brief path:
    the sink accepts it, the row is marked delivered, and nothing is printed."""
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    sink = _RecordingSink()
    consumer.handle_lifecycle(_FakeConn(slack_ts="9.9"), _resolved_json(),
                              window_s=0, sink=sink, sleep=lambda s: None)
    out = capsys.readouterr().out
    assert "INC_1" in out and "postmortem" in out.lower(), \
        f"a delivered postmortem must name the incident in the log, got: {out!r}"
