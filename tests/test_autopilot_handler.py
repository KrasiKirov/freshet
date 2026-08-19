import pytest

from freshet.autopilot import consumer
from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.pipeline.lifecycle import LifecycleEvent


class _FakeConn:
    """Routes by SQL: RETURNING → claim; slack_ts → stored ts; due → scheduled
    briefs; vector_records count → how much evidence is indexed."""
    def __init__(self, *, claim_ok=True, slack_ts=None, due=(("INC_1", "api"),), indexed=3):
        self.claim_ok = claim_ok
        self.slack_ts = slack_ts
        self.due = list(due)
        self.indexed = indexed
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        row, rows = None, []
        if "RETURNING" in sql:
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


def _open_json():
    return LifecycleEvent("opened", "INC_1", "api", "2026-07-01T00:00:00+00:00").to_json()


def _resolved_json():
    return LifecycleEvent("resolved", "INC_1", "api", "2026-07-01T00:00:00+00:00").to_json()


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
    assert any("brief_delivered_at = now()" in q and p == ("9.9", "INC_1")
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
