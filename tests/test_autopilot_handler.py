import pytest

from freshet.autopilot import consumer
from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.pipeline.lifecycle import LifecycleEvent


class _FakeConn:
    """Routes by SQL: RETURNING → claim result; SELECT slack_ts → the stored ts."""
    def __init__(self, *, claim_ok=True, slack_ts=None):
        self.claim_ok = claim_ok
        self.slack_ts = slack_ts
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        row = None
        if "RETURNING" in sql:
            row = ("INC_1",) if self.claim_ok else None
        elif "SELECT slack_ts" in sql:
            row = (self.slack_ts,)

        class _R:
            def __init__(self, r):
                self._r = r
            def fetchone(self):
                return self._r
        return _R(row)


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


def test_opened_briefs_once_when_claim_won(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", "bad deploy",
                                                 "[ev1 @ 2026-07-01 00:00:00]",
                                                 None, None, None, None))
    consumer.handle_lifecycle(_FakeConn(), object(), _open_json(),
                              window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out = capsys.readouterr().out
    assert "INCIDENT BRIEF" in out and "bad deploy" in out


def test_opened_persists_slack_ts_when_sink_returns_handle(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, object(), _open_json(),
                              window_s=0, sink=_RecordingSink(handle="9.9"), sleep=lambda s: None)
    # delivery + thread id land in ONE statement, so a crash cannot separate them
    assert any("brief_delivered_at = now()" in sql and "slack_ts" in sql
               and params == ("9.9", "INC_1") for sql, params in conn.executed)


def test_opened_leaves_slack_ts_untouched_when_handle_none(monkeypatch):
    """A sink with no handle (stdout, dry-run) still marks delivery, and passes NULL
    so the SQL's coalesce preserves any ts already stored."""
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, object(), _open_json(),
                              window_s=0, sink=_RecordingSink(handle=None), sleep=lambda s: None)
    marks = [(sql, params) for sql, params in conn.executed if "brief_delivered_at = now()" in sql]
    assert len(marks) == 1
    assert marks[0][1] == (None, "INC_1")
    assert "coalesce(%s, slack_ts)" in marks[0][0]


def test_opened_skips_when_claim_lost(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not brief")))
    consumer.handle_lifecycle(_FakeConn(claim_ok=False), object(), _open_json(),
                              window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    assert "already briefed" in capsys.readouterr().out.lower()


def test_resolved_posts_postmortem_threaded_under_slack_ts(monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    sink = _RecordingSink()
    consumer.handle_lifecycle(_FakeConn(slack_ts="9.9"), object(), _resolved_json(),
                              window_s=0, sink=sink, sleep=lambda s: None)
    assert len(sink.calls) == 1
    findings, thread = sink.calls[0]
    assert findings.status == "resolved" and thread == "9.9"


def test_resolved_skips_on_redelivery(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run")))
    sink = _RecordingSink()
    consumer.handle_lifecycle(_FakeConn(claim_ok=False), object(), _resolved_json(),
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


def test_a_failed_delivery_releases_the_claim_so_the_brief_is_not_lost_forever():
    """The claim exists to stop duplicate posts under at-least-once delivery. If
    it is kept when the work fails, a transient Slack or LLM error suppresses
    that incident's brief PERMANENTLY — the event is never redelivered to a
    consumer that would act on it."""
    from freshet.autopilot.consumer import handle_lifecycle

    class ExplodingSink:
        def deliver(self, findings, thread=None):
            raise RuntimeError("slack is down")

    conn = _RecordingConn()
    with pytest.raises(RuntimeError):
        handle_lifecycle(conn, object(), _lifecycle(), window_s=0,
                         sink=ExplodingSink(), sleep=lambda _: None)
    assert conn.released, "the claim must be released so a retry can brief it"


class _RaisingSink:
    def deliver(self, findings, *, thread=None):
        raise RuntimeError("slack down")


def test_failed_delivery_releases_the_claim_and_never_marks_delivered(monkeypatch):
    """The lease only works if a failed post propagates: marking a brief delivered
    that Slack rejected suppresses every future retry permanently."""
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, None))
    conn = _FakeConn()
    with pytest.raises(RuntimeError, match="slack down"):
        consumer.handle_lifecycle(conn, object(), _open_json(), window_s=0,
                                  sink=_RaisingSink(), sleep=lambda s: None)
    sql = " ".join(q for q, _ in conn.executed)
    assert "briefed_at = NULL" in sql          # claim released for the redelivery
    assert "brief_delivered_at = now()" not in sql   # never recorded as delivered


def test_failed_postmortem_releases_its_claim(monkeypatch):
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: _pm())
    conn = _FakeConn(slack_ts="1.2")
    with pytest.raises(RuntimeError, match="slack down"):
        consumer.handle_lifecycle(conn, object(), _resolved_json(), window_s=0,
                                  sink=_RaisingSink(), sleep=lambda s: None)
    sql = " ".join(q for q, _ in conn.executed)
    assert "postmortem_at = NULL" in sql
    assert "postmortem_delivered_at = now()" not in sql
