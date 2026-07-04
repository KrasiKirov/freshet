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
    assert any("UPDATE incidents SET slack_ts" in sql and params == ("9.9", "INC_1")
               for sql, params in conn.executed)


def test_opened_no_slack_ts_update_when_handle_none(monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", None, None, None, None, None, "n"))
    conn = _FakeConn()
    consumer.handle_lifecycle(conn, object(), _open_json(),
                              window_s=0, sink=_RecordingSink(handle=None), sleep=lambda s: None)
    assert not any("UPDATE incidents SET slack_ts" in sql for sql, _ in conn.executed)


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
