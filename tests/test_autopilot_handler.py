# tests/test_autopilot_handler.py
from freshet.autopilot import consumer
from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.pipeline.lifecycle import LifecycleEvent


class _FakeConn:
    """Returns a preset claim result; records if claim SQL ran."""
    def __init__(self, claim_ok):
        self._claim_ok = claim_ok
        self.claims = 0

    def execute(self, sql, params=None):
        self.claims += 1
        class _R:
            def __init__(self, ok):
                self._ok = ok
            def fetchone(self):
                return ("INC_1",) if self._ok else None
        return _R(self._claim_ok)


def _open_json():
    return LifecycleEvent("opened", "INC_1", "api", "2026-07-01T00:00:00+00:00").to_json()


def test_resolved_event_is_noop(monkeypatch):
    called = {"gather": 0}
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: called.__setitem__("gather", called["gather"] + 1))
    raw = LifecycleEvent("resolved", "INC_1", "api", "2026-07-01T00:00:00+00:00").to_json()
    consumer.handle_lifecycle(_FakeConn(True), object(), raw,
                              window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    assert called["gather"] == 0


def test_opened_briefs_once_when_claim_won(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: Findings("api", "open", "bad deploy",
                                                 "[ev1 @ 2026-07-01 00:00:00]",
                                                 None, None, None, None))
    consumer.handle_lifecycle(_FakeConn(True), object(), _open_json(),
                              window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out = capsys.readouterr().out
    assert "INCIDENT BRIEF" in out and "bad deploy" in out


def test_opened_skips_when_claim_lost(capsys, monkeypatch):
    monkeypatch.setattr(consumer, "gather_findings",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not brief")))
    consumer.handle_lifecycle(_FakeConn(False), object(), _open_json(),
                              window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out = capsys.readouterr().out
    assert "already briefed" in out.lower()
