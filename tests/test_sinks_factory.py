import pytest

from freshet.autopilot.sinks.factory import make_sink
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.autopilot.sinks.slack import SlackSink


def test_default_is_stdout():
    assert isinstance(make_sink(), StdoutSink)
    assert isinstance(make_sink("stdout"), StdoutSink)


def test_slack_missing_creds_raises_naming_the_var(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_CHANNEL", raising=False)
    with pytest.raises(RuntimeError) as exc:
        make_sink("slack")
    assert "SLACK_BOT_TOKEN" in str(exc.value)


def test_slack_with_creds_returns_live_sink(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("SLACK_CHANNEL", "#c")
    s = make_sink("slack")
    assert isinstance(s, SlackSink) and s.dry_run is False


def test_dry_run_needs_no_token(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_CHANNEL", raising=False)
    s = make_sink("slack-dry-run")
    assert isinstance(s, SlackSink) and s.dry_run is True


def test_unknown_kind_raises():
    with pytest.raises(ValueError):
        make_sink("carrier-pigeon")
