import pytest

from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.slack import MAX_ATTEMPTS, RETRY_BASE_S, SlackSink


def _f():
    return Findings(service="api", status="open", cause_text="bad deploy",
                    cause_cite="[ev1 @ 2026-07-01 00:00:00]", fix_text=None, fix_cite=None,
                    runbook="rb", narrative=None)


def test_dry_run_prints_and_makes_no_call(capsys):
    # slack_sdk is NOT installed in CI; dry-run must not import it or hit the network.
    SlackSink(token="", channel="#c", dry_run=True).deliver(_f())
    out = capsys.readouterr().out
    assert "#c" in out and "bad deploy" in out


class _FakeClient:
    def __init__(self):
        self.calls = []

    def chat_postMessage(self, **kw):
        self.calls.append(kw)
        return {"ok": True, "ts": "1.2"}


def test_posts_once_with_channel_and_blocks():
    fake = _FakeClient()
    ret = SlackSink(token="x", channel="#c", client=fake).deliver(_f())
    assert len(fake.calls) == 1
    assert fake.calls[0]["channel"] == "#c"
    assert fake.calls[0]["blocks"][0]["type"] == "header"
    assert fake.calls[0]["text"]  # non-empty plain-text fallback
    assert ret == "1.2"


def test_deliver_returns_ts():
    fake = _FakeClient()
    ret = SlackSink(token="x", channel="#c", client=fake).deliver(_f())
    assert ret == "1.2"


def test_deliver_passes_thread_ts():
    fake = _FakeClient()
    SlackSink(token="x", channel="#c", client=fake).deliver(_f(), thread="9.9")
    assert fake.calls[0]["thread_ts"] == "9.9"


def test_dry_run_returns_none_and_shows_thread(capsys):
    ret = SlackSink(token="", channel="#c", dry_run=True).deliver(_f(), thread="9.9")
    out = capsys.readouterr().out
    assert ret is None and "9.9" in out


class _BoomClient:
    """Fails `fails` times, then succeeds (always, if fails is None)."""
    def __init__(self, fails=None):
        self.fails = fails
        self.attempts = 0

    def chat_postMessage(self, **kw):
        self.attempts += 1
        if self.fails is None or self.attempts <= self.fails:
            raise RuntimeError("boom")
        return {"ok": True, "ts": "1.2"}


def test_persistent_post_failure_raises():
    # Regression: this used to be swallowed and return None, so the consumer marked
    # the brief delivered and no retry could ever fire again. Delivery failures MUST
    # propagate — the consumer releases its claim and the Kafka offset is not committed.
    client = _BoomClient()
    sink = SlackSink(token="x", channel="#c", client=client, sleep=lambda s: None)
    with pytest.raises(RuntimeError, match="boom"):
        sink.deliver(_f())
    assert client.attempts == MAX_ATTEMPTS


def test_transient_failure_is_retried_then_succeeds(capsys):
    # A 429 or a blip must not take the autopilot down for an incident it can deliver.
    client = _BoomClient(fails=MAX_ATTEMPTS - 1)
    slept = []
    sink = SlackSink(token="x", channel="#c", client=client, sleep=slept.append)
    assert sink.deliver(_f()) == "1.2"
    assert client.attempts == MAX_ATTEMPTS
    assert slept == [RETRY_BASE_S * n for n in range(1, MAX_ATTEMPTS)]  # backs off
    assert "retrying" in capsys.readouterr().out.lower()
