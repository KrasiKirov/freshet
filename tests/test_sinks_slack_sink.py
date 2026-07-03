from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.slack import SlackSink


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
    SlackSink(token="x", channel="#c", client=fake).deliver(_f())
    assert len(fake.calls) == 1
    assert fake.calls[0]["channel"] == "#c"
    assert fake.calls[0]["blocks"][0]["type"] == "header"
    assert fake.calls[0]["text"]  # non-empty plain-text fallback


class _BoomClient:
    def chat_postMessage(self, **kw):
        raise RuntimeError("boom")


def test_post_failure_is_swallowed_and_logged(capsys):
    SlackSink(token="x", channel="#c", client=_BoomClient()).deliver(_f())  # must NOT raise
    assert "post failed" in capsys.readouterr().out.lower()
