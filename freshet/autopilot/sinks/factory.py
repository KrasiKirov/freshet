"""Select an output sink. Default `stdout` never posts; `slack` posts (and requires
credentials); `slack-dry-run` renders the Slack payload without posting."""

from __future__ import annotations

import os

from freshet.autopilot.sinks.base import Sink
from freshet.autopilot.sinks.slack import SlackSink
from freshet.autopilot.sinks.stdout import StdoutSink


def make_sink(kind: str = "stdout") -> Sink:
    if kind == "stdout":
        return StdoutSink()
    if kind == "slack":
        token = os.environ.get("SLACK_BOT_TOKEN")
        channel = os.environ.get("SLACK_CHANNEL")
        if not token or not channel:
            missing = [n for n, v in (("SLACK_BOT_TOKEN", token), ("SLACK_CHANNEL", channel)) if not v]
            raise RuntimeError(
                f"--sink slack requires {' and '.join(missing)} (set in .env.local)"
            )
        return SlackSink(token, channel)
    if kind == "slack-dry-run":
        return SlackSink(os.environ.get("SLACK_BOT_TOKEN", ""),
                         os.environ.get("SLACK_CHANNEL") or "#dry-run", dry_run=True)
    raise ValueError(f"unknown sink: {kind!r} (expected stdout, slack, or slack-dry-run)")
