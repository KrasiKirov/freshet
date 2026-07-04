"""Slack sink: build a small Block Kit layout from Findings and deliver it via
chat.postMessage. slack_sdk is lazy-imported and only when actually posting, so the
keyless core (and CI without the [slack] extra) never imports it."""

from __future__ import annotations

from typing import Optional

from freshet.autopilot.brief import Findings, render_brief

_EMOJI = {"open": "🔴", "investigating": "🔴", "identified": "🔴",
          "monitoring": "🟠", "resolved": "🟢", "postmortem": "🟢"}


def _emoji(status: str) -> str:
    return _EMOJI.get((status or "").lower(), "🔴")


def slack_blocks(f: Findings) -> list[dict]:
    header = {"type": "header",
              "text": {"type": "plain_text", "text": f"{_emoji(f.status)} {f.service} — {f.status}"}}
    if f.narrative:
        body = f.narrative
    else:
        cause = (f"*Cause:* {f.cause_text} `{f.cause_cite}`" if f.cause_text
                 else "*Cause:* not identified from retrieved evidence")
        resolution = (f"*Resolution:* {f.fix_text} `{f.fix_cite}`" if f.fix_text
                      else "*Resolution:* not identified from retrieved evidence")
        body = f"{cause}\n{resolution}"
    section = {"type": "section", "text": {"type": "mrkdwn", "text": body}}
    runbook = f"Runbook: {f.runbook}" if f.runbook else "Runbook: none found"
    parts = [runbook]
    if f.impact:
        parts.insert(0, f.impact)
    if f.meta:
        parts.insert(0, f.meta)
    context = {"type": "context", "elements": [{"type": "mrkdwn", "text": "\n".join(parts)}]}
    return [header, section, context]


class SlackSink:
    def __init__(self, token: str, channel: str, dry_run: bool = False, client=None):
        self._token = token
        self._channel = channel
        self._dry_run = dry_run
        self._client = client  # injection seam for tests; None in production

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def deliver(self, findings: Findings, *, thread: Optional[str] = None) -> Optional[str]:
        blocks = slack_blocks(findings)
        text = render_brief(findings)  # plain-text notification fallback
        if self._dry_run:
            print(f"[slack-dry-run] channel={self._channel} thread={thread}\ntext={text}\nblocks={blocks}")
            return None
        client = self._client
        if client is None:
            try:
                from slack_sdk import WebClient  # lazy: only when actually posting
            except ImportError as exc:
                raise ImportError(
                    "Slack posting needs slack_sdk: pip install -e \".[slack]\""
                ) from exc
            client = WebClient(token=self._token)
        try:
            resp = client.chat_postMessage(channel=self._channel, text=text,
                                           blocks=blocks, thread_ts=thread)
            return resp["ts"] if resp is not None else None
        except Exception as exc:  # never crash the autopilot loop on a delivery failure
            print(f"[slack] post failed: {exc!r}")
            return None
