"""Slack sink: build a small Block Kit layout from Findings and deliver it via
chat.postMessage. slack_sdk is lazy-imported and only when actually posting, so the
core (and CI without the [slack] extra) never imports it."""

from __future__ import annotations

import re
import time

from freshet.autopilot.brief import Findings, render_brief

# A transient Slack error (429, a blip) must not take the autopilot down, but an
# undeliverable brief must NOT be reported as delivered: the consumer marks the
# incident delivered on return, which would permanently suppress the retry. So:
# retry a few times, then RAISE. The consumer releases its claim and the Kafka
# offset stays uncommitted, so the brief is redelivered rather than lost.
MAX_ATTEMPTS = 3
RETRY_BASE_S = 2.0

_EMOJI = {"open": "🔴", "investigating": "🔴", "identified": "🔴",
          "monitoring": "🟠", "resolved": "🟢", "postmortem": "🟢"}

# The LLM narrative is standard Markdown, but Slack section blocks use *mrkdwn*,
# where bold is single asterisks and there are no ATX headings — so `**bold**` and
# `## Heading` render literally unless converted first.
_MD_BOLD = re.compile(r"\*\*(.+?)\*\*", re.S)
_MD_HEADING = re.compile(r"(?m)^[ \t]{0,3}#{1,6}[ \t]+(.*?)[ \t]*#*[ \t]*$")


def _to_mrkdwn(text: str) -> str:
    """Convert standard Markdown to Slack mrkdwn: `## H` -> `*H*`, `**b**` -> `*b*`.
    Leaves single-asterisk bold and `[event_id @ ts]` citations untouched."""
    text = _MD_HEADING.sub(r"*\1*", text)
    text = _MD_BOLD.sub(r"*\1*", text)
    return text


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
    section = {"type": "section", "text": {"type": "mrkdwn", "text": _to_mrkdwn(body)}}
    runbook = f"Runbook: {f.runbook}" if f.runbook else "Runbook: none found"
    parts = [runbook]
    if f.impact:
        parts.insert(0, f.impact)
    if f.meta:
        parts.insert(0, f.meta)
    context = {"type": "context", "elements": [{"type": "mrkdwn", "text": "\n".join(parts)}]}
    return [header, section, context]


class SlackSink:
    def __init__(self, token: str, channel: str, dry_run: bool = False, client=None,
                 sleep=time.sleep):
        self._token = token
        self._channel = channel
        self._dry_run = dry_run
        self._client = client  # injection seam for tests; None in production
        self._sleep = sleep    # injection seam: tests must not actually back off

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def deliver(self, findings: Findings, *, thread: str | None = None) -> str | None:
        blocks = slack_blocks(findings)
        text = render_brief(findings)  # plain-text notification fallback
        if self._dry_run:
            # text on its own line: it starts with "=== ... ===", so an inline
            # "text=" prefix renders as "text====" and is hard to read
            print(f"[slack-dry-run] channel={self._channel} thread={thread}\n"
                  f"{text}\nblocks={blocks}")
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
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = client.chat_postMessage(channel=self._channel, text=text,
                                               blocks=blocks, thread_ts=thread)
                return resp["ts"] if resp is not None else None
            except Exception as exc:
                if attempt == MAX_ATTEMPTS:
                    raise
                print(f"[slack] post failed (attempt {attempt}/{MAX_ATTEMPTS}): "
                      f"{exc!r}; retrying in {RETRY_BASE_S * attempt:.0f}s")
                self._sleep(RETRY_BASE_S * attempt)
        raise AssertionError("unreachable: the loop returns or raises")
