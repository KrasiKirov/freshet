"""Slack sink: build a small Block Kit layout from Findings and deliver it via
chat.postMessage. slack_sdk is lazy-imported and only when actually posting, so the
keyless core (and CI without the [slack] extra) never imports it."""

from __future__ import annotations

from freshet.autopilot.brief import Findings, IMPACT_STUB, render_brief

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
    context = {"type": "context",
               "elements": [{"type": "mrkdwn", "text": f"{runbook}\n{IMPACT_STUB}"}]}
    return [header, section, context]
