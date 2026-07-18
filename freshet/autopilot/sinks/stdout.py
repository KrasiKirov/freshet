"""Default, keyless sink: render the plain-text brief and print it (identical to
sub-project ①'s original stdout behaviour)."""

from __future__ import annotations

from freshet.autopilot.brief import Findings, render_brief


class StdoutSink:
    def deliver(self, findings: Findings, *, thread: str | None = None) -> str | None:
        print(render_brief(findings))
        if thread:
            print(f"(reply to {thread})")
        return None
