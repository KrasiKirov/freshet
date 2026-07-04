"""The Sink protocol: an output destination for an incident brief. Each sink owns
its own formatting (a terminal and Slack want different renderings)."""

from __future__ import annotations

from typing import Optional, Protocol

from freshet.autopilot.brief import Findings


class Sink(Protocol):
    def deliver(self, findings: Findings, *, thread: Optional[str] = None) -> Optional[str]: ...
