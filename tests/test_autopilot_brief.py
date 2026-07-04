from dataclasses import dataclass
from datetime import datetime, timezone

from freshet.autopilot.brief import (
    Findings, cite_hit, findings_from_timeline, render_brief,
)


@dataclass
class _Hit:  # minimal stand-in for RetrievedHit
    event_id: str
    ts: datetime
    text: str
    service: str = "scheduler-api"


def test_cite_hit_format():
    h = _Hit("ev1", datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc), "deploy X")
    assert cite_hit(h) == "[ev1 @ 2026-07-01 12:00:00]"


def test_render_includes_cause_runbook_and_status():
    f = Findings(service="scheduler-api", status="open",
                 cause_text="bad deploy", cause_cite="[ev1 @ 2026-07-01 12:00:00]",
                 fix_text=None, fix_cite=None, runbook="restart the worker", narrative=None)
    out = render_brief(f)
    assert "scheduler-api" in out
    assert "bad deploy" in out and "[ev1 @ 2026-07-01 12:00:00]" in out
    assert "restart the worker" in out
    assert "estimation pending" not in out  # the ④ stub is gone


def test_render_shows_impact_when_set():
    from freshet.autopilot.brief import Findings, render_brief
    f = Findings(service="api", status="open", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=None, narrative="n",
                 impact="Impact: High — 3 services, ongoing")
    assert "Impact: High — 3 services, ongoing" in render_brief(f)


def test_findings_from_timeline_uses_cause_hit():
    tl_cause = _Hit("evC", datetime(2026, 7, 1, 9, 0, 0, tzinfo=timezone.utc), "rollout")

    @dataclass
    class _TL:
        service: str
        cause: object
        fix: object
    tl = _TL(service="api", cause=tl_cause, fix=None)
    f = findings_from_timeline(tl, status="open", runbook=None)
    assert f.service == "api" and f.cause_text == "rollout"
    assert f.cause_cite == "[evC @ 2026-07-01 09:00:00]" and f.fix_text is None


def test_render_prefers_narrative_when_present():
    f = Findings(service="api", status="resolved", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=None,
                 narrative="Cause: bad deploy [evX @ 2026-07-01 09:00:00].")
    out = render_brief(f)
    assert "bad deploy [evX @ 2026-07-01 09:00:00]" in out


def test_meta_renders_when_present():
    from freshet.autopilot.brief import Findings, render_brief
    f = Findings(service="api", status="resolved", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=None,
                 narrative="Root cause: bad deploy.", meta="Duration 42m · rolled back")
    out = render_brief(f)
    assert "POSTMORTEM" in out and "Duration 42m · rolled back" in out


def test_meta_absent_by_default_leaves_brief_unchanged():
    from freshet.autopilot.brief import Findings, render_brief
    f = Findings(service="api", status="open", cause_text="bad deploy",
                 cause_cite="[ev1 @ 2026-07-01 00:00:00]", fix_text=None, fix_cite=None,
                 runbook="rb", narrative=None)
    out = render_brief(f)
    assert "INCIDENT BRIEF" in out and "Duration" not in out
