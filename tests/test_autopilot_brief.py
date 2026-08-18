from dataclasses import dataclass
from datetime import UTC, datetime

from freshet.autopilot.brief import (
    Findings,
    cite_hit,
    findings_from_timeline,
    render_brief,
)


@dataclass
class _Hit:  # minimal stand-in for RetrievedHit
    event_id: str
    ts: datetime
    text: str
    service: str = "scheduler-api"


def test_cite_hit_format():
    h = _Hit("ev1", datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC), "deploy X")
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
    tl_cause = _Hit("evC", datetime(2026, 7, 1, 9, 0, 0, tzinfo=UTC), "rollout")

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


def _hit(minute, text, eid=None):
    from datetime import UTC, datetime
    from types import SimpleNamespace
    return SimpleNamespace(event_id=eid or f"e{minute}", text=text,
                           ts=datetime(2026, 8, 18, 12, minute, tzinfo=UTC))


def test_findings_from_updates_cites_every_line_newest_first():
    from freshet.autopilot.brief import findings_from_updates

    hits = [_hit(0, "We are investigating elevated errors."),
            _hit(30, "A fix has been implemented."),
            _hit(15, "The issue has been identified.")]

    f = findings_from_updates("github", "opened", hits, runbook=None)

    assert len(f.updates) == 3
    assert "12:30" in f.updates[0], "newest update must come first"
    assert "12:00" in f.updates[-1]
    for line in f.updates:
        assert "[e" in line and "@" in line, f"uncited update line: {line}"


def test_findings_from_updates_caps_the_brief_and_truncates_long_text():
    from freshet.autopilot.brief import MAX_UPDATES, findings_from_updates

    hits = [_hit(i, "x" * 400) for i in range(MAX_UPDATES + 5)]
    f = findings_from_updates("github", "opened", hits, runbook=None)
    assert len(f.updates) == MAX_UPDATES, "a Slack brief must stay skimmable"
    assert all(len(line) < 300 for line in f.updates)


def test_findings_from_updates_collapses_whitespace():
    from freshet.autopilot.brief import findings_from_updates

    f = findings_from_updates("x", "opened", [_hit(1, "a\n\n  b\tc")], runbook=None)
    assert "a b c" in f.updates[0]


def test_render_brief_shows_the_update_timeline():
    from freshet.autopilot.brief import Findings, render_brief

    f = Findings(service="github", status="opened", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=None, narrative=None,
                 updates=["12:30 — A fix has been implemented. [e30 @ 2026-08-18 12:30:00]"])
    text = render_brief(f)
    assert "Updates:" in text
    assert "A fix has been implemented." in text


def test_render_brief_omits_the_section_when_there_are_no_updates():
    from freshet.autopilot.brief import Findings, render_brief

    f = Findings(service="x", status="opened", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=None, narrative=None)
    assert "Updates:" not in render_brief(f)
