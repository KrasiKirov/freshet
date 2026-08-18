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


def test_cause_is_quoted_from_the_providers_own_words():
    from freshet.autopilot.brief import cause_from_updates

    hits = [_hit(0, "We are investigating elevated error rates."),
            _hit(20, "The issue was caused by a misconfigured load balancer. "
                     "We are rolling back."),
            _hit(40, "This incident has been resolved.")]

    stated = cause_from_updates(hits)
    assert stated is not None
    text, cite = stated
    assert text == "The issue was caused by a misconfigured load balancer."
    assert "[e20 @" in cite


def test_no_cause_is_claimed_when_none_is_stated():
    """The whole point. Status updates usually announce progress, not causes;
    inventing one would be worse than saying nothing."""
    from freshet.autopilot.brief import cause_from_updates

    hits = [_hit(0, "We are investigating reports of degraded performance."),
            _hit(10, "We are continuing to monitor for any further issues."),
            _hit(20, "This incident has been resolved.")]
    assert cause_from_updates(hits) is None


def test_identified_alone_is_not_a_cause_statement():
    """"We have identified the issue" names nothing. It is a status, not a cause."""
    from freshet.autopilot.brief import cause_from_updates

    assert cause_from_updates([_hit(0, "We have identified the issue.")]) is None
    stated = cause_from_updates([_hit(0, "We identified the source of a "
                                         "communication failure between services.")])
    assert stated is not None and "communication failure" in stated[0]


def test_the_earliest_stated_cause_wins():
    from freshet.autopilot.brief import cause_from_updates

    hits = [_hit(30, "Resolved. This was due to a bad deploy."),
            _hit(10, "The outage was caused by a database failover.")]
    text, cite = cause_from_updates(hits)
    assert "database failover" in text
    assert "[e10 @" in cite


def test_only_the_causal_sentence_is_quoted_not_the_whole_update():
    from freshet.autopilot.brief import cause_from_updates

    hits = [_hit(5, "Thanks for your patience. The outage was caused by an expired "
                    "certificate. A full postmortem will follow shortly.")]
    text, _ = cause_from_updates(hits)
    assert text == "The outage was caused by an expired certificate."
    assert "postmortem" not in text


def test_a_promised_future_rca_is_not_a_cause():
    """Real string from GitHub's feed. "root cause" appears, but the sentence
    promises a future analysis — quoting it as the cause is invention."""
    from freshet.autopilot.brief import cause_from_updates

    assert cause_from_updates([_hit(0,
        "This incident has been resolved. A detailed root cause analysis will "
        "be shared as soon as it is available.")]) is None


def test_saying_the_cause_was_found_is_not_saying_what_it_was():
    """Real strings from Cloudflare and GitHub. Both announce that a cause was
    identified without naming it; the brief must not present them as a cause."""
    from freshet.autopilot.brief import cause_from_updates

    assert cause_from_updates([_hit(0,
        "We have identified the root cause and reverted the impacted change.")]) is None
    assert cause_from_updates([_hit(0,
        "We identified the source of the issue affecting creation of "
        "fine-grained personal access tokens and have applied a mitigation.")]) is None


def test_a_named_cause_still_survives_the_stricter_rules():
    """Real string from GitHub — this one DOES name something."""
    from freshet.autopilot.brief import cause_from_updates

    stated = cause_from_updates([_hit(0,
        "Our engineering teams are actively investigating the root cause, which "
        "appears to be related to a database infrastructure issue.")])
    assert stated is not None and "database infrastructure" in stated[0]


def test_an_ongoing_investigation_is_not_a_cause():
    """Real string from GitHub's feed. Mentions "root cause" while saying only
    that the investigation continues."""
    from freshet.autopilot.brief import cause_from_updates

    assert cause_from_updates([_hit(0,
        "Investigations are on-going into the root cause, and updates will "
        "continue to be provided as we investigate.")]) is None


def test_a_hypothesis_with_a_named_subject_still_counts():
    """Contrast with the above: also an in-progress investigation, but it names
    what the cause appears to be, which is what a responder needs."""
    from freshet.autopilot.brief import cause_from_updates

    stated = cause_from_updates([_hit(0,
        "Our engineering teams are actively investigating the root cause, which "
        "appears to be related to a database infrastructure issue.")])
    assert stated is not None
