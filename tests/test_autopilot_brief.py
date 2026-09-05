from dataclasses import dataclass
from datetime import UTC, datetime

from freshet.autopilot.brief import (
    Findings,
    cite_hit,
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


def test_update_lines_cites_every_line_newest_first():
    from freshet.autopilot.brief import update_lines

    hits = [_hit(0, "We are investigating elevated errors."),
            _hit(30, "A fix has been implemented."),
            _hit(15, "The issue has been identified.")]

    lines = update_lines(hits)

    assert len(lines) == 3
    assert "12:30" in lines[0], "newest update must come first"
    assert "12:00" in lines[-1]
    for line in lines:
        assert "[e" in line and "@" in line, f"uncited update line: {line}"


def test_update_lines_caps_the_brief_and_truncates_long_text():
    from freshet.autopilot.brief import MAX_UPDATES, update_lines

    hits = [_hit(i, "x" * 400) for i in range(MAX_UPDATES + 5)]
    lines = update_lines(hits)
    assert len(lines) == MAX_UPDATES, "a Slack brief must stay skimmable"
    assert all(len(line) < 300 for line in lines)


def test_update_lines_collapses_whitespace():
    from freshet.autopilot.brief import update_lines

    lines = update_lines([_hit(1, "a\n\n  b\tc")])
    assert "a b c" in lines[0]


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
    """"We have identified the issue" names nothing. It is a status, not a cause.

    REVERSED 2026-09-03. This case previously asserted that "identified the
    source of a communication failure between services" IS a cause. Measured
    against the live 42-provider index, that construction is an announcement:
    the provider says they found the source of a SYMPTOM without saying what the
    source was. Its siblings in the feeds are "identified the source of load",
    "of errors", "of latency", "of packet loss" — 13 of 243 detector hits, all
    reaching the brief's Cause line as if they were diagnoses. The module's own
    rule is that reporting one "would be inventing content the provider never
    gave", and this is that. A sentence that goes on to name something ("...as a
    misconfigured load balancer", "...root cause: an expired certificate") still
    counts — see test_a_cause_named_after_identifying_it_still_survives.
    """
    from freshet.autopilot.brief import cause_from_updates

    assert cause_from_updates([_hit(0, "We have identified the issue.")]) is None
    assert cause_from_updates([_hit(0, "We identified the source of a "
                                       "communication failure between services.")]) is None


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


def test_render_brief_shows_a_summary_and_the_cause_together():
    """The narrative used to REPLACE the cause line. With the LLM as the default
    author, the summary is prose and the cause is a verbatim provider quote —
    a responder wants both."""
    from freshet.autopilot.brief import Findings, render_brief

    f = Findings(service="github", status="opened",
                 cause_text="The outage was caused by an expired certificate.",
                 cause_cite="[e1 @ 2026-08-18 12:00:00]",
                 fix_text=None, fix_cite=None, runbook=None,
                 narrative="GitHub is investigating elevated errors [e1 @ ...].",
                 updates=["12:00 — something [e1 @ ...]"])
    text = render_brief(f)
    assert "GitHub is investigating" in text, "summary must render"
    assert "expired certificate" in text, "cause must survive alongside the summary"
    assert "Updates:" in text




def test_the_narrative_sees_a_bounded_window_of_updates():
    """p50 is 3 updates, but the tail runs to 179 — ~58k input tokens, billed
    twice per incident. Deterministic extraction still reads every update."""
    from freshet.autopilot.investigate import MAX_NARRATIVE_UPDATES, _summarise

    seen = {}

    class _C:
        def compose(self, question, hits):
            seen["n"] = len(list(hits))
            return "narrative"

    class _U:
        def __init__(self, i):
            self.event_id, self.text, self.source = f"e{i}", "text", "alert"
            from datetime import UTC, datetime
            self.ts = datetime.now(UTC)

    _summarise([_U(i) for i in range(200)], _C(), "q")
    assert seen["n"] == MAX_NARRATIVE_UPDATES == 20


def test_a_short_incident_is_not_padded_or_truncated():
    from freshet.autopilot.investigate import _summarise

    seen = {}

    class _C:
        def compose(self, question, hits):
            seen["n"] = len(list(hits))
            return "narrative"

    class _U:
        def __init__(self, i):
            self.event_id, self.text, self.source = f"e{i}", "t", "alert"
            from datetime import UTC, datetime
            self.ts = datetime.now(UTC)

    _summarise([_U(i) for i in range(3)], _C(), "q")
    assert seen["n"] == 3


def test_a_cause_in_an_unterminated_final_sentence_is_found():
    """`[^.!?]+[.!?]` required terminal punctuation, and status-feed updates
    frequently omit it on the last sentence — so the cause was invisible."""
    from freshet.autopilot.brief import _cause_sentence

    assert _cause_sentence("This was caused by a bad deploy") == \
        "This was caused by a bad deploy"


def test_an_investigation_in_progress_is_not_reported_as_a_cause():
    """"root cause" passed every filter in a sentence that names no cause."""
    from freshet.autopilot.brief import _cause_sentence

    assert _cause_sentence("We are still investigating the root cause.") is None
    assert _cause_sentence("The root cause is still unknown.") is None


def test_a_named_cause_survives_a_continuing_investigation_in_the_same_sentence():
    from freshet.autopilot.brief import _cause_sentence

    text = ("The outage was caused by an expired certificate; we are still "
            "investigating the full customer impact.")
    assert _cause_sentence(text) is not None


def test_a_long_update_is_clipped_at_a_sentence_boundary_not_mid_word():
    """text[:200] put back exactly the mid-sentence fragment the chunker was
    fixed to stop producing (3fca358)."""
    from freshet.autopilot.brief import update_lines

    sentence = "Customers may see elevated error rates on write requests. "
    [line] = update_lines([_hit(1, sentence * 6)])
    body = line.split(" — ", 1)[1]
    body = body[:body.rindex(" [")]
    assert body.endswith("...")
    assert body.removesuffix("...").endswith("requests."), body


def test_a_brief_and_a_postmortem_are_assembled_by_the_same_code():
    """They were ~80% duplicated, in different orders, and the drift was real:
    the postmortem's narrative once bypassed citation verification entirely."""
    import inspect

    from freshet.autopilot import investigate

    for fn in (investigate.gather_findings, investigate.gather_postmortem):
        assert "_gather(" in inspect.getsource(fn), f"{fn.__name__} still duplicates"


def test_an_incident_update_satisfies_the_composer_protocol():
    """`_Update` structurally duck-typed RetrievedHit, and compose() was
    annotated list[RetrievedHit] — a lie mypy could not see."""
    from datetime import UTC, datetime

    from freshet.autopilot.investigate import _Update
    from freshet.rag.composer import Cited

    u = _Update(event_id="evt_1", ts=datetime.now(UTC), text="x",
                service="api", type="status_update")
    assert isinstance(u, Cited)


# each was surfaced as a "Cause" by the detector, and none of them names one —
# they announce that the cause was found or fixed, or name the symptom traced
_ANNOUNCEMENTS_NOT_CAUSES = [
    "The root cause has been fixed, and we are monitoring recovery.",
    "The root cause has been addressed, and some sessions are starting to see recovery.",
    "The root cause has been resolved, and no data was lost.",
    "We have identified the source of load and addressed it, and are monitoring recovery.",
    "We identified the source of errors affecting Pull Requests, Issues, and Search.",
    "We've identified the cause of an issue impacting Log writes.",
    "Our team has identified the cause of packet loss to our US data centers.",
    "We've identified the source of latency and are reverting that change.",
    "We have identified the cause of delays in DNS records creation.",
]


def test_announcing_that_the_cause_was_found_or_fixed_is_not_a_cause():
    """Measured on the live index: 13 of 243 detector hits were these. The
    filter only caught "identified the source of THE|THIS", so a bare symptom
    noun ("of load", "of packet loss") walked straight through — and the Cause
    line in the Slack brief quoted an announcement as if it were a diagnosis."""
    from freshet.autopilot.brief import _cause_sentence

    for text in _ANNOUNCEMENTS_NOT_CAUSES:
        assert _cause_sentence(text) is None, text


def test_a_cause_named_after_identifying_it_still_survives():
    """The rejection must not swallow the sentences that DO name something: the
    distinction is whether the provider says what it was, not whether the word
    'identified' appears."""
    from freshet.autopilot.brief import _cause_sentence

    for text in (
        "We identified the cause of the outage as a misconfigured load balancer.",
        "We have identified the root cause: an expired TLS certificate.",
        "The outage was caused by an expired certificate.",
        "Our teams are investigating the root cause, which appears to be related "
        "to a database infrastructure issue.",
    ):
        assert _cause_sentence(text) is not None, text


def test_identifying_the_root_cause_OF_something_is_still_an_announcement():
    """The rule covered "identified the cause of" but not "identified the ROOT
    cause of", so "We have identified the root cause of this issue and fixed it"
    survived — found and fixed, naming nothing. My own false-positive counter
    had the same blind spot and reported zero while this was still getting
    through."""
    from freshet.autopilot.brief import _cause_sentence

    assert _cause_sentence("We have identified the root cause of this issue "
                           "and fixed it.") is None
    assert _cause_sentence("We have identified the root cause of the Sign-ups "
                           "and Billing outage.") is None
    # still rescued when it actually names one
    assert _cause_sentence("The root cause of the incident was traced to a code "
                           "change which dropped change events.") is not None


def test_more_ways_of_saying_the_investigation_is_ongoing():
    """Same class as "still investigating the root cause", different words. Both
    strings are verbatim from the live index."""
    from freshet.autopilot.brief import _cause_sentence

    assert _cause_sentence("We are in the process of investigating the root "
                           "cause of this incident.") is None
    assert _cause_sentence("We have mitigated the problem and continue looking "
                           "into the root cause.") is None
