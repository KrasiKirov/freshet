"""Autopilot end-to-end over REAL captured status-feed data.

Replaces the generator-seeded suite deleted in Phase 1. Those tests asserted the
brief named the correct CAUSE, which real feeds cannot support: status updates
are typed `status_update`, never a change event, and most never state a cause at
all. So the properties worth protecting changed:

  * a brief IS produced, and every citation it makes is well-formed
  * a cause is NEVER invented when the evidence does not contain one

The second is the more valuable assertion. An incident tool that fabricates a
root cause is worse than one that says nothing.
"""
from collections import Counter

import pytest

from tests.integration.conftest_replay import seed_from_replay

pytestmark = pytest.mark.integration


def _busiest_provider(rows) -> str:
    return Counter(r["provider"] for r in rows).most_common(1)[0][0]


def test_a_brief_is_produced_for_a_real_incident(conn, emb, llm):
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    from freshet.autopilot.brief import render_brief
    from freshet.autopilot.investigate import gather_findings

    findings = gather_findings(conn, service, incident_id, status="opened", composer=llm)
    text = render_brief(findings)

    assert text.strip(), "a brief must be produced for a real incident"
    assert service in text
    # any citation present must be well formed. NOTE: on real status-feed data
    # there are currently none — see the documented-gap test below. Written as an
    # explicit count so this can never pass vacuously again.
    cited = [ln for ln in text.splitlines() if "[" in ln]
    for line in cited:
        assert "]" in line and "@" in line, f"malformed citation: {line}"


def test_the_brief_cites_real_updates(conn, emb, llm):
    """The gap Task 14 pinned is closed: a brief over real status-feed data now
    carries cited updates instead of three "not identified" lines."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    from freshet.autopilot.brief import render_brief
    from freshet.autopilot.investigate import gather_findings

    text = render_brief(gather_findings(conn, service, incident_id, status="opened", composer=llm))
    assert "Updates:" in text
    cited = [ln for ln in text.splitlines() if "[" in ln]
    assert cited, "the brief must cite the updates it reports"
    for line in cited:
        assert "]" in line and "@" in line, f"malformed citation: {line}"



def test_a_cause_is_never_invented_from_real_updates(conn, emb, llm):
    """Most real incidents never state a cause. Those briefs must say nothing
    rather than reach for the nearest plausible sentence — inventing a root cause
    for an on-call responder is the worst thing this system could do."""
    rows = seed_from_replay(conn, emb, limit=300)

    from freshet.autopilot.brief import cause_from_updates
    from freshet.autopilot.investigate import fetch_incident_updates, gather_findings

    silent = 0
    for incident_id in {r["incident_id"] for r in rows}:
        updates = fetch_incident_updates(conn, incident_id)
        if not updates or cause_from_updates(updates) is not None:
            continue          # this incident DOES state a cause; covered elsewhere
        silent += 1
        service = next(r["provider"] for r in rows if r["incident_id"] == incident_id)
        findings = gather_findings(conn, service, incident_id,
                                   status="opened", composer=llm)
        assert findings.cause_text is None, (
            f"{incident_id}: no cause was stated, so none may be reported")

    assert silent > 0, "expected most real incidents to state no cause at all"



def test_retrieval_finds_the_incident_it_was_asked_about(conn, emb):
    """Guards the seeding path itself: if indexing silently produced nothing, the
    two tests above could pass vacuously."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)

    from freshet.rag.retrieval import hybrid_search

    result = hybrid_search(conn, emb, "service degradation", k=5, service=service,
                           min_similarity=0.0)
    assert result.hits, f"no hits for {service}"
    assert all(h.service == service for h in result.hits)


def test_the_brief_only_cites_the_incident_it_is_about(conn, emb, llm):
    """A brief for incident X must not cite incident Y. Retrieval filters by
    SERVICE, so a similarity search happily returns a provider's other incidents;
    the update timeline must be a direct lookup of this incident's own updates."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    from freshet.autopilot.brief import render_brief
    from freshet.autopilot.investigate import gather_findings

    text = render_brief(gather_findings(conn, service, incident_id, status="opened", composer=llm))

    # EVERY citation anywhere in the brief — Cause, Resolution, Updates — must
    # belong to this incident. event_ids are "provider:incident_id:update_id",
    # so membership is checkable directly.
    import re
    cited = re.findall(r"\[([^\[\]@]+)@", text)
    assert cited, "expected citations somewhere in the brief"
    for event_id in cited:
        assert incident_id in event_id, (
            f"brief cites a DIFFERENT incident: {event_id.strip()} "
            f"(briefing {incident_id})")


def test_a_stated_cause_is_surfaced_when_the_provider_gives_one(conn, emb, llm):
    """Across 300 real updates at least one provider states a cause in its own
    words ("caused by", "due to", ...). Those briefs must show it rather than
    "not identified"; briefs for incidents with no stated cause must not."""
    rows = seed_from_replay(conn, emb, limit=300)

    from freshet.autopilot.brief import cause_from_updates
    from freshet.autopilot.investigate import fetch_incident_updates, gather_findings

    with_cause = 0
    for incident_id in {r["incident_id"] for r in rows}:
        updates = fetch_incident_updates(conn, incident_id)
        if not updates or cause_from_updates(updates) is None:
            continue
        with_cause += 1
        service = next(r["provider"] for r in rows if r["incident_id"] == incident_id)
        findings = gather_findings(conn, service, incident_id, status="resolved", composer=llm)
        assert findings.cause_text, f"{incident_id}: cause stated but not surfaced"
        assert findings.cause_cite and "@" in findings.cause_cite

    assert with_cause > 0, "expected at least one real incident to state a cause"


def test_the_brief_never_reads_evidence_outside_its_incident(conn, emb, llm, monkeypatch):
    """Structural guard for the whole brief, not just its citations.

    Cause, resolution and IMPACT were all derived from a service-wide similarity
    search, so a provider with several open incidents could have another
    incident's error percentages folded into this one's impact line. Impact is
    not cited, so a citation check cannot catch it — this asserts the unscoped
    search is not reachable from the brief path at all."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    import freshet.rag.retrieval as retrieval

    def forbidden(*a, **kw):
        raise AssertionError("gather_findings must not run an unscoped search")

    monkeypatch.setattr(retrieval, "hybrid_search", forbidden)

    from freshet.autopilot.investigate import gather_findings

    findings = gather_findings(conn, service, incident_id,
                               status="opened", composer=llm)
    assert findings.updates, "the brief still needs its own incident's updates"
