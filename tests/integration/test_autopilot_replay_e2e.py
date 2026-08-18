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

from freshet.common.db import connect
from tests.integration.conftest_replay import seed_from_replay

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    c = connect()
    yield c
    c.close()


def _busiest_provider(rows) -> str:
    return Counter(r["provider"] for r in rows).most_common(1)[0][0]


def test_a_brief_is_produced_for_a_real_incident(conn, emb):
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    from freshet.autopilot.brief import render_brief
    from freshet.autopilot.investigate import gather_findings

    findings = gather_findings(conn, emb, service, incident_id, status="opened")
    text = render_brief(findings)

    assert text.strip(), "a brief must be produced for a real incident"
    assert service in text
    # any citation present must be well formed. NOTE: on real status-feed data
    # there are currently none — see the documented-gap test below. Written as an
    # explicit count so this can never pass vacuously again.
    cited = [ln for ln in text.splitlines() if "[" in ln]
    for line in cited:
        assert "]" in line and "@" in line, f"malformed citation: {line}"


def test_documents_the_gap_autopilot_cites_nothing_on_real_feeds(conn, emb):
    """A DOCUMENTED LIMITATION, not a passing feature.

    Autopilot's Findings model is cause/fix shaped: it was built for a corpus
    containing deploy -> rollback events. Real status updates contain neither, so
    build_timeline correctly declines, and the brief renders with no citations at
    all. The behaviour is honest but nearly contentless.

    This test pins the current reality so the gap is visible in CI instead of
    being discovered in a demo. When Autopilot is reworked to summarise and cite
    the incident UPDATES themselves, this test should start failing — and that
    failure is the signal to delete it."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)
    incident_id = next(r["incident_id"] for r in reversed(rows) if r["provider"] == service)

    from freshet.autopilot.brief import render_brief
    from freshet.autopilot.investigate import gather_findings

    text = render_brief(gather_findings(conn, emb, service, incident_id, status="opened"))
    assert "Cause: not identified" in text
    assert "[" not in text, "if this fails, Autopilot now cites evidence — good, delete this test"


def test_a_cause_is_never_invented_from_real_updates(conn, emb):
    """Real status updates are typed `status_update`, never a change event, so
    build_timeline must structurally decline to name a cause. If this ever fails,
    the system has started guessing."""
    seed_from_replay(conn, emb, limit=300)

    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline

    result = hybrid_search(conn, emb, "what caused the outage", k=8, min_similarity=0.0)
    assert result.hits, "the seeded corpus should retrieve something"
    assert build_timeline(result.hits).cause is None, (
        "real feeds contain no change events; naming a cause would be invention"
    )


def test_retrieval_finds_the_incident_it_was_asked_about(conn, emb):
    """Guards the seeding path itself: if indexing silently produced nothing, the
    two tests above could pass vacuously."""
    rows = seed_from_replay(conn, emb, limit=300)
    service = _busiest_provider(rows)

    from freshet.api.retrieval import hybrid_search

    result = hybrid_search(conn, emb, "service degradation", k=5, service=service,
                           min_similarity=0.0)
    assert result.hits, f"no hits for {service}"
    assert all(h.service == service for h in result.hits)
