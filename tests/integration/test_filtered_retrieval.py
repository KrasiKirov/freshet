"""A filtered query browses a window; an unfiltered one must be untouched.

Regression for a real failure: "what incidents happened today?" returned HubSpot
boilerplate from April, because nothing in ranking preferred recent events. The
naive fix — applying the time filter — retrieved the RIGHT incidents and then
abstained anyway, since a browse question scores ~0.55 against any single
incident and the calibrated floor is 0.70.
"""
from datetime import UTC, datetime, timedelta

import pytest

from freshet.api.retrieval import hybrid_search

pytestmark = pytest.mark.integration

QUESTION = "what incidents happened today?"


@pytest.fixture
def seeded(conn, emb):
    """One recent incident and one old one, both indexed with the real embedder."""
    now = datetime.now(UTC)
    rows = [("flt_new", "flt:new:1", "acme", now - timedelta(hours=2),
             "Elevated API error rates in eu-west"),
            ("flt_old", "flt:old:1", "acme", now - timedelta(days=120),
             "We conduct a thorough review after each incident to prevent recurrence")]
    for chunk_id, event_id, service, ts, text in rows:
        [vec] = emb.encode([text])
        conn.execute(
            "INSERT INTO vector_records (chunk_id, event_id, incident_id, service, ts,"
            " indexed_at, source, text, embedding, model)"
            " VALUES (%s,%s,%s,%s,%s,now(),'alert',%s,%s::vector,%s)"
            " ON CONFLICT (chunk_id) DO UPDATE SET ts=EXCLUDED.ts, embedding=EXCLUDED.embedding",
            (chunk_id, event_id, event_id, service, ts, text, str(vec), getattr(emb, "name", None)))
    yield now
    conn.execute("DELETE FROM vector_records WHERE chunk_id LIKE 'flt_%'")


def test_a_time_filter_surfaces_todays_incident_and_answers(conn, emb, seeded):
    since = seeded - timedelta(hours=24)
    r = hybrid_search(conn, emb, QUESTION, k=6, since=since)
    assert not r.abstained, "a filtered browse must not abstain on a non-empty window"
    ids = {h.event_id for h in r.hits}
    assert "flt:new:1" in ids
    assert "flt:old:1" not in ids, "the old boilerplate is outside the window"


def test_the_filtered_path_returns_evidence_the_floor_would_have_vetoed(conn, emb, seeded):
    """The whole point: correct evidence below the semantic floor is now usable."""
    r = hybrid_search(conn, emb, QUESTION, k=6, since=seeded - timedelta(hours=24))
    assert r.hits and not r.abstained
    assert max(h.similarity for h in r.hits) < 0.70, (
        "if this ever exceeds the floor the test no longer proves anything")


def test_an_empty_window_abstains_honestly(conn, emb, seeded):
    r = hybrid_search(conn, emb, QUESTION, k=6, since=seeded + timedelta(days=1))
    assert r.abstained and not r.hits


def test_the_unfiltered_path_still_applies_the_calibrated_floor(conn, emb, seeded):
    """Guards against this becoming a global threshold cut."""
    r = hybrid_search(conn, emb, "zzz unrelated gibberish query", k=6)
    assert r.abstained, "weak unfiltered matches must still abstain"


def test_a_service_filter_gets_the_same_browse_contract(conn, emb, seeded):
    r = hybrid_search(conn, emb, QUESTION, k=6, service="acme")
    assert not r.abstained and r.hits


def test_retrieved_hits_carry_the_incident_title(conn, emb):
    """The API cannot label a citation by name unless retrieval returns one."""
    from freshet.api.retrieval import hybrid_search
    r = hybrid_search(conn, emb, "elevated errors", k=5)
    assert r.hits, "need hits for this to mean anything"
    titled = [h for h in r.hits if h.title]
    assert titled, "no hit carried a title — the column is not reaching RetrievedHit"
    for h in titled:
        assert h.title.strip() == h.title and len(h.title) < 200
