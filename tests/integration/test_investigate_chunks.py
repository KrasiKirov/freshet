"""A brief must see the WHOLE update, not one chunk of it.

Long updates are chunked before indexing. The query used DISTINCT ON (event_id),
so it returned a single arbitrary chunk — a cause stated anywhere but that chunk
was invisible to the brief, and the incident reported "no cause" while the
provider had named one.
"""
from datetime import UTC, datetime

import pytest

from freshet.autopilot.brief import cause_from_updates
from freshet.autopilot.investigate import fetch_incident_updates

pytestmark = pytest.mark.integration

INCIDENT = "INC-CHUNKED"
CAUSE = "The outage was caused by a failed database migration."


@pytest.fixture
def chunked(conn, emb):
    """One update stored as three chunks; the cause sits in the middle one."""
    ts = datetime.now(UTC)
    chunks = ["Elevated errors: we are investigating reports of failures.",
              CAUSE,
              "A fix has been deployed and we are monitoring recovery."]
    conn.execute("DELETE FROM vector_records WHERE incident_id = %s", (INCIDENT,))
    for i, text in enumerate(chunks):
        [vec] = emb.encode([text])
        conn.execute(
            "INSERT INTO vector_records (chunk_id, event_id, incident_id, service, ts,"
            " indexed_at, source, text, embedding, model)"
            " VALUES (%s,%s,%s,'acme',%s,now(),'alert',%s,%s::vector,%s)",
            (f"chk_acme:{INCIDENT}:u1_{i}", f"acme:{INCIDENT}:u1", INCIDENT, ts, text,
             str(vec), getattr(emb, "name", None)))
    yield ts
    conn.execute("DELETE FROM vector_records WHERE incident_id = %s", (INCIDENT,))


def test_the_update_is_reassembled_from_all_its_chunks(conn, chunked):
    updates = fetch_incident_updates(conn, INCIDENT)
    assert len(updates) == 1, "three chunks are ONE update, not three"
    assert CAUSE in updates[0].text
    assert updates[0].text.index("investigating") < updates[0].text.index("caused by"), \
        "chunks must be concatenated in order"


def test_a_cause_in_a_middle_chunk_is_found(conn, chunked):
    """The behaviour that was broken: the cause was not in the returned chunk."""
    found = cause_from_updates(fetch_incident_updates(conn, INCIDENT))
    assert found is not None, "the provider stated a cause and the brief must quote it"
    sentence, _cite = found
    assert "failed database migration" in sentence


def test_chunk_ten_does_not_sort_before_chunk_two(conn, emb):
    """chunk_id is text: '_10' sorts before '_2' lexically, so the index is cast."""
    ts = datetime.now(UTC)
    conn.execute("DELETE FROM vector_records WHERE incident_id = %s", ("INC-ORDER",))
    try:
        for i in (2, 10):
            [vec] = emb.encode([f"part{i}"])
            conn.execute(
                "INSERT INTO vector_records (chunk_id, event_id, incident_id, service,"
                " ts, indexed_at, source, text, embedding, model)"
                " VALUES (%s,%s,%s,'acme',%s,now(),'alert',%s,%s::vector,%s)",
                (f"chk_acme:INC-ORDER:u1_{i}", "acme:INC-ORDER:u1", "INC-ORDER", ts,
                 f"part{i}", str(vec), getattr(emb, "name", None)))
        text = fetch_incident_updates(conn, "INC-ORDER")[0].text
        assert text.index("part2") < text.index("part10")
    finally:
        conn.execute("DELETE FROM vector_records WHERE incident_id = %s", ("INC-ORDER",))
