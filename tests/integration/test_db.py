from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def test_schema_applied():
    from freshet.common.db import connect

    conn = connect()
    try:
        ext = conn.execute(
            "SELECT count(*) FROM pg_extension WHERE extname = 'vector'"
        ).fetchone()[0]
        assert ext == 1
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'vector_records'"
            ).fetchall()
        }
        assert {
            "chunk_id", "event_id", "incident_id", "service",
            "ts", "indexed_at", "source", "text", "embedding",
        } <= cols
        inc_cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'incidents'"
            ).fetchall()
        }
        assert {
            "incident_id", "title", "opened_at",
            "resolved_at", "resolution_summary",
        } <= inc_cols
        assert "services" not in inc_cols and "event_ids" not in inc_cols

        svc_cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'incident_services'"
            ).fetchall()
        }
        assert {"incident_id", "service"} <= svc_cols

        evt_cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'incident_events'"
            ).fetchall()
        }
        assert {"incident_id", "event_id"} <= evt_cols
    finally:
        conn.close()


# These exercise the namespacing migration on rows the test itself owns.
#
# Fixture-specific service names and a ts four years back, so no recency
# window or service filter in another test can reach them.
_SCHEMA = Path("db/init.sql").read_text()
_VEC = "(SELECT array_fill(0.1::real, ARRAY[768])::vector)"
_TS = "now() - interval '1460 days'"


def _seed_chunk(conn, chunk_id, event_id, incident_id, service):
    conn.execute(
        "INSERT INTO vector_records"
        " (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)"
        f" VALUES (%s, %s, %s, %s, {_TS}, now(), 'alert', 'seeded', {_VEC})",
        (chunk_id, event_id, incident_id, service))


def test_the_migration_namespaces_rows_that_predate_it(conn):
    conn.execute("DELETE FROM vector_records WHERE chunk_id = 'chk_ns_0'")
    conn.execute("DELETE FROM incidents WHERE incident_id IN ('NS1', 'nsfix-a:NS1')")
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at, primary_service)"
        " VALUES ('NS1', 't', now(), 'nsfix-a')")
    _seed_chunk(conn, "chk_ns_0", "nsfix-a:NS1:aaaaaaaaaaaa", "NS1", "nsfix-a")

    conn.execute(_SCHEMA)

    assert conn.execute(
        "SELECT incident_id FROM incidents WHERE incident_id = 'nsfix-a:NS1'"
    ).fetchone() is not None
    assert conn.execute(
        "SELECT incident_id FROM vector_records WHERE chunk_id = 'chk_ns_0'"
    ).fetchone()[0] == "nsfix-a:NS1"


def test_vector_records_are_converted_even_when_incidents_is_already_clean(conn):
    """Regression: the guard named only `incidents`, so a database holding bare-id
    chunks but no bare-id incidents rows skipped the entire migration and kept
    serving un-namespaced chunks. Found on a test database in exactly this state."""
    conn.execute("DELETE FROM vector_records WHERE chunk_id = 'chk_only_0'")
    conn.execute("DELETE FROM incidents WHERE incident_id NOT LIKE '%:%'")
    _seed_chunk(conn, "chk_only_0", "nsfix-b:ONLY1:bbbbbbbbbbbb", "ONLY1", "nsfix-b")

    conn.execute(_SCHEMA)

    assert conn.execute(
        "SELECT incident_id FROM vector_records WHERE chunk_id = 'chk_only_0'"
    ).fetchone()[0] == "nsfix-b:ONLY1"


def test_a_bare_row_converts_even_when_its_namespaced_form_is_already_taken(conn):
    """Regression, found by running the migration against the real database rather
    than trusting a green suite: it died on a duplicate key.

    An eval run had seeded 225 fixture stubs into the working database, already in
    namespaced form, with no provider and no chunks. 26 of them collided with real
    incidents that still had bare ids, so the UPDATE tried to claim an id a stub
    already held. The stub must go first — and it is identified by having no
    evidence, not by the shape of its id."""
    conn.execute("DELETE FROM vector_records WHERE chunk_id = 'chk_taken_0'")
    conn.execute("DELETE FROM incidents WHERE incident_id IN ('TAKEN1', 'nsfix-a:TAKEN1')")
    # The eval stub: already namespaced, no provider, no evidence.
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at, primary_service)"
        " VALUES ('nsfix-a:TAKEN1', 'nsfix-a: resolved', now(), NULL)")
    # The real incident: bare id, a provider, and a chunk to cite.
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at, primary_service)"
        " VALUES ('TAKEN1', 'Elevated error rates', now(), 'nsfix-a')")
    _seed_chunk(conn, "chk_taken_0", "nsfix-a:TAKEN1:aaaaaaaaaaaa", "TAKEN1", "nsfix-a")

    conn.execute(_SCHEMA)

    row = conn.execute(
        "SELECT title FROM incidents WHERE incident_id = 'nsfix-a:TAKEN1'").fetchone()
    assert row is not None and row[0] == "Elevated error rates", \
        "the real incident must win the id; the evidence-free stub is what gets dropped"
    assert conn.execute(
        "SELECT count(*) FROM incidents WHERE incident_id = 'TAKEN1'").fetchone()[0] == 0


def test_two_providers_sharing_a_raw_id_do_not_collide_after_migration(conn):
    """The whole point of the change: Statuspage ids are unique per tenant, so the
    same raw id from two providers must land on two distinct rows."""
    for cid in ("chk_dup_a", "chk_dup_b"):
        conn.execute("DELETE FROM vector_records WHERE chunk_id = %s", (cid,))
    _seed_chunk(conn, "chk_dup_a", "nsfix-a:SHARED:aaaaaaaaaaaa", "SHARED", "nsfix-a")
    _seed_chunk(conn, "chk_dup_b", "nsfix-c:SHARED:bbbbbbbbbbbb", "SHARED", "nsfix-c")

    conn.execute(_SCHEMA)

    ids = {r[0] for r in conn.execute(
        "SELECT incident_id FROM vector_records WHERE chunk_id IN ('chk_dup_a','chk_dup_b')"
    ).fetchall()}
    assert ids == {"nsfix-a:SHARED", "nsfix-c:SHARED"}
