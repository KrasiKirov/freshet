"""update_id changed meaning, so in-window updates re-appear once under a new id.

The cleanup keys on the update's TEXT, which is exactly right here: the ids differ
by construction, and the text is what a reader would call the same update.
"""
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

MIGRATION = Path("db/migrations/2026-08-22-dedupe-reidentified-updates.sql")
_VEC = "(SELECT array_fill(0.1::real, ARRAY[768])::vector)"


def _insert(conn, chunk_id, event_id, incident_id, text, age_hours):
    conn.execute(
        "INSERT INTO vector_records"
        " (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)"
        f" VALUES (%s, %s, %s, 'github', now(), now() - (%s || ' hours')::interval,"
        f"         'alert', %s, {_VEC})",
        (chunk_id, event_id, incident_id, str(age_hours), text))


def test_the_older_copy_of_a_reidentified_update_is_removed(conn):
    conn.execute("DELETE FROM vector_records WHERE incident_id = 'github:REID1'")
    _insert(conn, "chk_old_0", "github:REID1:aaaaaaaaaaaa", "github:REID1",
            "We are investigating elevated errors.", 5)
    _insert(conn, "chk_new_0", "github:REID1:bbbbbbbbbbbb", "github:REID1",
            "We are investigating elevated errors.", 1)
    conn.execute(MIGRATION.read_text())
    rows = conn.execute(
        "SELECT chunk_id FROM vector_records WHERE incident_id = 'github:REID1'"
    ).fetchall()
    assert [r[0] for r in rows] == ["chk_new_0"], "keep the newest-indexed copy"


def test_two_genuinely_different_updates_are_both_kept(conn):
    """The key is (incident, text). Two updates of one incident that say different
    things are not duplicates, however close together they were indexed."""
    conn.execute("DELETE FROM vector_records WHERE incident_id = 'github:REID2'")
    _insert(conn, "chk_a_0", "github:REID2:aaaaaaaaaaaa", "github:REID2",
            "We are investigating.", 5)
    _insert(conn, "chk_b_0", "github:REID2:bbbbbbbbbbbb", "github:REID2",
            "This incident has been resolved.", 5)
    conn.execute(MIGRATION.read_text())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE incident_id = 'github:REID2'"
    ).fetchone()[0] == 2


def test_the_same_text_under_two_different_incidents_is_not_collapsed(conn):
    """Providers repeat boilerplate across incidents ('We are continuing to
    monitor'). Keying on text alone would delete real evidence."""
    for inc in ("github:REID3", "github:REID4"):
        conn.execute("DELETE FROM vector_records WHERE incident_id = %s", (inc,))
    _insert(conn, "chk_c_0", "github:REID3:aaaaaaaaaaaa", "github:REID3",
            "We are continuing to monitor.", 5)
    _insert(conn, "chk_d_0", "github:REID4:bbbbbbbbbbbb", "github:REID4",
            "We are continuing to monitor.", 5)
    conn.execute(MIGRATION.read_text())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE incident_id IN"
        " ('github:REID3','github:REID4')").fetchone()[0] == 2


def test_running_it_twice_changes_nothing(conn):
    conn.execute("DELETE FROM vector_records WHERE incident_id = 'github:REID5'")
    _insert(conn, "chk_e_0", "github:REID5:aaaaaaaaaaaa", "github:REID5", "Same text.", 5)
    _insert(conn, "chk_f_0", "github:REID5:bbbbbbbbbbbb", "github:REID5", "Same text.", 1)
    conn.execute(MIGRATION.read_text())
    after_first = conn.execute(
        "SELECT count(*) FROM vector_records WHERE incident_id = 'github:REID5'"
    ).fetchone()[0]
    conn.execute(MIGRATION.read_text())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE incident_id = 'github:REID5'"
    ).fetchone()[0] == after_first == 1
