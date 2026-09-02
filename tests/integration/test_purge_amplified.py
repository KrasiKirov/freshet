"""The purge must delete only rows the new parser can no longer produce, and must
leave every other provider untouched."""
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

MIGRATION = Path("db/migrations/2026-08-22-purge-amplified-updates.sql")
_VEC = "(SELECT array_fill(0.1::real, ARRAY[768])::vector)"
# A fixture in a shared database must be invisible to every test but its own.
# `service` has to stay a real provider name here, because the migration is scoped
# by service — so `ts` carries the isolation instead: four years back puts these
# rows outside any recency window a retrieval test can ask for. Seeding at now()
# once cost another session a debugging session, its rows having taken every slot
# in a filtered top-k. `indexed_at` still varies, because that is what this
# migration keys on and therefore what these tests actually exercise.
_TS = "now() - interval '1460 days'"


def test_purge_removes_stale_rows_and_spares_everyone_else(conn):
    conn.execute("DELETE FROM vector_records WHERE chunk_id IN ('chk_stale_0','chk_keep_0')")
    conn.execute(
        "INSERT INTO vector_records"
        " (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)"
        " VALUES ('chk_stale_0', 'openai:INC1:deadbeefdead', 'INC1', 'openai',"
        f"         {_TS}, now() - interval '1 day', 'alert',"
        f"         'Affected components Login (Operational)', {_VEC}),"
        "        ('chk_keep_0', 'github:INC2:cafebabecafe', 'INC2', 'github',"
        f"         {_TS}, now() - interval '1 day', 'alert',"
        f"         'Elevated error rates', {_VEC})")
    conn.execute(MIGRATION.read_text())
    rows = conn.execute(
        "SELECT chunk_id FROM vector_records WHERE chunk_id IN ('chk_stale_0','chk_keep_0')"
    ).fetchall()
    assert [r[0] for r in rows] == ["chk_keep_0"], "only the amplified provider's rows go"


def test_rows_indexed_by_the_new_parser_survive(conn):
    """The migration runs AFTER the fixed poller has already indexed fresh rows."""
    conn.execute("DELETE FROM vector_records WHERE chunk_id = 'chk_fresh_0'")
    conn.execute(
        "INSERT INTO vector_records"
        " (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)"
        " VALUES ('chk_fresh_0', 'openai:INC3:0011223344ff', 'INC3', 'openai',"
        f"         {_TS}, now(), 'alert', 'The issue has been resolved.', {_VEC})")
    conn.execute(MIGRATION.read_text())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE chunk_id = 'chk_fresh_0'"
    ).fetchone()[0] == 1
