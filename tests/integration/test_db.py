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
    finally:
        conn.close()
