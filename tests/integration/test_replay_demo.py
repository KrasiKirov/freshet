import psycopg
import pytest

from freshet.autopilot.replay_demo import TABLES, isolate, replay
from tests.integration.conftest import _ensure_test_db

pytestmark = pytest.mark.integration


def test_replay_is_repeatable_and_does_not_touch_public_tables(capsys):
    dsn = _ensure_test_db()
    outputs = []
    for _ in range(2):
        with psycopg.connect(dsn, autocommit=True) as conn:
            before = [conn.execute(f"SELECT count(*) FROM public.{t}").fetchone() for t in TABLES]
            isolate(conn)
            assert replay(conn) == 3
            outputs.append(capsys.readouterr().out)
            after = [conn.execute(f"SELECT count(*) FROM public.{t}").fetchone() for t in TABLES]
            assert after == before
            assert conn.execute("SELECT count(*) FROM incidents").fetchone()[0] == 1
    assert outputs[0] == outputs[1]
    assert "INCIDENT BRIEF" in outputs[0]
    assert "INCIDENT UPDATE" in outputs[0]
    assert "POSTMORTEM" in outputs[0]
    assert "fully recovered" in outputs[0]


def test_replay_refuses_a_connection_to_public_tables():
    with (psycopg.connect(_ensure_test_db(), autocommit=True) as conn,
          pytest.raises(RuntimeError, match="temporary tables")):
        replay(conn)
