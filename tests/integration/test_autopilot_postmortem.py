"""Integration: keyless gather_postmortem returns a resolved Findings with a
non-empty narrative + duration/resolution meta for a seeded, resolved incident."""
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_gather_postmortem_keyless(conn, emb, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.autopilot.investigate import gather_postmortem
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    # a resolved incident row for this service (opened 42 min before it resolved)
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, services, opened_at, resolved_at,"
        " resolution_summary) VALUES (%s, %s, ARRAY[%s], now() - interval '42 minutes',"
        " now(), %s)",
        (iid, f"{truth.service}: resolved", truth.service, "rolled back the deploy"),
    )

    pm = gather_postmortem(conn, emb, truth.service, iid)
    assert pm.status == "resolved"
    assert pm.narrative and pm.narrative.strip()
    assert pm.meta and "rolled back the deploy" in pm.meta and "42m" in pm.meta
