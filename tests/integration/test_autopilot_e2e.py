"""Integration: an opened incident on the stream yields exactly one cited brief,
keyless. Exercises correlate→lifecycle→claim→gather→render end to end."""
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_open_incident_briefs_once(conn, monkeypatch, capsys):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.pipeline.embedding import make_embedder
    from freshet.pipeline.lifecycle import LifecycleEvent
    from freshet.autopilot.consumer import handle_lifecycle
    from freshet.generator.generator import build_benchmark
    from freshet.eval.run_eval import index_corpus

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    emb = make_embedder("bge")
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    # simulate the incident being opened + present in the incidents table
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, services, opened_at)"
        " VALUES (%s, %s, ARRAY[%s], now())",
        (iid, f"{truth.service}: open", truth.service),
    )
    raw = LifecycleEvent("opened", iid, truth.service, "2026-07-01T00:00:00+00:00").to_json()

    # first handle → briefs
    handle_lifecycle(conn, emb, raw, window_s=0, sleep=lambda s: None)
    out1 = capsys.readouterr().out
    assert "INCIDENT BRIEF" in out1 and truth.cause_id in out1

    # second handle (redelivery) → claim lost → no second brief
    handle_lifecycle(conn, emb, raw, window_s=0, sleep=lambda s: None)
    out2 = capsys.readouterr().out
    assert "already briefed" in out2.lower() and "INCIDENT BRIEF" not in out2
