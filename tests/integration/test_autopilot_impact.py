"""Integration: gather_findings/gather_postmortem set a non-empty Findings.impact,
rendered in place of the old stub. Keyless."""
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_gather_postmortem_sets_impact(conn, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.pipeline.embedding import make_embedder
    from freshet.autopilot.investigate import gather_postmortem
    from freshet.autopilot.brief import render_brief
    from freshet.generator.generator import build_benchmark
    from freshet.eval.run_eval import index_corpus

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    emb = make_embedder("bge")
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, services, opened_at, resolved_at)"
        " VALUES (%s, %s, ARRAY[%s], now() - interval '20 minutes', now())",
        (iid, f"{truth.service}: resolved", truth.service),
    )
    pm = gather_postmortem(conn, emb, truth.service, iid)
    assert pm.impact and pm.impact.startswith("Impact:")
    out = render_brief(pm)
    assert "Impact:" in out and "estimation pending" not in out


def test_gather_findings_sets_impact_ongoing(conn, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.pipeline.embedding import make_embedder
    from freshet.autopilot.investigate import gather_findings
    from freshet.generator.generator import build_benchmark
    from freshet.eval.run_eval import index_corpus

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    emb = make_embedder("bge")
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, services, opened_at)"
        " VALUES (%s, %s, ARRAY[%s], now())",
        (iid, f"{truth.service}: open", truth.service),
    )
    f = gather_findings(conn, emb, truth.service, iid, "open")
    assert f.impact and "ongoing" in f.impact
