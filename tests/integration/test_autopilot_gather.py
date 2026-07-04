"""Integration: keyless gather_findings recovers the true cause for a seeded
incident (mirrors the completeness eval's service-scoped setup)."""
import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_gather_findings_keyless_recovers_cause(conn, monkeypatch):
    # force the keyless path even if a key is present in the environment
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.pipeline.embedding import make_embedder
    from freshet.autopilot.investigate import gather_findings
    from freshet.generator.generator import build_benchmark
    from freshet.eval.run_eval import index_corpus

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    emb = make_embedder("bge")
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    f = gather_findings(conn, emb, truth.service, "INC-does-not-exist", "open")
    assert f.service is not None
    # cause line should cite the authored cause event id
    assert f.cause_cite is not None and truth.cause_id in f.cause_cite
