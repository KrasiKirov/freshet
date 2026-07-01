from datetime import datetime, timezone

import pytest

from freshet.api.rerank import NoopReranker, make_reranker
from freshet.api.retrieval import RetrievedHit


def _hit(eid, text, score):
    return RetrievedHit(chunk_id=eid, event_id=eid, service="s",
                        ts=datetime(2026, 6, 6, tzinfo=timezone.utc),
                        indexed_at=datetime(2026, 6, 6, tzinfo=timezone.utc),
                        source="alert", text=text, type="error_spike",
                        similarity=0.5, score=score)


def test_noop_passthrough():
    hits = [_hit("a", "x", 0.1), _hit("b", "y", 0.2)]
    assert NoopReranker().rerank("q", hits) == hits


def test_make_reranker_gating(monkeypatch):
    monkeypatch.delenv("FRESHET_RERANK", raising=False)
    assert isinstance(make_reranker(), NoopReranker)
    monkeypatch.setenv("FRESHET_RERANK", "nonsense")
    with pytest.raises(ValueError):
        make_reranker()
