"""The endpoint infers a window only to fill a gap, and always reports it."""
from datetime import UTC, datetime

import pytest

from freshet.api import app as app_mod
from freshet.api.app import QueryRequest, query
from freshet.api.retrieval import HybridResult, RetrievedHit


@pytest.fixture
def captured(monkeypatch):
    """Capture the `since` hybrid_search actually received."""
    seen = {}

    def fake_search(conn, embedder, question, **kw):
        seen.update(kw)
        now = datetime.now(UTC)
        hit = RetrievedHit(chunk_id="c", event_id="s:1:a", service="s", ts=now,
                           indexed_at=now, source="alert", text="Elevated errors",
                           type="status_update", similarity=0.9, score=0.5)
        return HybridResult(hits=[hit], abstained=False)

    monkeypatch.setattr(app_mod, "hybrid_search", fake_search)
    return seen


class _Composer:
    def compose(self, question, hits):
        return "answer"


def _deps():
    return (object(), object(), _Composer())


def test_a_temporal_question_infers_and_reports_the_window(captured):
    resp = query(QueryRequest(question="what incidents happened today?"), deps=_deps())
    assert resp.window == "today"
    assert captured["since"] is not None, "the filter must reach retrieval"


def test_a_plain_question_infers_nothing(captured):
    resp = query(QueryRequest(question="Cloudflare errors"), deps=_deps())
    assert resp.window is None
    assert captured["since"] is None


def test_an_explicit_since_wins_over_inference(captured):
    explicit = datetime(2026, 1, 1, tzinfo=UTC)
    resp = query(QueryRequest(question="what happened today?", since=explicit), deps=_deps())
    assert captured["since"] == explicit, "caller's filter must not be overridden"
    assert resp.window is None, "no inferred window to report when the caller set one"


def test_an_abstained_filtered_query_still_reports_its_window(monkeypatch):
    monkeypatch.setattr(app_mod, "hybrid_search",
                        lambda *a, **k: HybridResult(hits=[], abstained=True))
    resp = query(QueryRequest(question="what broke today?"), deps=_deps())
    assert resp.abstained and resp.window == "today", (
        "the user must see WHY nothing came back")
