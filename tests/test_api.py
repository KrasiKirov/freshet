from datetime import UTC, datetime

from fastapi.testclient import TestClient

from freshet.api.app import app, get_deps
from freshet.api.retrieval import HybridResult, RetrievedHit
from freshet.pipeline.embedding import StubEmbedder


class FakeComposer:
    def compose(self, question, hits):
        return f"answer({len(hits)} hits)"


def _hit(event_id="evt1"):
    now = datetime.now(UTC)
    return RetrievedHit(
        chunk_id=f"chk_{event_id}_0", event_id=event_id, service="scheduler-api",
        ts=now, indexed_at=now, source="alert", text="5xx spike", type="alert_fired",
        similarity=0.8, score=0.9,
    )


def test_query_returns_answer_and_hits(monkeypatch):
    import freshet.api.app as appmod

    monkeypatch.setattr(
        appmod, "hybrid_search",
        lambda *a, **k: HybridResult(hits=[_hit()], abstained=False),
    )
    app.dependency_overrides[get_deps] = lambda: (object(), StubEmbedder(), FakeComposer())
    try:
        resp = TestClient(app).post("/query", json={"question": "what is wrong?", "k": 3})
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    body = resp.json()
    assert body["abstained"] is False
    assert body["answer"] == "answer(1 hits)"
    assert body["hits"][0]["event_id"] == "evt1"
    assert "score" in body["hits"][0]


def test_query_abstains_without_calling_composer(monkeypatch):
    import freshet.api.app as appmod

    monkeypatch.setattr(
        appmod, "hybrid_search",
        lambda *a, **k: HybridResult(hits=[], abstained=True),
    )

    class BoomComposer:
        def compose(self, q, h):
            raise AssertionError("composer must not be called when abstaining")

    app.dependency_overrides[get_deps] = lambda: (object(), StubEmbedder(), BoomComposer())
    try:
        resp = TestClient(app).post("/query", json={"question": "unrelated", "k": 3})
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    body = resp.json()
    assert body["abstained"] is True
    assert "don't have enough" in body["answer"].lower()
    assert body["hits"] == []


def test_stats_endpoint_reads_prometheus(monkeypatch):
    import freshet.api.app as appmod

    def fake_instant(query):
        if "0.50" in query:
            return 3.5
        if "0.95" in query:
            return 6.0
        if "max_offset" in query:
            return 12.0
        return None

    monkeypatch.setattr(appmod, "_prom_instant", fake_instant)
    resp = TestClient(app).get("/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["latency_p50_s"] == 3.5
    assert body["latency_p95_s"] == 6.0
    assert body["consumer_lag"] == 12.0


def test_stats_degrades_when_prometheus_down(monkeypatch):
    import freshet.api.app as appmod

    monkeypatch.setattr(appmod, "_prom_instant", lambda q: None)
    body = TestClient(app).get("/stats").json()
    assert body["latency_p50_s"] is None
    assert body["consumer_lag"] is None
