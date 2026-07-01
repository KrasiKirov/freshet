"""The /query opt-in multi_query flag: 400 without a key; uses multi_query_search
when enabled."""
from types import SimpleNamespace

from fastapi.testclient import TestClient

from freshet.api.app import app, get_deps
from freshet.pipeline.embedding import StubEmbedder


class FakeComposer:
    def compose(self, question, hits):
        return f"answer({len(hits)} hits)"


def _client():
    from freshet.api import app as appmod
    return TestClient(appmod.app), appmod


def test_multi_query_requires_key(monkeypatch):
    import freshet.api.app as appmod
    client, _ = _client()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(
        appmod, "hybrid_search",
        lambda *a, **k: SimpleNamespace(hits=[], abstained=True),
    )
    app.dependency_overrides[get_deps] = lambda: (object(), StubEmbedder(), FakeComposer())
    try:
        r = client.post("/query", json={"question": "what broke?", "multi_query": True})
        assert r.status_code == 400
    finally:
        app.dependency_overrides.clear()


def test_multi_query_uses_multi_query_search(monkeypatch):
    import freshet.api.app as appmod
    client, _ = _client()
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    called = {}

    def fake_mqs(conn, embedder, question, k, client=None, service=None, since=None):
        called["yes"] = True
        return SimpleNamespace(hits=[], abstained=True)

    monkeypatch.setattr("freshet.api.multiquery.multi_query_search", fake_mqs)
    app.dependency_overrides[get_deps] = lambda: (object(), StubEmbedder(), FakeComposer())
    try:
        r = client.post("/query", json={"question": "what broke?", "multi_query": True})
        assert r.status_code == 200
        assert called.get("yes") is True
    finally:
        app.dependency_overrides.clear()
