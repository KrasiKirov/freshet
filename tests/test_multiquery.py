"""Unit tests for multi-query retrieval (scripted FakeClient, mocked retrieval)."""
from types import SimpleNamespace


class _Resp:
    def __init__(self, text):
        self.content = [SimpleNamespace(type="text", text=text)]


class FakeClient:
    def __init__(self, text):
        self._text = text

    @property
    def messages(self):
        text = self._text

        class _M:
            def create(self, **kw):
                return _Resp(text)

        return _M()


def test_paraphrase_returns_original_plus_variants():
    from freshet.api.multiquery import paraphrase
    out = paraphrase("what caused the incident?",
                     client=FakeClient("what broke the service?\nwhy did it fail?"), n=2)
    assert out == ["what caused the incident?", "what broke the service?", "why did it fail?"]


def test_paraphrase_empty_falls_back_to_original():
    from freshet.api.multiquery import paraphrase
    assert paraphrase("q?", client=FakeClient(""), n=2) == ["q?"]


def test_multi_query_event_ids_fuses(monkeypatch):
    from freshet.api import multiquery
    calls = []

    def fake_ids(conn, embedder, q, k, service=None, since=None):
        calls.append(q)
        return {"q?": ["a", "b"], "v1": ["b", "c"], "v2": ["c", "d"]}[q]

    monkeypatch.setattr(multiquery.modes, "hybrid_event_ids", fake_ids)
    out = multiquery.multi_query_event_ids(None, None, "q?", k=3,
                                           client=FakeClient("v1\nv2"))
    assert len(calls) == 3            # original + 2 variants
    assert "c" in out                 # appears in two lists -> ranks high
    assert len(out) <= 3


def test_multi_query_search_returns_hits(monkeypatch):
    from freshet.api import multiquery
    from freshet.api.retrieval import HybridResult

    def fake_hs(conn, embedder, q, k, service=None, since=None, **kw):
        return HybridResult(hits=[SimpleNamespace(event_id="a"),
                                  SimpleNamespace(event_id="b")], abstained=False)

    monkeypatch.setattr(multiquery, "hybrid_search", fake_hs)
    res = multiquery.multi_query_search(None, None, "q?", k=2,
                                        client=FakeClient("v1\nv2"))
    assert [h.event_id for h in res.hits]
    assert res.abstained is False
