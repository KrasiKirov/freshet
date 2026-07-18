"""Unit tests for the embedding interface: dim, query-instruction prefixing."""
from freshet.pipeline.embedding import (
    EMBEDDING_DIM,
    StubEmbedder,
    _apply_query_instruction,
    make_embedder,
)


def test_embedding_dim_is_768():
    assert EMBEDDING_DIM == 768


def test_stub_encode_query_matches_encode():
    e = StubEmbedder()
    q = ["what caused the outage?"]
    assert e.encode_query(q) == e.encode(q)


def test_apply_query_instruction_prepends_when_set():
    assert _apply_query_instruction("PREFIX:", ["a", "b"]) == ["PREFIX: a", "PREFIX: b"]


def test_apply_query_instruction_passthrough_when_empty():
    assert _apply_query_instruction("", ["a", "b"]) == ["a", "b"]


def test_st_encode_query_applies_instruction(monkeypatch):
    captured = {}

    class _FakeST:
        def __init__(self, name):
            pass

        def encode(self, texts, normalize_embeddings=True):
            captured["texts"] = list(texts)
            return [[0.0] * EMBEDDING_DIM for _ in texts]

    # Mock the module-level import within SentenceTransformerEmbedder.__init__
    import sys
    import types
    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = _FakeST
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    e = make_embedder("bge")
    e.encode_query(["hello"])
    assert captured["texts"] == [
        "Represent this sentence for searching relevant passages: hello"
    ]
