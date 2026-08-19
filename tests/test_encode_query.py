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


def test_thread_capping_survives_a_missing_torch(monkeypatch):
    """CI installs neither torch nor sentence_transformers. A hard `import torch`
    turned a CPU nicety into an ImportError for anyone without the [embed] extra."""
    import builtins

    from freshet.pipeline.embedding import _cap_torch_threads

    real_import = builtins.__import__

    def _no_torch(name, *a, **k):
        if name == "torch":
            raise ImportError("No module named 'torch'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _no_torch)
    _cap_torch_threads()          # must not raise


def test_capping_is_skipped_when_disabled(monkeypatch):
    monkeypatch.setenv("FRESHET_TORCH_THREADS", "0")
    import builtins

    from freshet.pipeline.embedding import _cap_torch_threads
    real_import = builtins.__import__

    def _boom(name, *a, **k):
        if name == "torch":
            raise AssertionError("torch must not be imported when capping is off")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _boom)
    _cap_torch_threads()
