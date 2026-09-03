import pytest

from freshet.pipeline.embedding import (
    EMBEDDING_DIM,
    StubEmbedder,
    make_embedder,
    vec_literal,
)


def test_stub_is_deterministic_and_distinct():
    e = StubEmbedder()
    [a1] = e.encode(["error spike on scheduler-api"])
    [a2] = e.encode(["error spike on scheduler-api"])
    [b] = e.encode(["routine deploy finished"])
    assert a1 == a2
    assert a1 != b
    assert len(a1) == EMBEDDING_DIM


def test_stub_vectors_are_unit_norm():
    [v] = StubEmbedder().encode(["x"])
    assert abs(sum(x * x for x in v) - 1.0) < 1e-6


def test_make_embedder():
    assert isinstance(make_embedder("stub"), StubEmbedder)
    with pytest.raises(ValueError):
        make_embedder("nope")


def test_minilm_is_retired():
    # 384-dim MiniLM cannot index into the vector(768) schema; fail fast with
    # a clear message instead of deep in psycopg.
    with pytest.raises(ValueError, match="vector\\(768\\)"):
        make_embedder("minilm")


def test_vec_literal_format():
    assert vec_literal([1.0, -0.5]) == "[1,-0.5]"


def test_protocol_declares_the_abstention_floors():
    """min_similarity is load-bearing on the query path. An embedder that omits
    it silently inherits the MiniLM floor of 0.3 — and since unrelated live
    chunk pairs average 0.594 cosine, that floor means 'never abstain'. Putting
    it on the Protocol turns a silent safety regression into a type error."""
    from freshet.pipeline.embedding import Embedder

    annotations = Embedder.__annotations__
    assert "min_similarity" in annotations
    assert "min_similarity_centered" in annotations


def test_every_shipped_embedder_carries_both_floors():
    e = StubEmbedder()
    assert isinstance(e.min_similarity, float)
    # the stub's random unit vectors follow no model distribution, so there is
    # no centered calibration for it — None, not a made-up number
    assert e.min_similarity_centered is None


def test_vec_literal_is_lossless_for_float32_and_compact():
    """`str(float)` emitted 17 significant digits for values that are float32
    to begin with — 16,285 bytes per 768-dim vector, ~194 MB for a full
    re-index.

    9 digits (FLT_DECIMAL_DIG) is the smallest precision that round-trips
    float32 exactly. 7 is NOT enough — it silently perturbs the low bits,
    which is what this test exists to catch."""
    import struct

    def f32(x):
        return struct.unpack("f", struct.pack("f", x))[0]

    values = [f32(v) for v in (0.010470855, -0.011114796, 1e-8, -0.9999999, 0.0)]
    parsed = [float(s) for s in vec_literal(values)[1:-1].split(",")]
    assert [f32(p) for p in parsed] == values

    big = vec_literal([f32(0.010470855049788952)] * EMBEDDING_DIM)
    assert len(big) < 11000       # was 16,285 with str(float)


def test_sentence_transformer_encode_batches():
    """encode() is called once per Kafka message (1-3 chunks). Passing an
    explicit batch_size is what lets a replay burst run at the model's real
    throughput: 107 chunks/s at batch=1 vs 507 at batch=32, on 2 threads."""
    import sys
    import types

    calls = {}

    class _FakeST:
        def __init__(self, name):
            self.name = name

        def encode(self, texts, normalize_embeddings=False, batch_size=None, **kw):
            calls["batch_size"] = batch_size
            return [[0.0, 1.0] for _ in texts]

    fake = types.ModuleType("sentence_transformers")
    fake.SentenceTransformer = _FakeST
    saved = sys.modules.get("sentence_transformers")
    sys.modules["sentence_transformers"] = fake
    try:
        from freshet.pipeline.embedding import SentenceTransformerEmbedder

        e = SentenceTransformerEmbedder("fake-model", batch_size=32)
        e.encode(["a", "b"])
        assert calls["batch_size"] == 32
    finally:
        if saved is None:
            del sys.modules["sentence_transformers"]
        else:
            sys.modules["sentence_transformers"] = saved
