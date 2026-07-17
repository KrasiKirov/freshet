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
    assert vec_literal([1.0, -0.5]) == "[1.0,-0.5]"
