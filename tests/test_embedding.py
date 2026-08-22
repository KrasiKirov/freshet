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
