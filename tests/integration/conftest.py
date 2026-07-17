"""Shared fixtures for integration tests.

`emb` is the embedder for flow-level tests (autopilot, commit signal): they
exercise pipeline logic, not embedding semantics, so any schema-compatible
embedder works. FRESHET_TEST_EMBEDDER selects it — default bge (the real
retriever); CI sets `stub` to skip the model download. Tests that DO depend on
real embedding semantics (abstention, retrieval quality) construct bge
explicitly and guard with importorskip("sentence_transformers") instead.
"""
import os

import pytest


@pytest.fixture
def emb():
    from freshet.pipeline.embedding import make_embedder
    return make_embedder(os.environ.get("FRESHET_TEST_EMBEDDER", "bge"))
