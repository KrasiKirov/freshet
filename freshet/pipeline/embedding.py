"""Embedding backends behind one tiny interface.

StubEmbedder is deterministic and dependency-free so unit tests and CI never
download model weights. SentenceTransformerEmbedder is the real local default
(no API key). Both produce EMBEDDING_DIM-dimensional vectors — the
vector_records.embedding column is sized to match.
"""

from __future__ import annotations

import hashlib
import math
import random
from typing import Protocol

EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output size


class Embedder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...


class StubEmbedder:
    """Deterministic fake embeddings: same text -> same unit vector."""

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]

    @staticmethod
    def _vec(text: str) -> list[float]:
        seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
        rng = random.Random(seed)
        v = [rng.uniform(-1.0, 1.0) for _ in range(EMBEDDING_DIM)]
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]


class SentenceTransformerEmbedder:
    """Real local embeddings. Lazy import; first use downloads ~90 MB."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [
            [float(x) for x in row]
            for row in self.model.encode(texts, normalize_embeddings=True)
        ]


def make_embedder(kind: str) -> Embedder:
    if kind == "stub":
        return StubEmbedder()
    if kind == "minilm":
        return SentenceTransformerEmbedder()
    raise ValueError(f"unknown embedder: {kind!r} (expected 'stub' or 'minilm')")


def vec_literal(v: list[float]) -> str:
    """Format a vector as a pgvector text literal for use with %s::vector."""
    return "[" + ",".join(str(x) for x in v) + "]"
