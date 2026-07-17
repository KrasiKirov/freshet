"""Embedding backends behind one tiny interface.

StubEmbedder is deterministic and dependency-free so unit tests and CI never
download model weights. SentenceTransformerEmbedder is the real local default
(no API key). Both produce EMBEDDING_DIM-dimensional vectors — the
vector_records.embedding column is sized to match.
"""

from __future__ import annotations

import hashlib
import math
import os
import random
from typing import Protocol

EMBEDDING_DIM = 768  # BAAI/bge-base-en-v1.5 output size

# Per-model abstention floors. Cosine-similarity distributions differ by model:
# MiniLM spreads roughly 0..1, while bge compresses similarities upward (~0.5+
# even for unrelated pairs), so a shared floor cannot work. Both values are
# calibrated with scripts/calibrate_abstention.py on the benchmark corpus: bge
# separates cleanly (on-corpus ≥ 0.735 vs hardest off-corpus negative 0.662;
# 0.7 is the gap midpoint). Override with FRESHET_MIN_SIMILARITY; recalibrate
# when the corpus or model changes.
MIN_SIMILARITY_MINILM = 0.3
MIN_SIMILARITY_BGE = 0.7


class Embedder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...
    def encode_query(self, texts: list[str]) -> list[list[float]]: ...


class StubEmbedder:
    """Deterministic fake embeddings: same text -> same unit vector."""

    # random unit vectors follow no model distribution; keep the MiniLM floor so
    # existing tests and keyless demos behave unchanged
    min_similarity = MIN_SIMILARITY_MINILM

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]

    def encode_query(self, texts: list[str]) -> list[list[float]]:
        return self.encode(texts)

    @staticmethod
    def _vec(text: str) -> list[float]:
        seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
        rng = random.Random(seed)
        v = [rng.uniform(-1.0, 1.0) for _ in range(EMBEDDING_DIM)]
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]


def _apply_query_instruction(instruction: str, texts: list[str]) -> list[str]:
    if not instruction:
        return list(texts)
    return [f"{instruction} {t}" for t in texts]


class SentenceTransformerEmbedder:
    """Real local embeddings. Lazy import; first use downloads the model.
    query_instruction (if set) is prepended only to query-side encodes."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
                 query_instruction: str = "",
                 min_similarity: float = MIN_SIMILARITY_MINILM):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name)
        self.query_instruction = query_instruction
        self.min_similarity = min_similarity

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [
            [float(x) for x in row]
            for row in self.model.encode(texts, normalize_embeddings=True)
        ]

    def encode_query(self, texts: list[str]) -> list[list[float]]:
        return self.encode(_apply_query_instruction(self.query_instruction, texts))


def make_embedder(kind: str) -> Embedder:
    emb: Embedder
    if kind == "stub":
        emb = StubEmbedder()
    elif kind == "minilm":
        # Retired from live use: MiniLM's 384-dim vectors cannot index into the
        # vector(768) schema, so every DB path would fail deep in psycopg. Its
        # benchmark numbers survive as the frozen baseline
        # (results/retrieval_metrics_minilm.json); for off-DB experiments,
        # construct SentenceTransformerEmbedder() directly.
        raise ValueError(
            "minilm (384-dim) no longer fits the vector(768) schema — use 'bge' "
            "(or 'stub' for keyless runs)")
    elif kind == "bge":
        emb = SentenceTransformerEmbedder(
            "BAAI/bge-base-en-v1.5",
            query_instruction="Represent this sentence for searching relevant passages:",
            min_similarity=MIN_SIMILARITY_BGE,
        )
    else:
        raise ValueError(f"unknown embedder: {kind!r} (expected 'stub' or 'bge')")
    override = os.environ.get("FRESHET_MIN_SIMILARITY")
    if override:
        emb.min_similarity = float(override)  # type: ignore[misc]
    return emb


def vec_literal(v: list[float]) -> str:
    """Format a vector as a pgvector text literal for use with %s::vector."""
    return "[" + ",".join(str(x) for x in v) + "]"
