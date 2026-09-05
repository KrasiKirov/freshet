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

# Per-model abstention floors: cosine distributions differ by model. These are
# now fixed constants — the labeled-fixture calibration tool that derived them
# has been retired, and its evidence retired with it, so the values below can
# no longer be reproduced or re-derived; override via FRESHET_MIN_SIMILARITY.
MIN_SIMILARITY_MINILM = 0.3
MIN_SIMILARITY_BGE = 0.7

# Same floor in mean-centered space (index_stats.py). Fixed for the same
# reason: no calibration tool remains to recompute it against a new corpus
# or model.
MIN_SIMILARITY_BGE_CENTERED = 0.44


class Embedder(Protocol):
    name: str
    min_similarity: float
    min_similarity_centered: float | None

    def encode(self, texts: list[str]) -> list[list[float]]: ...
    def encode_query(self, texts: list[str]) -> list[list[float]]: ...


class StubEmbedder:
    """Deterministic fake embeddings: same text -> same unit vector."""

    name = "stub"

    min_similarity = MIN_SIMILARITY_MINILM
    min_similarity_centered: float | None = None

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


def _cap_torch_threads() -> None:
    """Stop bge taking every core — it shares the machine with the user's work.

    torch is a transitive dependency of sentence_transformers, but the tests stub
    SentenceTransformer and run without either installed, so a hard import here
    turned a CPU nicety into an import error for anyone without the [embed] extra.
    """
    threads = int(os.environ.get("FRESHET_TORCH_THREADS", "2"))
    if threads <= 0:
        return
    try:
        import torch
    except ImportError:
        return
    torch.set_num_threads(threads)


class SentenceTransformerEmbedder:
    """Real local embeddings. Lazy import; first use downloads the model.
    query_instruction (if set) is prepended only to query-side encodes."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
                 query_instruction: str = "",
                 min_similarity: float = MIN_SIMILARITY_MINILM,
                 min_similarity_centered: float | None = None,
                 batch_size: int = 32):
        from sentence_transformers import SentenceTransformer

        _cap_torch_threads()
        self.model = SentenceTransformer(model_name)
        self.name = model_name
        self.query_instruction = query_instruction
        self.min_similarity = min_similarity
        self.min_similarity_centered = min_similarity_centered
        self.batch_size = batch_size

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [
            [float(x) for x in row]
            for row in self.model.encode(texts, normalize_embeddings=True,
                                         batch_size=self.batch_size)
        ]

    def encode_query(self, texts: list[str]) -> list[list[float]]:
        return self.encode(_apply_query_instruction(self.query_instruction, texts))


def make_embedder(kind: str) -> Embedder:
    emb: Embedder
    if kind == "stub":
        emb = StubEmbedder()
    elif kind == "minilm":
        raise ValueError(
            "minilm (384-dim) no longer fits the vector(768) schema — use 'bge' "
            "(or 'stub' for keyless runs)")
    elif kind == "bge":
        emb = SentenceTransformerEmbedder(
            "BAAI/bge-base-en-v1.5",
            query_instruction="Represent this sentence for searching relevant passages:",
            min_similarity=MIN_SIMILARITY_BGE,
            min_similarity_centered=MIN_SIMILARITY_BGE_CENTERED,
        )
    else:
        raise ValueError(f"unknown embedder: {kind!r} (expected 'stub' or 'bge')")
    override = os.environ.get("FRESHET_MIN_SIMILARITY")
    if override:
        emb.min_similarity = float(override)
    override_c = os.environ.get("FRESHET_MIN_SIMILARITY_CENTERED")
    if override_c:
        emb.min_similarity_centered = float(override_c)
    return emb


def vec_literal(v: list[float]) -> str:
    """Format a vector as a pgvector text literal for use with %s::vector.

    9 significant digits is FLT_DECIMAL_DIG: the smallest precision that
    round-trips float32 EXACTLY, which is what these values are on both sides
    of the wire (sentence-transformers emits float32, pgvector stores it).
    7 digits looks sufficient and is not — it perturbs the low bits, verified
    by test_vec_literal_is_lossless_for_float32_and_compact. `str(float)`
    went the other way, emitting up to 17 digits for values that never had
    that much information: 16,285 bytes per 768-dim vector against 10,437,
    or ~194 MB of decimal text across a full re-index, and the query path
    pays it twice (the keyword arm interpolates qvec again).
    """
    return "[" + ",".join(f"{x:.9g}" for x in v) + "]"
