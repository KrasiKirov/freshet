"""Optional cross-encoder reranking for the retrieval seam. Keyless (sentence-
transformers), off by default. A cross-encoder scores (query, passage) jointly,
which is more accurate than the bi-encoder similarity used for first-stage recall —
so it reorders the fused candidate pool before the top-k is taken."""

from __future__ import annotations

import os
from typing import Protocol, runtime_checkable

from freshet.api.retrieval import RetrievedHit

DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


@runtime_checkable
class Reranker(Protocol):
    def rerank(self, query: str, hits: list[RetrievedHit]) -> list[RetrievedHit]: ...


class NoopReranker:
    """Identity reranker — the keyless default when reranking is off."""

    def rerank(self, query: str, hits: list[RetrievedHit]) -> list[RetrievedHit]:
        return hits


class CrossEncoderReranker:
    """Lazily loads a sentence-transformers CrossEncoder and reorders by joint score."""

    def __init__(self, model: str | None = None):
        self._model_name = model or os.environ.get("FRESHET_RERANK_MODEL", DEFAULT_MODEL)
        self._model = None

    def _ensure(self):
        if self._model is None:
            from sentence_transformers import CrossEncoder
            self._model = CrossEncoder(self._model_name)
        return self._model

    def rerank(self, query: str, hits: list[RetrievedHit]) -> list[RetrievedHit]:
        if not hits:
            return hits
        model = self._ensure()
        scores = model.predict([(query, h.text) for h in hits])
        return [h for _, h in sorted(zip(scores, hits), key=lambda z: z[0], reverse=True)]


def make_reranker() -> Reranker:
    kind = os.environ.get("FRESHET_RERANK", "").strip()
    if not kind:
        return NoopReranker()
    if kind == "cross-encoder":
        return CrossEncoderReranker()
    raise ValueError(f"unknown FRESHET_RERANK: {kind!r} (expected 'cross-encoder' or unset)")
