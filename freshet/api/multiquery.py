"""LLM multi-query retrieval: paraphrase the question, retrieve for each variant,
RRF-fuse. Key-gated (client injectable for tests). Falls back to single-query when
the LLM returns nothing usable — never raises on empty output."""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from freshet.api.retrieval import HybridResult, hybrid_search, reciprocal_rank_fusion
from freshet.eval import modes

_PARAPHRASE_SYSTEM = (
    "You rewrite an on-call engineer's question into alternative search queries "
    "that mean the same thing but use different words. Reply with ONLY the "
    "rewrites, one per line, no numbering."
)


def _model() -> str:
    return os.environ.get("FRESHET_LLM_MODEL", "claude-sonnet-4-6")


def _client(client=None):
    if client is not None:
        return client
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY not set; multi-query is key-gated")
    import anthropic
    return anthropic.Anthropic()


def paraphrase(question: str, client=None, n: int = 2) -> list[str]:
    cl = _client(client)
    resp = cl.messages.create(
        model=_model(), max_tokens=256, system=_PARAPHRASE_SYSTEM,
        messages=[{"role": "user", "content": f"Question: {question}\nGive {n} rewrites:"}],
    )
    text = next((b.text for b in resp.content if b.type == "text"), "")
    variants = [ln.strip() for ln in text.splitlines() if ln.strip()][:n]
    return [question, *variants]


def multi_query_event_ids(conn, embedder, question: str, k: int, client=None,
                          service: Optional[str] = None,
                          since: Optional[datetime] = None) -> list[str]:
    variants = paraphrase(question, client)
    ranked = [modes.hybrid_event_ids(conn, embedder, v, k, service=service, since=since)
              for v in variants]
    fused = reciprocal_rank_fusion(ranked)
    return [cid for cid, _ in fused][:k]


def multi_query_search(conn, embedder, question: str, k: int, client=None,
                       service: Optional[str] = None,
                       since: Optional[datetime] = None) -> HybridResult:
    variants = paraphrase(question, client)
    per = [hybrid_search(conn, embedder, v, k=k, service=service, since=since)
           for v in variants]
    ranked = [[h.event_id for h in r.hits] for r in per]
    fused = reciprocal_rank_fusion(ranked)
    by_id = {h.event_id: h for r in per for h in r.hits}
    hits = [by_id[cid] for cid, _ in fused if cid in by_id][:k]
    abstained = all(r.abstained for r in per)
    return HybridResult(hits=hits, abstained=abstained)
