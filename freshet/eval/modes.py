"""Per-mode retrieval for evaluation: vector-only, keyword-only, and hybrid,
each returning ranked unique event ids. Reuses the production retrieval SQL and
fusion so the eval scores the real system, not a reimplementation."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from freshet.api.retrieval import hybrid_search, keyword_sql, vector_sql
from freshet.pipeline.embedding import Embedder, vec_literal

_CAND = 30  # candidate chunk depth before de-duplicating to events


def _dedupe(event_ids: list[str], k: int) -> list[str]:
    seen: list[str] = []
    for eid in event_ids:
        if eid not in seen:
            seen.append(eid)
        if len(seen) >= k:
            break
    return seen


def vector_only_event_ids(
    conn, embedder: Embedder, question: str, k: int,
    service: Optional[str] = None, since: Optional[datetime] = None,
) -> list[str]:
    [qvec] = embedder.encode_query([question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "k": _CAND}
    if service is not None:
        params["service"] = service
    if since is not None:
        params["since"] = since
    rows = conn.execute(vector_sql(service, since), params).fetchall()
    return _dedupe([r[1] for r in rows], k)


def keyword_only_event_ids(
    conn, question: str, k: int,
    service: Optional[str] = None, since: Optional[datetime] = None,
) -> list[str]:
    params: dict[str, Any] = {"q": question, "k": _CAND}
    if service is not None:
        params["service"] = service
    if since is not None:
        params["since"] = since
    rows = conn.execute(keyword_sql(service, since), params).fetchall()
    return _dedupe([r[1] for r in rows], k)


def hybrid_event_ids(
    conn, embedder: Embedder, question: str, k: int,
    service: Optional[str] = None, since: Optional[datetime] = None,
    tau_s: Optional[float] = None,
) -> list[str]:
    kwargs = {"min_similarity": 0.0}  # eval measures ranking; abstention gated elsewhere
    if tau_s is not None:
        kwargs["tau_s"] = tau_s       # recency-neutral for deterministic benchmark eval
    result = hybrid_search(
        conn, embedder, question, k=_CAND, service=service, since=since, **kwargs,
    )
    return _dedupe([h.event_id for h in result.hits], k)
