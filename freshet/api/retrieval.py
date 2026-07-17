"""Hybrid retrieval: a pgvector cosine arm and a Postgres full-text arm, fused
with reciprocal-rank fusion, recency-weighted, and gated by an abstention
threshold. The SQL builders interpolate only their own literal fragments; every
user value travels as a bound parameter.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from freshet.api.rerank import Reranker

from freshet.pipeline.embedding import Embedder, vec_literal

_COLS = "chunk_id, event_id, service, ts, indexed_at, source, text, type"


def _where(service: Optional[str], since: Optional[datetime]) -> str:
    clauses = []
    if service is not None:
        clauses.append("service = %(service)s")
    if since is not None:
        clauses.append("ts >= %(since)s")
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


def vector_sql(service: Optional[str], since: Optional[datetime]) -> str:
    # chunk_id breaks distance ties deterministically: without it, tied rows come
    # back in physical heap order, which shifts run-to-run (the eval DELETEs and
    # re-INSERTs every run) and makes the benchmark non-reproducible.
    return (
        f"SELECT {_COLS}, 1 - (embedding <=> %(qvec)s::vector) AS similarity"
        " FROM vector_records" + _where(service, since) +
        " ORDER BY embedding <=> %(qvec)s::vector, chunk_id LIMIT %(k)s"
    )


# OR semantics for the keyword arm. websearch_to_tsquery ANDs its terms, which
# zeroes recall when a verbose natural-language question is matched against terse
# operational events (no single event contains every query word). As a
# candidate-generation arm feeding fusion, keyword search should be high-recall:
# swap the &-operators in the (already-sanitized) tsquery for |, so any matching
# term retrieves and ts_rank + RRF + recency do the ranking. Safe against
# injection — websearch_to_tsquery has already parsed user input into a valid
# tsquery before the textual operator swap.
_OR_TSQUERY = "replace(websearch_to_tsquery('english', %(q)s)::text, '&', '|')::tsquery"


def keyword_sql(service: Optional[str], since: Optional[datetime]) -> str:
    where = _where(service, since)
    match = f"text_tsv @@ {_OR_TSQUERY}"
    where = (where + " AND " + match) if where else (" WHERE " + match)
    # ts_rank produces many ties across terse operational events, so rank alone
    # leaves the order to physical heap position (non-reproducible run-to-run).
    # chunk_id is the deterministic tiebreak that makes the benchmark byte-stable.
    return (
        f"SELECT {_COLS},"
        f" ts_rank(text_tsv, {_OR_TSQUERY}) AS rank"
        " FROM vector_records" + where +
        " ORDER BY rank DESC, chunk_id LIMIT %(k)s"
    )


RRF_K = 60  # standard reciprocal-rank-fusion constant


def reciprocal_rank_fusion(
    ranked_lists: list[list[str]], rrf_k: int = RRF_K
) -> list[tuple[str, float]]:
    """Fuse ranked id-lists into one ranking. Each id scores sum(1/(rrf_k+rank))
    across the lists it appears in (rank is 0-based). Returns (id, score)
    descending."""
    scores: dict[str, float] = {}
    for ranked in ranked_lists:
        for rank, cid in enumerate(ranked):
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf_k + rank)
    return sorted(scores.items(), key=lambda kv: kv[1], reverse=True)


def recency_weight(age_s: float, tau_s: float) -> float:
    """Exponential decay in (0, 1]: 1.0 at age 0, halving roughly every
    tau_s*ln2 seconds. Freshness-first retrieval leans on this."""
    return math.exp(-max(0.0, age_s) / tau_s)


def should_abstain(similarities: list[float], min_similarity: float) -> bool:
    """Abstain when nothing is retrieved or the best cosine similarity is below
    the threshold. Similarity (interpretable, 0..1) is a better abstention
    signal than the rank-based fused score. Thresholds are per-embedder,
    calibrated with scripts/calibrate_abstention.py (see pipeline.embedding)."""
    if not similarities:
        return True
    return max(similarities) < min_similarity



# Recency half-weight ~ 21 min — tuned to the scripted demo's incident span.
# Real corpora (status feeds with incidents hours/days old) need a much larger
# tau; override with FRESHET_TAU_S. Note the benchmark evals run recency-neutral
# (tau≈∞), so this decay is a product default, not a benchmarked one.
DEFAULT_TAU_S = 1800.0
# Fallback abstention floor (MiniLM-calibrated). When the embedder carries a
# per-model `min_similarity` attribute (see pipeline.embedding), that wins —
# bge's compressed cosine distribution makes 0.3 effectively "never abstain".
DEFAULT_MIN_SIMILARITY = 0.3
ARM_K = 20                   # per-arm candidate depth before fusion


def _default_tau_s() -> float:
    override = os.environ.get("FRESHET_TAU_S")
    return float(override) if override else DEFAULT_TAU_S


def _default_min_similarity(embedder) -> float:
    return float(getattr(embedder, "min_similarity", DEFAULT_MIN_SIMILARITY))


@dataclass
class RetrievedHit:
    chunk_id: str
    event_id: str
    service: str
    ts: datetime
    indexed_at: datetime
    source: str
    text: str
    type: str
    similarity: float   # best cosine from the vector arm (0.0 if keyword-only)
    score: float        # fused RRF score * recency weight


@dataclass
class HybridResult:
    hits: list[RetrievedHit]
    abstained: bool


@dataclass(frozen=True)
class NeighborEvent:
    event_id: str
    ts: datetime
    type: str
    text: str


def events_around(conn, service: str, ts: datetime, window_s: float = 900.0
                  ) -> list[NeighborEvent]:
    """Temporal neighbours: events for `service` within ±window_s of `ts`,
    time-ordered, deduped by event_id (rows are chunks). No embeddings — this is
    the non-semantic lookup that surfaces a terse change event a single semantic
    query misses."""
    lo, hi = ts - timedelta(seconds=window_s), ts + timedelta(seconds=window_s)
    rows = conn.execute(
        """
        SELECT DISTINCT ON (event_id) event_id, ts, type, text
        FROM vector_records
        WHERE service = %s AND ts BETWEEN %s AND %s
        ORDER BY event_id, ts
        """,
        (service, lo, hi),
    ).fetchall()
    out = [NeighborEvent(event_id=r[0], ts=r[1], type=r[2], text=r[3]) for r in rows]
    out.sort(key=lambda n: n.ts)
    return out


def _rows_to_map(rows: list[tuple], score_idx: int) -> dict[str, tuple[Any, float]]:
    """Map chunk_id -> (row, arm_score). score_idx is the trailing score column."""
    return {r[0]: (r, float(r[score_idx])) for r in rows}


def hybrid_search(
    conn,
    embedder: Embedder,
    question: str,
    k: int = 5,
    service: Optional[str] = None,
    since: Optional[datetime] = None,
    tau_s: Optional[float] = None,
    min_similarity: Optional[float] = None,
    now: Optional[datetime] = None,
    reranker: Optional["Reranker"] = None,
    rerank_pool: int = 30,
) -> HybridResult:
    # None -> resolve defaults: tau from FRESHET_TAU_S (else the demo-tuned
    # constant), abstention floor from the embedder's per-model attribute.
    if tau_s is None:
        tau_s = _default_tau_s()
    if min_similarity is None:
        min_similarity = _default_min_similarity(embedder)
    [qvec] = embedder.encode_query([question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "q": question, "k": ARM_K}
    if service is not None:
        params["service"] = service
    if since is not None:
        params["since"] = since

    vec_rows = conn.execute(vector_sql(service, since), params).fetchall()
    kw_rows = conn.execute(keyword_sql(service, since), params).fetchall()

    vec_map = _rows_to_map(vec_rows, 8)   # similarity is now column index 8
    kw_map = _rows_to_map(kw_rows, 8)     # rank is now column index 8
    fused = reciprocal_rank_fusion([[r[0] for r in vec_rows], [r[0] for r in kw_rows]])

    stamp = now or datetime.now(timezone.utc)
    hits: list[RetrievedHit] = []
    for chunk_id, rrf_score in fused:
        row, _ = vec_map.get(chunk_id) or kw_map[chunk_id]
        similarity = vec_map[chunk_id][1] if chunk_id in vec_map else 0.0
        age = (stamp - row[3]).total_seconds()   # row[3] is ts
        hits.append(
            RetrievedHit(
                chunk_id=row[0], event_id=row[1], service=row[2], ts=row[3],
                indexed_at=row[4], source=row[5], text=row[6], type=row[7],
                similarity=similarity,
                score=rrf_score * recency_weight(age, tau_s),
            )
        )

    hits.sort(key=lambda h: h.score, reverse=True)
    retrieval_topk = hits[:k]
    # abstention keys off retrieval similarities (unchanged), independent of rerank
    abstained = should_abstain([h.similarity for h in retrieval_topk], min_similarity)
    if reranker is not None:
        hits = reranker.rerank(question, hits[:rerank_pool])[:k]
    else:
        hits = retrieval_topk
    return HybridResult(hits=hits, abstained=abstained)
