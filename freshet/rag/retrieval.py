"""Hybrid retrieval: a pgvector cosine arm and a Postgres full-text arm, fused
with reciprocal-rank fusion and gated by an abstention threshold. The SQL builders interpolate only their own literal fragments; every
user value travels as a bound parameter.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from freshet.pipeline.embedding import Embedder, vec_literal
from freshet.pipeline.index_stats import get_centroid
from freshet.pipeline.metrics import ABSTENTIONS

# Columns are addressed by POSITION; adding one without shifting these
# indices silently mislabels every field after it.
_COLS = "chunk_id, event_id, service, ts, indexed_at, source, text, type, title"
TITLE_IDX = 8
_VEC_SIM_IDX = 9         # vector arm:  ..., title, similarity, centered_similarity
_VEC_CSIM_IDX = 10
_KW_SIM_IDX = 10         # keyword arm: ..., title, rank, similarity, centered_similarity
_KW_CSIM_IDX = 11


# Mean-centered cosine: `<=>` normalizes operands, so subtracting the centroid
# from both sides is the whole transform. Always emitted (NULL if no centroid)
# so the positional indices above stay constant-width.
def _centered_expr(centered: bool) -> str:
    if not centered:
        return " NULL::double precision AS centered_similarity"
    return (" 1 - ((embedding - %(centroid)s::vector)"
            " <=> (%(qvec)s::vector - %(centroid)s::vector)) AS centered_similarity")


def _where(service: str | None, since: datetime | None,
           exclude_event_id: str | None = None) -> str:
    clauses = []
    if service is not None:
        clauses.append("service = %(service)s")
    if since is not None:
        clauses.append("ts >= %(since)s")
    # Drops the query's own document — otherwise it's trivially its own top
    # hit, making the abstention metric structurally unable to fire.
    if exclude_event_id is not None:
        clauses.append("event_id <> %(exclude_event_id)s")
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


def vector_sql(service: str | None, since: datetime | None,
               exclude_event_id: str | None = None, centered: bool = False) -> str:
    # chunk_id breaks distance ties deterministically — without it, tied rows
    # come back in heap order, which shifts run-to-run since the eval re-INSERTs.
    return (
        f"SELECT {_COLS}, 1 - (embedding <=> %(qvec)s::vector) AS similarity,"
        + _centered_expr(centered) +
        " FROM vector_records" + _where(service, since, exclude_event_id) +
        " ORDER BY embedding <=> %(qvec)s::vector, chunk_id LIMIT %(k)s"
    )


# websearch_to_tsquery ANDs terms, killing recall for verbose questions against
# terse events. Swap & for | so any term matches and ts_rank/RRF do the
# ranking — safe, since the swap runs on an already-parsed tsquery.
#
# EXCEPT negated queries: `outage -maintenance` -> `'outag' & !'mainten'`; the
# swap to `|` matches every row merely lacking "maintenance" (measured 88% and
# 97% of the index on two live queries), degenerating the arm to near-everything.
# So a `-term` keeps websearch's AND form. `position('!' in ...)` avoids
# escaping a literal % in this %(name)s format string.
_WS_TSQUERY = "websearch_to_tsquery('english', %(q)s)"
_OR_TSQUERY = (
    f"CASE WHEN position('!' in {_WS_TSQUERY}::text) > 0"
    f" THEN {_WS_TSQUERY}"
    f" ELSE replace({_WS_TSQUERY}::text, '&', '|')::tsquery END"
)


def keyword_sql(service: str | None, since: datetime | None,
                exclude_event_id: str | None = None, centered: bool = False) -> str:
    where = _where(service, since, exclude_event_id)
    match = f"text_tsv @@ {_OR_TSQUERY}"
    where = (where + " AND " + match) if where else (" WHERE " + match)
    # ts_rank ties heavily on terse events, leaving the chunk_id hash to decide
    # LIMIT survivors. ts_rank_cd scores cover density instead (flag 32
    # normalizes by rank+1). Measured on 55 live labels: keyword_only recall@5
    # 0.309->0.364, mrr 0.232->0.251; hybrid recall@5 0.455->0.473, mrr
    # 0.328->0.313.
    # Cosine computed here too so a lexical-only hit isn't discarded by
    # abstention (used to default to 0.0, wrongly reading as "no evidence").
    return (
        f"SELECT {_COLS},"
        f" ts_rank_cd(text_tsv, {_OR_TSQUERY}, 32) AS rank,"
        f" 1 - (embedding <=> %(qvec)s::vector) AS similarity,"
        + _centered_expr(centered) +
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



def should_abstain(similarities: list[float], min_similarity: float) -> bool:
    """Abstain when nothing is retrieved or the best cosine similarity is below
    the threshold. Similarity (interpretable, 0..1) is a better abstention
    signal than the rank-based fused score. Thresholds are per-embedder,
    calibrated with freshet/eval/calibrate_abstention.py (see pipeline.embedding)."""
    if not similarities:
        return True
    return max(similarities) < min_similarity



# Fallback floor (MiniLM-calibrated); a per-model `min_similarity` attribute
# wins when set — bge's compressed cosine makes 0.3 effectively never-abstain.
DEFAULT_MIN_SIMILARITY = 0.3


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


# Per-arm depth before fusion. Vector-arm recall@5 0.436, recall@20 0.655,
# recall@50 0.764 — a third of found answers sit below this cut. Raising costs
# one larger LIMIT, no LLM tokens (delivered k is a separate decision). Sweep
# via FRESHET_ARM_K.
ARM_K = _int_env("FRESHET_ARM_K", 20)



def _default_min_similarity(embedder) -> float:
    return float(getattr(embedder, "min_similarity", DEFAULT_MIN_SIMILARITY))


def _default_min_similarity_centered(embedder) -> float | None:
    value = getattr(embedder, "min_similarity_centered", None)
    return float(value) if value is not None else None


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
    similarity: float   # measured cosine, whichever arm found the hit
    score: float        # fused RRF score (recency decay was deleted)
    # Optional and last: the chunk is always present, the incident's name is not
    # (legacy rows predate the column). Defaulting keeps every existing caller valid.
    title: str | None = None
    # Cosine measured after subtracting the index centroid. None when no
    # centroid is stored — abstention then falls back to the raw floor.
    centered_similarity: float | None = None


@dataclass
class HybridResult:
    hits: list[RetrievedHit]
    abstained: bool



def _rows_to_map(rows: list[tuple], score_idx: int) -> dict[str, tuple[Any, float]]:
    """Map chunk_id -> (row, arm_score). score_idx is the trailing score column."""
    return {r[0]: (r, float(r[score_idx])) for r in rows}


def hybrid_search(
    conn,
    embedder: Embedder,
    question: str,
    k: int = 5,
    service: str | None = None,
    since: datetime | None = None,
    min_similarity: float | None = None,
    exclude_event_id: str | None = None,
) -> HybridResult:
    if min_similarity is None:
        min_similarity = _default_min_similarity(embedder)
    centered_floor = _default_min_similarity_centered(embedder)
    model = getattr(embedder, "name", "") or ""
    # Only worth the centered arm if this model HAS a centered calibration;
    # otherwise the column would be measured against a floor nobody set.
    centroid = get_centroid(conn, model) if centered_floor is not None else None
    [qvec] = embedder.encode_query([question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "q": question, "k": ARM_K}
    if service is not None:
        params["service"] = service
    if since is not None:
        params["since"] = since
    if exclude_event_id is not None:
        params["exclude_event_id"] = exclude_event_id
    if centroid is not None:
        params["centroid"] = centroid

    centered = centroid is not None
    vec_rows = conn.execute(
        vector_sql(service, since, exclude_event_id, centered), params).fetchall()
    kw_rows = conn.execute(
        keyword_sql(service, since, exclude_event_id, centered), params).fetchall()

    vec_map = _rows_to_map(vec_rows, _VEC_SIM_IDX)
    kw_map = _rows_to_map(kw_rows, _KW_SIM_IDX)
    fused = reciprocal_rank_fusion([[r[0] for r in vec_rows], [r[0] for r in kw_rows]])

    hits: list[RetrievedHit] = []
    for chunk_id, rrf_score in fused:
        row, _ = vec_map.get(chunk_id) or kw_map[chunk_id]
        # both arms now report cosine, so a keyword-only hit is not treated as
        # having zero similarity
        similarity = (vec_map[chunk_id][1] if chunk_id in vec_map
                      else kw_map[chunk_id][1])
        csim_idx = _VEC_CSIM_IDX if chunk_id in vec_map else _KW_CSIM_IDX
        centered_similarity = row[csim_idx]
        hits.append(
            RetrievedHit(
                chunk_id=row[0], event_id=row[1], service=row[2], ts=row[3],
                indexed_at=row[4], source=row[5], text=row[6], type=row[7],
                title=row[TITLE_IDX],
                similarity=similarity,
                score=rrf_score,
                centered_similarity=(None if centered_similarity is None
                                     else float(centered_similarity)),
            )
        )

    hits.sort(key=lambda h: h.score, reverse=True)
    retrieval_topk = hits[:k]
    # An explicit filter changes the relevance contract: the calibrated cosine
    # floor assumes "is this specific thing in the corpus?", but a filtered
    # browse query resembles no single incident (a real outage scored 0.549,
    # which the floor would veto). So a time/service filter IS the relevance
    # signal — abstention there just means "the window is empty".
    if service is not None or since is not None:
        if not retrieval_topk:
            ABSTENTIONS.inc()
        return HybridResult(hits=retrieval_topk, abstained=not retrieval_topk)
    # The centered space is the better abstention signal — see
    # freshet/pipeline/index_stats.py. Ranking stays in raw cosine either way.
    csims = [h.centered_similarity for h in retrieval_topk
             if h.centered_similarity is not None]
    if centered_floor is not None and csims:
        abstained = should_abstain(csims, centered_floor)
    else:
        abstained = should_abstain([h.similarity for h in retrieval_topk], min_similarity)
    # Counted regardless of WHICH floor decided it: the metric answers "how often do
    # we refuse to answer", and that question does not change with the signal used.
    if abstained:
        ABSTENTIONS.inc()
    return HybridResult(hits=retrieval_topk, abstained=abstained)


# Demo-scale index: a full GROUP BY is cheap. At production scale this would move
# to a small provenance table written once per indexing run.
_INDEX_MODELS_SQL = "SELECT coalesce(model, 'unknown'), count(*) FROM vector_records GROUP BY 1"


def check_index_model(conn, embedder) -> str | None:
    """Compare the index's embedding provenance against the querying embedder.

    Raises on a genuine model conflict: vectors from two models are not
    comparable, so every similarity collapses toward zero and the API abstains on
    everything — which reads as "no relevant evidence" and hides the real cause.
    Failing loudly is the whole point. Rows predating the `model` column are
    labelled 'unknown' and only produce a returned warning, since a legacy index
    is usually fine and must not block startup.
    """
    name = getattr(embedder, "name", None)
    if name is None:
        return None
    counts = dict(conn.execute(_INDEX_MODELS_SQL).fetchall())
    if not counts:
        return None                       # empty index: nothing to conflict with
    conflicting = {m: n for m, n in counts.items() if m not in (name, "unknown")}
    if conflicting:
        raise RuntimeError(
            f"index/embedder mismatch: querying with {name!r}, but the index holds "
            f"{conflicting} — those vectors are not comparable, so every query would "
            f"abstain. Re-index with {name!r} (make embedder) or set FRESHET_EMBEDDER "
            f"to the model that built the index.")
    if "unknown" in counts:
        return (f"{counts['unknown']} rows predate embedding provenance; assuming "
                f"they were built with {name!r}")
    return None
