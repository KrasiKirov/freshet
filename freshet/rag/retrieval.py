"""Hybrid retrieval: a pgvector cosine arm and a Postgres full-text arm, fused
with reciprocal-rank fusion and gated by an abstention threshold. The SQL builders interpolate only their own literal fragments; every
user value travels as a bound parameter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from freshet.pipeline.embedding import Embedder, vec_literal
from freshet.pipeline.index_stats import get_centroid

# Row columns are addressed by POSITION, so the shared prefix and the per-arm
# score columns that follow it are declared together here. Adding a column
# without moving these indices silently mislabels every field after it.
_COLS = "chunk_id, event_id, service, ts, indexed_at, source, text, type, title"
TITLE_IDX = 8
_VEC_SIM_IDX = 9         # vector arm:  ..., title, similarity, centered_similarity
_VEC_CSIM_IDX = 10
_KW_SIM_IDX = 10         # keyword arm: ..., title, rank, similarity, centered_similarity
_KW_CSIM_IDX = 11


# Cosine in the MEAN-CENTERED space. `<=>` normalizes its operands, so
# subtracting the centroid from both sides is the whole transform — no
# client-side maths and no second round trip. Emitted as a constant-width
# column (NULL when no centroid is stored) so the positional indices above
# never depend on runtime state.
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
    # Drops the query's OWN document. When the query is text lifted from an
    # indexed update, that update is trivially its own top hit — and because
    # abstention keys off the top similarity, leaving it in makes the
    # abstention metric structurally incapable of firing.
    if exclude_event_id is not None:
        clauses.append("event_id <> %(exclude_event_id)s")
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


def vector_sql(service: str | None, since: datetime | None,
               exclude_event_id: str | None = None, centered: bool = False) -> str:
    # chunk_id breaks distance ties deterministically: without it, tied rows come
    # back in physical heap order, which shifts run-to-run (the eval DELETEs and
    # re-INSERTs every run) and makes the benchmark non-reproducible.
    return (
        f"SELECT {_COLS}, 1 - (embedding <=> %(qvec)s::vector) AS similarity,"
        + _centered_expr(centered) +
        " FROM vector_records" + _where(service, since, exclude_event_id) +
        " ORDER BY embedding <=> %(qvec)s::vector, chunk_id LIMIT %(k)s"
    )


# OR semantics for the keyword arm. websearch_to_tsquery ANDs its terms, which
# zeroes recall when a verbose natural-language question is matched against terse
# operational events (no single event contains every query word). As a
# candidate-generation arm feeding fusion, keyword search should be high-recall:
# swap the &-operators in the (already-sanitized) tsquery for |, so any matching
# term retrieves and ts_rank + RRF do the ranking. Safe against injection —
# websearch_to_tsquery has already parsed user input into a valid tsquery before
# the textual operator swap.
#
# EXCEPT when the query negates. websearch renders `outage -maintenance` as
# `'outag' & !'mainten'`; swapping the operator yields `'outag' | !'mainten'`,
# which matches every row that merely LACKS "maintenance" — measured on the live
# index, 10,708 of 12,155 rows (88%), and `database errors -scheduled` reached
# 97%. The arm degenerates to near-everything, ts_rank ties out, and 20
# effectively arbitrary candidates enter RRF at full weight. A `-term` is the one
# case where the swap changes meaning rather than just breadth, so those queries
# keep websearch's AND form. `position('!' in ...)` rather than a LIKE pattern
# because a literal % would have to be escaped in this %(name)s format string.
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
    # ts_rank produces many ties across terse operational events, so rank alone
    # leaves the order to physical heap position (non-reproducible run-to-run).
    # chunk_id is the deterministic tiebreak that makes the benchmark byte-stable.
    # Cosine is computed here too, so a hit found ONLY by the lexical arm still
    # carries a real similarity. It used to default to 0.0 — a missing value, not
    # a measured one — and since abstention keys off cosine, an exact lexical
    # match with no vector match was discarded as "no evidence".
    return (
        f"SELECT {_COLS},"
        f" ts_rank(text_tsv, {_OR_TSQUERY}) AS rank,"
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



# Fallback abstention floor (MiniLM-calibrated). When the embedder carries a
# per-model `min_similarity` attribute (see pipeline.embedding), that wins —
# bge's compressed cosine distribution makes 0.3 effectively "never abstain".
DEFAULT_MIN_SIMILARITY = 0.3
ARM_K = 20                   # per-arm candidate depth before fusion



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
    # None -> abstention floor from the embedder's per-model attribute.
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
    # An explicit filter changes the relevance contract. The cosine floor was
    # calibrated for "is this specific thing in the corpus?", where a strong match
    # should exist. A filtered query ("what happened today?") is a BROWSE: it
    # resembles no single incident, and a real outage scored 0.549 against it —
    # correct evidence the floor would veto. When the caller narrowed the candidate
    # set by time or service, that filter IS the relevance signal, so abstention
    # means "the window is empty", not "no strong semantic match". The unfiltered
    # path keeps the calibrated floor exactly as it was.
    if service is not None or since is not None:
        return HybridResult(hits=retrieval_topk, abstained=not retrieval_topk)
    # The centered space is the better abstention signal — see
    # freshet/pipeline/index_stats.py. Ranking stays in raw cosine either way.
    csims = [h.centered_similarity for h in retrieval_topk
             if h.centered_similarity is not None]
    if centered_floor is not None and csims:
        abstained = should_abstain(csims, centered_floor)
    else:
        abstained = should_abstain([h.similarity for h in retrieval_topk], min_similarity)
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
