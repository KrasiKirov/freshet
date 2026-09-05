"""The index's own geometry, recorded per embedding model.

bge's cosine space is anisotropic: on this corpus, RANDOM unrelated chunk pairs
average 0.594 and 12.2% of them clear the 0.70 abstention floor. An absolute
floor in that space cuts a percentile, not a meaning — which is why
calibrate_abstention could find no threshold separating on-corpus from
off-corpus questions.

Subtracting the corpus mean removes the shared component. Measured on the live
index (55 labels, self-document excluded), raw @0.70 gives 4/55 false
abstentions and centered @0.44 gives 2/55, with all 6 off-corpus questions still
rejected in both.

The centroid is stored, not recomputed per query: it is an aggregate over the
whole index and moves slowly. It is refreshed by `make index-stats`, and a
missing one is not an error — the query path falls back to the raw floor.
"""

from __future__ import annotations

import time

_CENTROID_SQL = ("SELECT avg(embedding)::text, count(*)"
                 " FROM vector_records WHERE model = %s")
_UPSERT_SQL = (
    "INSERT INTO index_stats (model, centroid, n_chunks, computed_at)"
    " VALUES (%s, %s::vector, %s, now())"
    " ON CONFLICT (model) DO UPDATE"
    "   SET centroid = EXCLUDED.centroid,"
    "       n_chunks = EXCLUDED.n_chunks,"
    "       computed_at = EXCLUDED.computed_at")
_READ_SQL = "SELECT centroid::text FROM index_stats WHERE model = %s"

# The centroid is an average over the whole index; a few minutes of drift is
# immaterial, and re-reading it every query would add a hot-path round trip.
CENTROID_TTL_S = 300.0

_CACHE: dict[str, tuple[float, str | None]] = {}


def clear_cache() -> None:
    """Drop the in-process centroid cache (tests, and after a refresh)."""
    _CACHE.clear()


def compute_centroid(conn, model: str) -> str | None:
    """The mean embedding over `model`'s rows, as a pgvector text literal.

    None when the model has no rows: an empty index has no geometry, and the
    caller falls back to the raw-cosine floor rather than failing."""
    row = conn.execute(_CENTROID_SQL, (model,)).fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def refresh_centroid(conn, model: str) -> int:
    """Recompute and store the centroid. Returns the row count behind it."""
    row = conn.execute(_CENTROID_SQL, (model,)).fetchone()
    if not row or row[0] is None:
        return 0
    centroid, n = row[0], int(row[1])
    conn.execute(_UPSERT_SQL, (model, centroid, n))
    clear_cache()
    return n


def get_centroid(conn, model: str, now=time.monotonic) -> str | None:
    """The stored centroid for `model`, cached for CENTROID_TTL_S.

    A blank model name means the embedder carries no provenance (StubEmbedder),
    so there is nothing to look up and no query is issued."""
    if not model:
        return None
    hit = _CACHE.get(model)
    stamp = now()
    if hit is not None and stamp - hit[0] < CENTROID_TTL_S:
        return hit[1]
    row = conn.execute(_READ_SQL, (model,)).fetchone()
    value = row[0] if row else None
    _CACHE[model] = (stamp, value)
    return value


def main() -> None:
    import argparse

    from freshet.common.db import connect

    p = argparse.ArgumentParser(description="Recompute the index centroid used by abstention")
    p.add_argument("--model", default="BAAI/bge-base-en-v1.5")
    p.add_argument("--dsn", default=None)
    a = p.parse_args()
    conn = connect(a.dsn)
    n = refresh_centroid(conn, a.model)
    if not n:
        print(f"[index-stats] no rows for {a.model!r} — nothing stored")
        return
    print(f"[index-stats] centroid for {a.model!r} recomputed over {n} chunks")


if __name__ == "__main__":
    main()
