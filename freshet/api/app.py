"""M2 query API: vector-only top-k over vector_records.

Deliberately minimal — hybrid retrieval, recency weighting, abstention, and
answer composition arrive in M5. Exists so the vertical slice is provable
end to end.

Run:
    uvicorn freshet.api.app:app --port 8000
Config via env: FRESHET_DSN, FRESHET_EMBEDDER (minilm|stub).
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from fastapi import Depends, FastAPI
from pydantic import BaseModel, Field

from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal


class QueryRequest(BaseModel):
    question: str
    k: int = Field(default=5, ge=1, le=50)
    service: Optional[str] = None
    since: Optional[datetime] = None


class Hit(BaseModel):
    chunk_id: str
    event_id: str
    service: str
    ts: datetime
    indexed_at: datetime
    source: str
    text: str
    score: float


class QueryResponse(BaseModel):
    hits: list[Hit]


def topk_sql(service: Optional[str], since: Optional[datetime]) -> str:
    where = []
    if service is not None:
        where.append("service = %(service)s")
    if since is not None:
        where.append("ts >= %(since)s")
    where_clause = (" WHERE " + " AND ".join(where)) if where else ""
    return (
        "SELECT chunk_id, event_id, service, ts, indexed_at, source, text,"
        " 1 - (embedding <=> %(qvec)s::vector) AS score"
        " FROM vector_records" + where_clause +
        " ORDER BY embedding <=> %(qvec)s::vector LIMIT %(k)s"
    )


def search(conn, embedder: Embedder, req: QueryRequest) -> list[Hit]:
    [qvec] = embedder.encode([req.question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "k": req.k}
    if req.service is not None:
        params["service"] = req.service
    if req.since is not None:
        params["since"] = req.since
    rows = conn.execute(topk_sql(req.service, req.since), params).fetchall()
    return [
        Hit(
            chunk_id=r[0], event_id=r[1], service=r[2], ts=r[3],
            indexed_at=r[4], source=r[5], text=r[6], score=float(r[7]),
        )
        for r in rows
    ]


_conn = None
_embedder: Optional[Embedder] = None


def get_deps():
    global _conn, _embedder
    if _embedder is None:
        _embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "minilm"))
    if _conn is None:
        from freshet.common.db import connect

        _conn = connect()
    return _conn, _embedder


app = FastAPI(title="Freshet query API")


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest, deps=Depends(get_deps)) -> QueryResponse:
    conn, embedder = deps
    return QueryResponse(hits=search(conn, embedder, req))
