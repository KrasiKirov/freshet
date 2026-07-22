"""M5 query API: hybrid retrieval (vector + keyword + filters) fused with
reciprocal-rank fusion, recency-weighted, with abstention on weak evidence and
a grounded-answer composer (keyless template default, optional Anthropic).

Run:
    uvicorn freshet.api.app:app --port 8000
Config via env: FRESHET_DSN, FRESHET_EMBEDDER (bge|stub),
FRESHET_COMPOSER (auto|template|anthropic), FRESHET_LLM_MODEL, ANTHROPIC_API_KEY,
FRESHET_TAU_S (opt-in recency decay; default is recency-neutral — see RESULTS M15),
FRESHET_MIN_SIMILARITY (abstention floor; default is per-embedder).
"""

from __future__ import annotations

import json
import os
import threading
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from freshet.api.composer import NO_EVIDENCE, Composer, make_composer
from freshet.api.retrieval import RetrievedHit, hybrid_search
from freshet.pipeline.embedding import Embedder, make_embedder

ABSTAIN_MESSAGE = (
    "I don't have enough recent, relevant evidence to answer that confidently."
)
_STATIC = Path(__file__).parent / "static"

PROMETHEUS_URL = os.environ.get("FRESHET_PROMETHEUS_URL", "http://localhost:9090")

# Same metrics the Grafana dashboard uses, as instant queries.
#
# Pipeline latency (ingested -> queryable), NOT end-to-end freshness
# (ts -> queryable). On replayed or status-feed corpora `ts` is the moment the
# incident happened — often days ago — so end-to-end freshness measures the age
# of the news, not the speed of the pipeline. Pipeline latency is meaningful on
# every corpus; see Event.pipeline_latency_s / end_to_end_latency_s.
# 15m window, not 5m: `make live-demo` polls the feeds once at startup, so all
# ingestion lands in one burst. A 5m window leaves the gauges empty for anyone
# who opens the UI a few minutes later, which reads as broken rather than idle.
_Q_LATENCY_P50 = "histogram_quantile(0.50, sum(rate(freshet_pipeline_latency_seconds_bucket[15m])) by (le))"
_Q_LATENCY_P95 = "histogram_quantile(0.95, sum(rate(freshet_pipeline_latency_seconds_bucket[15m])) by (le))"
# Consumer groups are named per demo (normalizer/embedder, live-norm/live-emb,
# sd-norm/sd-emb, drill-emb...), so match on the role substring rather than
# pinning exact names — otherwise lag silently reads empty outside `make api`.
_Q_CONSUMER_LAG = (
    "sum(clamp_min(sum by (redpanda_group) ("
    'redpanda_kafka_max_offset{redpanda_namespace="kafka",redpanda_topic=~"raw.events|normalized.events"}'
    " - on(redpanda_topic, redpanda_partition) group_right() "
    'redpanda_kafka_consumer_group_committed_offset{redpanda_group=~".*(norm|emb).*"}), 0))'
)


def _prom_instant(query: str) -> float | None:
    """Run a Prometheus instant query; return the scalar value, or None if
    Prometheus is unreachable or the result is empty/NaN. Never raises — a
    down obs stack just yields a dash in the UI."""
    url = PROMETHEUS_URL + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        with urllib.request.urlopen(url, timeout=2.0) as resp:
            data = json.load(resp)
        result = data["data"]["result"]
        if not result:
            return None
        value = float(result[0]["value"][1])
        return None if value != value else value  # NaN check
    except Exception:
        return None


class QueryRequest(BaseModel):
    question: str
    k: int = Field(default=5, ge=1, le=50)
    service: str | None = None
    since: datetime | None = None
    multi_query: bool = False


class Hit(BaseModel):
    chunk_id: str
    event_id: str
    service: str
    ts: datetime
    indexed_at: datetime
    source: str
    text: str
    type: str = ""
    similarity: float
    score: float


class QueryResponse(BaseModel):
    answer: str
    abstained: bool
    hits: list[Hit]


class Stats(BaseModel):
    latency_p50_s: float | None
    latency_p95_s: float | None
    consumer_lag: float | None


class IncidentSummary(BaseModel):
    incident_id: str
    service: str
    latest_ts: datetime
    latest_indexed: datetime
    text: str
    status: str
    severity: str | None = None


_pool = None
_embedder: Embedder | None = None
_composer: Composer | None = None
_deps_lock = threading.Lock()


def get_deps():
    # FastAPI runs sync endpoints in a threadpool: guard the lazy init so two
    # concurrent first requests can't race and build duplicate embedders/pools.
    global _pool, _embedder, _composer
    with _deps_lock:
        if _embedder is None:
            _embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
        if _composer is None:
            _composer = make_composer(os.environ.get("FRESHET_COMPOSER", "auto"))
        if _pool is None:
            from freshet.common.db import make_pool

            _pool = make_pool()
    # yield a pooled connection for this request; it returns to the pool on exit
    with _pool.connection() as conn:
        yield conn, _embedder, _composer


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # close the pool's background threads on shutdown (no-op if never opened);
    # reset the global so a subsequent startup rebuilds it instead of reusing a
    # closed pool
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


app = FastAPI(title="Freshet query API", lifespan=lifespan)


def _to_hit(h: RetrievedHit) -> Hit:
    return Hit(
        chunk_id=h.chunk_id, event_id=h.event_id, service=h.service, ts=h.ts,
        indexed_at=h.indexed_at, source=h.source, text=h.text, type=h.type,
        similarity=h.similarity, score=h.score,
    )


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest, deps=Depends(get_deps)) -> QueryResponse:
    conn, embedder, composer = deps
    if req.multi_query:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise HTTPException(status_code=400,
                                detail="multi_query requires ANTHROPIC_API_KEY")
        from freshet.api.multiquery import multi_query_search
        result = multi_query_search(
            conn, embedder, req.question, k=req.k, service=req.service, since=req.since
        )
    else:
        result = hybrid_search(
            conn, embedder, req.question, k=req.k, service=req.service, since=req.since
        )
    if result.abstained:
        return QueryResponse(answer=ABSTAIN_MESSAGE, abstained=True, hits=[])
    answer = composer.compose(req.question, result.hits)
    return QueryResponse(
        answer=answer or NO_EVIDENCE,
        abstained=False,
        hits=[_to_hit(h) for h in result.hits],
    )


@app.get("/stats", response_model=Stats)
def stats() -> Stats:
    return Stats(
        latency_p50_s=_prom_instant(_Q_LATENCY_P50),
        latency_p95_s=_prom_instant(_Q_LATENCY_P95),
        consumer_lag=_prom_instant(_Q_CONSUMER_LAG),
    )


@app.get("/incidents", response_model=list[IncidentSummary])
def incidents(limit: int = 20, deps=Depends(get_deps)) -> list[IncidentSummary]:
    conn, _, _ = deps
    rows = conn.execute(
        """
        SELECT incident_id, service,
               max(ts)          AS latest_ts,
               max(indexed_at)  AS latest_indexed,
               (array_agg(text ORDER BY ts DESC))[1]     AS text,
               (array_agg(type ORDER BY ts DESC))[1]     AS status,
               (array_agg(severity ORDER BY ts DESC))[1] AS severity
        FROM vector_records
        WHERE incident_id IS NOT NULL AND source = 'alert'
        GROUP BY incident_id, service
        ORDER BY latest_ts DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    return [
        IncidentSummary(
            incident_id=r[0], service=r[1], latest_ts=r[2], latest_indexed=r[3],
            text=r[4], status=r[5], severity=r[6],
        )
        for r in rows
    ]


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC), name="static")
