"""Embedding worker: normalized.events -> embed -> idempotent upsert into pgvector.

chunk_id derives from event_id, so redelivered or replayed events overwrite
their own row instead of duplicating (at-least-once + idempotent = effectively
once in the index). One chunk per event at M2; chunking for long texts (e.g.
postmortems) arrives in M4.

Run (stack up first; use --embedder stub to skip the model download):
    python -m freshet.pipeline.embedder --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event, VectorRecord
from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal
from freshet.pipeline.metrics import FRESHNESS, INDEXED_EVENTS, start_metrics_server
from freshet.pipeline.normalizer import NORMALIZED_TOPIC


def to_vector_record(ev: Event, now: Optional[datetime] = None) -> VectorRecord:
    return VectorRecord(
        chunk_id=f"chk_{ev.event_id}_0",
        event_id=ev.event_id,
        incident_id=ev.incident_id,
        service=ev.service,
        ts=ev.ts,
        indexed_at=now or datetime.now(timezone.utc),
        text=ev.text,
        source=ev.source,
    )


UPSERT_SQL = """
INSERT INTO vector_records
    (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, embedding)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
ON CONFLICT (chunk_id) DO UPDATE
    SET indexed_at = EXCLUDED.indexed_at,
        text = EXCLUDED.text,
        embedding = EXCLUDED.embedding
"""


def upsert_record(conn, rec: VectorRecord, embedding: list[float]) -> None:
    conn.execute(
        UPSERT_SQL,
        (
            rec.chunk_id,
            rec.event_id,
            rec.incident_id,
            rec.service,
            rec.ts,
            rec.indexed_at,
            rec.source.value,
            rec.text,
            vec_literal(embedding),
        ),
    )


def observe_indexed(rec: VectorRecord) -> None:
    """Record metrics for one indexed (queryable) record."""
    INDEXED_EVENTS.inc()
    FRESHNESS.observe((rec.indexed_at - rec.ts).total_seconds())


def run(
    brokers: str,
    group: str = "embedder",
    max_messages: Optional[int] = None,
    topic: str = NORMALIZED_TOPIC,
    embedder: Optional[Embedder] = None,
    dsn: Optional[str] = None,
    metrics_port: int = 0,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop

    emb = embedder or make_embedder("minilm")
    conn = connect(dsn)

    def handle(value: str) -> None:
        ev = Event.model_validate_json(value)
        if not ev.text.strip():
            return
        rec = to_vector_record(ev)
        [vector] = emb.encode([rec.text])
        upsert_record(conn, rec, vector)
        observe_indexed(rec)

    try:
        n = consume_loop(brokers, group, [topic], handle, max_messages, auto_commit=False)
    finally:
        conn.close()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet embedding worker (normalized.events -> pgvector)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="embedder")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--embedder", choices=["minilm", "stub"], default="minilm")
    p.add_argument("--dsn", default=None)
    p.add_argument("--metrics-port", type=int, default=8002, help="Prometheus /metrics port (0 disables)")
    a = p.parse_args()
    n = run(a.brokers, group=a.group, max_messages=a.max, embedder=make_embedder(a.embedder), dsn=a.dsn, metrics_port=a.metrics_port)
    print(f"[embedder] processed {n} messages")


if __name__ == "__main__":
    main()
