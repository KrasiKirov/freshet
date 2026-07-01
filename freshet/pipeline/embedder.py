"""Embedding worker: normalized.events -> embed -> idempotent upsert into pgvector.

chunk_id derives from event_id, so redelivered or replayed events overwrite
their own row instead of duplicating (at-least-once + idempotent = effectively
once in the index). Long texts are chunked; each chunk is its own idempotent row.

Run (stack up first; use --embedder stub to skip the model download):
    python -m freshet.pipeline.embedder --brokers localhost:9092
"""

from __future__ import annotations

import argparse
import signal
import threading
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event, VectorRecord
from freshet.pipeline.chunking import chunk_text
from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter
from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal
from freshet.pipeline.metrics import DEADLETTER_EVENTS, FRESHNESS, INDEXED_EVENTS, start_metrics_server
from freshet.pipeline.normalizer import NORMALIZED_TOPIC


def records_for_event(ev: Event, now: Optional[datetime] = None) -> list[VectorRecord]:
    """One record per text chunk. chunk_id derives from event_id + index, so
    redelivery and replay overwrite the same rows (idempotent). Blank text
    yields no records."""
    stamp = now or datetime.now(timezone.utc)
    return [
        VectorRecord(
            chunk_id=f"chk_{ev.event_id}_{i}",
            event_id=ev.event_id,
            incident_id=ev.incident_id,
            service=ev.service,
            ts=ev.ts,
            indexed_at=stamp,
            text=chunk,
            source=ev.source,
            severity=ev.severity,
            type=ev.type,
        )
        for i, chunk in enumerate(chunk_text(ev.text))
    ]


UPSERT_SQL = """
INSERT INTO vector_records
    (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, severity, type, embedding)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
ON CONFLICT (chunk_id) DO UPDATE
    SET indexed_at = EXCLUDED.indexed_at,
        text = EXCLUDED.text,
        severity = EXCLUDED.severity,
        type = EXCLUDED.type,
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
            rec.severity.value if rec.severity else None,
            rec.type,
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
    deadletter_topic: str = DEADLETTER_TOPIC,
    metrics_port: int = 0,
    stop: Optional[threading.Event] = None,
    idle_timeout_s: Optional[float] = None,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop, make_producer, produce_sync

    emb = embedder or make_embedder("bge")
    conn = connect(dsn)
    producer = make_producer(brokers)

    def handle(value: str) -> None:
        try:
            ev = Event.model_validate_json(value)
        except Exception as e:
            produce_sync(producer, deadletter_topic, build_deadletter(str(e), value, topic))
            DEADLETTER_EVENTS.inc()
            return
        records = records_for_event(ev)
        if not records:
            return
        vectors = emb.encode([r.text for r in records])
        if len(vectors) != len(records):
            # zip would silently truncate; a miscounting embedder must fail loudly
            raise RuntimeError(f"embedder returned {len(vectors)} vectors for {len(records)} chunks")
        for rec, vector in zip(records, vectors):
            upsert_record(conn, rec, vector)
            observe_indexed(rec)

    try:
        n = consume_loop(brokers, group, [topic], handle, max_messages, auto_commit=False, stop=stop, idle_timeout_s=idle_timeout_s)
    finally:
        producer.flush()
        conn.close()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet embedding worker (normalized.events -> pgvector)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="embedder")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--embedder", choices=["minilm", "stub", "bge"], default="bge")
    p.add_argument("--dsn", default=None)
    p.add_argument("--metrics-port", type=int, default=8002, help="Prometheus /metrics port (0 disables)")
    p.add_argument("--idle-timeout", type=float, default=None, help="exit after N seconds without messages (replay)")
    a = p.parse_args()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    n = run(a.brokers, group=a.group, max_messages=a.max, embedder=make_embedder(a.embedder), dsn=a.dsn, metrics_port=a.metrics_port, stop=stop, idle_timeout_s=a.idle_timeout)
    print(f"[embedder] processed {n} messages")


if __name__ == "__main__":
    main()
