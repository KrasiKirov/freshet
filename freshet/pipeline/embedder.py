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
import time
from datetime import UTC, datetime

from freshet.common.schemas import Event, VectorRecord
from freshet.pipeline.chunking import chunk_text
from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter
from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal
from freshet.pipeline.metrics import (
    DEADLETTER_EVENTS,
    FRESHNESS,
    INDEXED_EVENTS,
    PIPELINE_LATENCY,
    start_metrics_server,
)
from freshet.pipeline.normalizer import NORMALIZED_TOPIC


def records_for_event(ev: Event, now: datetime | None = None) -> list[VectorRecord]:
    """One record per text chunk. chunk_id derives from event_id + index, so
    redelivery and replay overwrite the same rows (idempotent). Blank text
    yields no records."""
    stamp = now or datetime.now(UTC)
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


def observe_indexed(rec: VectorRecord, ingested_at: datetime | None = None) -> None:
    """Record metrics for one indexed (queryable) record.

    `ingested_at` (from the source Event) additionally records pipeline latency.
    It is optional because VectorRecord does not carry it; without it only
    end-to-end freshness is observed."""
    INDEXED_EVENTS.inc()
    FRESHNESS.observe((rec.indexed_at - rec.ts).total_seconds())
    if ingested_at is not None:
        PIPELINE_LATENCY.observe((rec.indexed_at - ingested_at).total_seconds())


# Encode failures retry this many times inline before the message dead-letters,
# so one poison event cannot crash-loop the worker (crash → redelivery → crash).
EMBED_ATTEMPTS = 3


def make_handler(conn, emb: Embedder, producer, *,
                 topic: str = NORMALIZED_TOPIC,
                 deadletter_topic: str = DEADLETTER_TOPIC,
                 attempts: int = EMBED_ATTEMPTS,
                 sleep=time.sleep):
    """Build the per-message handler: parse → embed (with retry) → upsert.

    Parse failures and repeated embed failures dead-letter the message.
    Upsert failures propagate: they are infrastructure problems, not message
    poison (the resilient connection has already retried reconnects), and
    dead-lettering them during a DB outage would drain the stream into the DLQ.
    """
    from freshet.common.kafka_io import produce_sync

    def _dead_letter(error: str, value: str) -> None:
        produce_sync(producer, deadletter_topic, build_deadletter(error, value, topic))
        DEADLETTER_EVENTS.inc()

    def handle(value: str) -> None:
        try:
            ev = Event.model_validate_json(value)
        except Exception as e:
            _dead_letter(str(e), value)
            return
        records = records_for_event(ev)
        if not records:
            return
        for attempt in range(1, attempts + 1):
            try:
                vectors = emb.encode([r.text for r in records])
                break
            except Exception as e:
                if attempt == attempts:
                    _dead_letter(f"embed failed after {attempts} attempts: {e}", value)
                    return
                sleep(0.2 * attempt)
        if len(vectors) != len(records):
            # zip would silently truncate; a miscounting embedder is a code
            # bug, not message poison — fail loudly
            raise RuntimeError(f"embedder returned {len(vectors)} vectors for {len(records)} chunks")
        for rec, vector in zip(records, vectors, strict=True):
            upsert_record(conn, rec, vector)
            observe_indexed(rec, ingested_at=ev.ingested_at)

    return handle


def run(
    brokers: str,
    group: str = "embedder",
    max_messages: int | None = None,
    topic: str = NORMALIZED_TOPIC,
    embedder: Embedder | None = None,
    dsn: str | None = None,
    deadletter_topic: str = DEADLETTER_TOPIC,
    metrics_port: int = 0,
    stop: threading.Event | None = None,
    idle_timeout_s: float | None = None,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop, make_producer

    emb = embedder or make_embedder("bge")
    conn = connect(dsn)
    producer = make_producer(brokers)
    handle = make_handler(conn, emb, producer, topic=topic, deadletter_topic=deadletter_topic)

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
    p.add_argument("--embedder", choices=["stub", "bge"], default="bge")
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
