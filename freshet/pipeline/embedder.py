"""Embedding worker: normalized.updates -> embed -> idempotent upsert into pgvector.

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

from freshet.common.heartbeat import Heartbeat
from freshet.common.incidents import ensure_incident
from freshet.common.schemas import Event, VectorRecord
from freshet.pipeline.chunking import chunk_text
from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter
from freshet.pipeline.embedding import Embedder, make_embedder, vec_literal
from freshet.pipeline.metrics import (
    DEADLETTER_EVENTS,
    EMBEDDER_MESSAGES,
    FRESHNESS,
    INDEXED_EVENTS,
    PIPELINE_LATENCY,
    start_metrics_server,
)

# Produced by the Flink dedup job (freshet/stream/dedup_job.py). The poller is
# stateless and writes raw.incidents; everything downstream of dedup reads this.
NORMALIZED_TOPIC = "normalized.updates"


# A title is the "<incident_name>: " prefix Flink prepends to every update. Split
# on the first ": " only — titles legitimately contain colons ("Aug 10: 30am UTC"),
# and the incident name is always the leading segment.
MAX_TITLE_LEN = 120


def title_of(text: str) -> str | None:
    """The incident name prefixed to an update's text, or None if absent."""
    head, sep, rest = text.partition(": ")
    if not sep or not rest.strip():
        return None
    head = head.strip()
    return head if 0 < len(head) <= MAX_TITLE_LEN else None


def records_for_event(ev: Event, now: datetime | None = None) -> list[VectorRecord]:
    """One record per text chunk. chunk_id derives from event_id + index, so
    redelivery and replay overwrite the same rows (idempotent). Blank text
    yields no records."""
    stamp = now or datetime.now(UTC)
    # Prefer the field Flink sends; derive only for legacy messages that lack it.
    title = ev.title or title_of(ev.text)
    return [
        VectorRecord(
            chunk_id=f"chk_{ev.event_id}_{i}",
            event_id=ev.event_id,
            incident_id=ev.incident_id,
            service=ev.service,
            ts=ev.ts,
            indexed_at=stamp,
            text=chunk,
            title=title,
            source=ev.source,
            severity=ev.severity,
            type=ev.type,
        )
        for i, chunk in enumerate(chunk_text(ev.text))
    ]


# The chunk index is the trailing _N of chunk_id; anything at or beyond the
# current chunk count is left over from a previous, longer version of this text.
_DELETE_ORPHAN_CHUNKS_SQL = (
    "DELETE FROM vector_records WHERE event_id = %s"
    " AND (regexp_match(chunk_id, '_(\\d+)$'))[1]::int >= %s")
# incident_events exists in the schema but nothing wrote to it.
_INCIDENT_EVENT_SQL = (
    "INSERT INTO incident_events (incident_id, event_id) VALUES (%s, %s)"
    " ON CONFLICT DO NOTHING")

UPSERT_SQL = """
INSERT INTO vector_records
    (chunk_id, event_id, incident_id, service, ts, indexed_at, source, text, title, severity, type, embedding, model)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s)
ON CONFLICT (chunk_id) DO UPDATE
    SET indexed_at = EXCLUDED.indexed_at,
        text = EXCLUDED.text,
        severity = EXCLUDED.severity,
        type = EXCLUDED.type,
        embedding = EXCLUDED.embedding,
        model = EXCLUDED.model,
        title = EXCLUDED.title
"""


def upsert_record(conn, rec: VectorRecord, embedding: list[float],
                  model: str | None = None) -> None:
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
            rec.title,
            rec.severity.value if rec.severity else None,
            rec.type,
            vec_literal(embedding),
            model,
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
                 heartbeat: Heartbeat | None = None,
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

    heartbeat = heartbeat or Heartbeat("embedder")

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
            upsert_record(conn, rec, vector, getattr(emb, "name", None))
            observe_indexed(rec, ingested_at=ev.ingested_at)
        # Re-embedding a SHORTER text leaves the previous run's extra chunks
        # behind: chunk_id is per index, so upserts overwrite _0.._n and orphan
        # _n+1.. — stale text that still answers queries.
        conn.execute(_DELETE_ORPHAN_CHUNKS_SQL, (ev.event_id, len(records)))
        # Autopilot claims against `incidents`; without a row its UPDATE matches
        # nothing and the incident is silently never briefed. Written after the
        # upserts so a failed index does not leave a claimable row with no evidence.
        ensure_incident(conn, ev.incident_id, ev.service, ev.ts, ev.title or "")
        if ev.incident_id:
            conn.execute(_INCIDENT_EVENT_SQL, (ev.incident_id, ev.event_id))
        EMBEDDER_MESSAGES.inc()      # one per Kafka message, not per chunk
        # Proof of uptime for the freshness eval: without it a catch-up burst
        # after an outage is indistinguishable from a slow pipeline.
        heartbeat.beat(conn)

    return handle


def _beat(heartbeat: Heartbeat, conn) -> None:
    """consume_loop wants a None-returning hook."""
    heartbeat.beat(conn)


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
    # One heartbeat shared by the handler and the idle tick. Beating only on
    # handled messages made a quiet stretch indistinguishable from downtime:
    # at ~2 updates/hour the freshness window reset every few minutes and the
    # measurement could never accumulate.
    heartbeat = Heartbeat("embedder")
    handle = make_handler(conn, emb, producer, topic=topic,
                          deadletter_topic=deadletter_topic, heartbeat=heartbeat)

    try:
        n = consume_loop(brokers, group, [topic], handle, max_messages,
                         auto_commit=False, stop=stop,
                         idle_timeout_s=idle_timeout_s,
                         idle_hook=lambda: _beat(heartbeat, conn))
    finally:
        producer.flush()
        conn.close()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet embedding worker (normalized.updates -> pgvector)")
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
