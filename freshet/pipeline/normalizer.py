"""Normalizer worker: raw.events -> validate -> correlate -> normalized.events.

Validates each payload against the canonical Event schema, stamps ingested_at,
attaches the event to an incident (state in Postgres), and republishes keyed
by service. Invalid payloads go to the dead-letter topic, never silently
dropped. The produce is flushed before the consumer offset commits, so a crash
can only cause redelivery (at-least-once), not loss.

Run (stack up first):
    python -m freshet.pipeline.normalizer --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event
from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter
from freshet.pipeline.metrics import (
    DEADLETTER_EVENTS,
    INGEST_LAG,
    NORMALIZED_EVENTS,
    start_metrics_server,
)
from freshet.pipeline.incidents import correlate

RAW_TOPIC = "raw.events"
NORMALIZED_TOPIC = "normalized.events"


def normalize(value: str, now: Optional[datetime] = None) -> Optional[Event]:
    """Parse and validate one raw payload; stamp ingested_at. None if invalid."""
    try:
        ev = Event.model_validate_json(value)
    except Exception:
        return None
    ev.ingested_at = now or datetime.now(timezone.utc)
    return ev


def observe_normalized(ev: Event) -> None:
    """Record metrics for one validated, ingested-stamped event."""
    NORMALIZED_EVENTS.inc()
    if ev.ingested_at is not None:
        INGEST_LAG.observe((ev.ingested_at - ev.ts).total_seconds())


def run(
    brokers: str,
    group: str = "normalizer",
    max_messages: Optional[int] = None,
    raw_topic: str = RAW_TOPIC,
    normalized_topic: str = NORMALIZED_TOPIC,
    deadletter_topic: str = DEADLETTER_TOPIC,
    metrics_port: int = 0,
    dsn: Optional[str] = None,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.db import connect
    from freshet.common.kafka_io import consume_loop, make_producer

    conn = connect(dsn)
    producer = make_producer(brokers)
    skipped = 0

    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            DEADLETTER_EVENTS.inc()
            producer.produce(deadletter_topic, value=build_deadletter("validation failed", value, raw_topic))
            producer.flush()
            print(f"[normalizer] dead-lettered invalid payload ({skipped} so far)")
            return
        assigned = correlate(conn, ev)
        if assigned is not None:
            ev.incident_id = assigned
        # key by service to preserve per-service ordering downstream
        producer.produce(normalized_topic, key=ev.service, value=ev.model_dump_json())
        # flush before the loop commits this offset: closes the documented
        # produce-before-commit loss window (per-message flush is fine at demo rate)
        producer.flush()
        observe_normalized(ev)

    try:
        n = consume_loop(brokers, group, [raw_topic], handle, max_messages, auto_commit=False)
    finally:
        producer.flush()
        conn.close()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet normalizer (raw.events -> normalized.events)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="normalizer")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--metrics-port", type=int, default=8001, help="Prometheus /metrics port (0 disables)")
    p.add_argument("--dsn", default=None)
    a = p.parse_args()
    n = run(a.brokers, group=a.group, max_messages=a.max, metrics_port=a.metrics_port, dsn=a.dsn)
    print(f"[normalizer] processed {n} messages")


if __name__ == "__main__":
    main()
