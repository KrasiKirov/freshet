"""Normalizer worker: raw.events -> validate -> stamp ingested_at -> normalized.events.

M2 scope: validation + timestamping only. Incident correlation and the
dead-letter topic arrive in M4 — until then invalid payloads are skipped with
a warning, never silently dropped without trace. Also deferred to M4: the
produce happens async and the consumer offset commits before the producer
flushes, so a crash in that window can lose (not duplicate) an event; the
recovery path until then is topic replay from offset 0.

Run (stack up first):
    python -m freshet.pipeline.normalizer --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from freshet.common.schemas import Event
from freshet.pipeline.metrics import (
    INGEST_LAG,
    INVALID_EVENTS,
    NORMALIZED_EVENTS,
    start_metrics_server,
)

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
    metrics_port: int = 0,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.kafka_io import consume_loop, make_producer

    producer = make_producer(brokers)
    skipped = 0

    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            INVALID_EVENTS.inc()
            print(f"[normalizer] skipped invalid payload ({skipped} so far)")
            return
        # key by service to preserve per-service ordering downstream
        producer.produce(normalized_topic, key=ev.service, value=ev.model_dump_json())
        producer.poll(0)
        observe_normalized(ev)

    n = consume_loop(brokers, group, [raw_topic], handle, max_messages, auto_commit=False)
    producer.flush()
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet normalizer (raw.events -> normalized.events)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="normalizer")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--metrics-port", type=int, default=8001, help="Prometheus /metrics port (0 disables)")
    a = p.parse_args()
    n = run(a.brokers, group=a.group, max_messages=a.max, metrics_port=a.metrics_port)
    print(f"[normalizer] processed {n} messages")


if __name__ == "__main__":
    main()
