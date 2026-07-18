"""Normalizer worker: raw.events -> validate -> correlate -> normalized.events.

Validates each payload against the canonical Event schema, stamps ingested_at,
attaches the event to an incident (state in Postgres), and republishes keyed
by service. Invalid payloads go to the dead-letter topic, never silently
dropped. Produces are batched (BufferedProducer) and flush-checked before the
consumer offsets commit (consume_loop's pre_commit hook), so a crash or failed
produce can only cause redelivery of the current batch (at-least-once), never
loss — downstream idempotent upserts absorb the duplicates. `--commit-every`
sets the batch size; the old flush-per-event behavior (the measured ~100 ev/s
ceiling) is `--commit-every 1`.

Run (stack up first):
    python -m freshet.pipeline.normalizer --brokers localhost:9092
"""

from __future__ import annotations

import argparse
import signal
import threading
from datetime import UTC, datetime

from freshet.common.schemas import Event
from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter
from freshet.pipeline.incidents import correlate
from freshet.pipeline.metrics import (
    DEADLETTER_EVENTS,
    INGEST_LAG,
    NORMALIZED_EVENTS,
    start_metrics_server,
)

RAW_TOPIC = "raw.events"
NORMALIZED_TOPIC = "normalized.events"


def normalize(value: str, now: datetime | None = None) -> Event | None:
    """Parse and validate one raw payload; stamp ingested_at. None if invalid."""
    try:
        ev = Event.model_validate_json(value)
    except Exception:
        return None
    ev.ingested_at = now or datetime.now(UTC)
    return ev


def observe_normalized(ev: Event) -> None:
    """Record metrics for one validated, ingested-stamped event."""
    NORMALIZED_EVENTS.inc()
    if ev.ingested_at is not None:
        INGEST_LAG.observe((ev.ingested_at - ev.ts).total_seconds())


def run(
    brokers: str,
    group: str = "normalizer",
    max_messages: int | None = None,
    raw_topic: str = RAW_TOPIC,
    normalized_topic: str = NORMALIZED_TOPIC,
    deadletter_topic: str = DEADLETTER_TOPIC,
    metrics_port: int = 0,
    dsn: str | None = None,
    stop: threading.Event | None = None,
    commit_every: int = 1,
) -> int:
    start_metrics_server(metrics_port)
    from freshet.common.db import connect
    from freshet.common.kafka_io import BufferedProducer, consume_loop
    from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC, LifecycleEvent

    conn = connect(dsn)
    producer = BufferedProducer(brokers)
    skipped = 0

    def handle(value: str) -> None:
        nonlocal skipped
        ev = normalize(value)
        if ev is None:
            skipped += 1
            producer.produce(deadletter_topic, build_deadletter("validation failed", value, raw_topic))
            DEADLETTER_EVENTS.inc()
            print(f"[normalizer] dead-lettered invalid payload ({skipped} so far)")
            return
        result = correlate(conn, ev)
        assigned = result.incident_id
        if assigned is not None:
            ev.incident_id = assigned
        if result.transition is not None:
            # a transition (opened/resolved) always carries the incident it applies to
            assert result.incident_id is not None
            life = LifecycleEvent(
                type=result.transition,
                incident_id=result.incident_id,
                service=ev.service,
                ts=ev.ts.isoformat(),
            )
            producer.produce(LIFECYCLE_TOPIC, life.to_json(), key=ev.service)
        # key by service to preserve per-service ordering downstream. Produces
        # are buffered; consume_loop's pre_commit flush-checks the whole batch
        # before offsets commit, so a failed produce can never be committed past.
        producer.produce(normalized_topic, ev.model_dump_json(), key=ev.service)
        observe_normalized(ev)

    try:
        n = consume_loop(brokers, group, [raw_topic], handle, max_messages,
                         auto_commit=False, stop=stop,
                         commit_every=commit_every,
                         pre_commit=producer.flush_checked)
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
    p.add_argument("--commit-every", type=int, default=100,
                   help="commit offsets every N messages (batched produce+commit; 1 = legacy per-message)")
    a = p.parse_args()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    n = run(a.brokers, group=a.group, max_messages=a.max, metrics_port=a.metrics_port, dsn=a.dsn, stop=stop,
            commit_every=a.commit_every)
    print(f"[normalizer] processed {n} messages")


if __name__ == "__main__":
    main()
