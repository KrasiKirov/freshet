"""Phase 0 hello-world consumer.

Consumes raw.events, parses each into the canonical Event schema, stamps
ingested_at, and prints a one-line summary. This proves the end-to-end
produce -> Kafka -> consume -> validate path before any real processing
(normalization, embedding) is added in Phase 1.

Run (with docker-compose up first):
    python -m freshet.pipeline.consumer_helloworld --brokers localhost:9092
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from freshet.common.kafka_io import consume_loop
from freshet.common.schemas import Event


def handle(value: str) -> None:
    ingested_at = datetime.now(timezone.utc)
    ev = Event.model_validate_json(value)
    ev.ingested_at = ingested_at
    inc = f" incident={ev.incident_id}" if ev.incident_id else ""
    print(f"[{ev.source.value:10}] {ev.service:20} {ev.type:16}{inc}  {ev.text[:60]}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--topic", default="raw.events")
    p.add_argument("--group", default="helloworld")
    p.add_argument("--max", type=int, default=None)
    args = p.parse_args()
    n = consume_loop(args.brokers, args.group, [args.topic], handle, args.max)
    print(f"consumed {n} events")


if __name__ == "__main__":
    main()
