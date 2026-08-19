"""Republish dead-lettered messages to the topic they came from.

The pipeline dead-letters a message it cannot parse or embed, which is the right
call — one poison record must not stall the stream. But nothing ever read the
queue back, so a message that failed during a transient outage stayed lost.

Each record carries its own `source_topic`, so replay needs no configuration.
Run after fixing whatever caused the failure:

    python -m freshet.pipeline.replay_deadletter --max 100
"""

from __future__ import annotations

import argparse
import json
import logging

from freshet.pipeline.embedder import DEADLETTER_TOPIC

log = logging.getLogger(__name__)


def replay_record(producer, raw: str) -> str | None:
    """Republish one dead-letter record. Returns the topic, or None if unusable."""
    try:
        record = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("dead-letter record is not JSON (%s); leaving it", exc)
        return None
    topic, payload = record.get("source_topic"), record.get("payload")
    if not topic or payload is None:
        log.warning("dead-letter record has no source_topic/payload; leaving it")
        return None
    producer.produce(topic, payload)
    return topic


def main() -> None:
    from freshet.common.kafka_io import BufferedProducer, consume_loop

    p = argparse.ArgumentParser(description="Replay dead-lettered messages")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default="deadletter-replay")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--idle-timeout", type=float, default=10.0)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    producer = BufferedProducer(args.brokers)
    replayed: dict[str, int] = {}

    def handle(raw: str) -> None:
        topic = replay_record(producer, raw)
        if topic:
            replayed[topic] = replayed.get(topic, 0) + 1

    # Offsets commit only after a successful flush, so a crash mid-replay
    # redelivers rather than silently dropping the record a second time.
    n = consume_loop(args.brokers, args.group, [DEADLETTER_TOPIC], handle,
                     max_messages=args.max, auto_commit=False,
                     idle_timeout_s=args.idle_timeout,
                     pre_commit=producer.flush_checked)
    producer.flush_checked()
    print(f"[replay] read {n} dead-letter records; republished {sum(replayed.values())} "
          f"{dict(replayed) or ''}")


if __name__ == "__main__":
    main()
