"""Thin Kafka helpers. Isolated here so the rest of the codebase (and the tests)
don't import a Kafka client unless they actually talk to a broker.

Uses confluent-kafka, the standard Kafka client. The broker is provided by
docker-compose (Redpanda, which speaks the Kafka protocol). Delivery is
at-least-once; downstream upserts must be idempotent (keyed on chunk_id).
"""

from __future__ import annotations

from typing import Callable, Optional


def make_producer(brokers: str):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": brokers, "linger.ms": 5})


def make_consumer(brokers: str, group_id: str, topics: list[str]):
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe(topics)
    return c


def consume_loop(
    brokers: str,
    group_id: str,
    topics: list[str],
    handler: Callable[[str], None],
    max_messages: Optional[int] = None,
) -> int:
    """Run a simple consume loop, calling handler(value_str) per message.

    Returns the number of messages processed. `max_messages` lets callers/tests
    bound the loop; None runs until interrupted.
    """
    c = make_consumer(brokers, group_id, topics)
    n = 0
    try:
        while max_messages is None or n < max_messages:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # in real code: route to dead-letter; here we just print
                print(f"[consume error] {msg.error()}")
                continue
            handler(msg.value().decode("utf-8"))
            n += 1
    finally:
        c.close()
    return n
