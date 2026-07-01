"""Thin Kafka helpers. Isolated here so the rest of the codebase (and the tests)
don't import a Kafka client unless they actually talk to a broker.

Uses confluent-kafka, the standard Kafka client. The broker is provided by
docker-compose (Redpanda, which speaks the Kafka protocol). Delivery is
at-least-once; downstream upserts must be idempotent (keyed on chunk_id).
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional


def make_producer(brokers: str):
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": brokers, "linger.ms": 5})


def produce_sync(producer, topic: str, value, key: Optional[str] = None) -> None:
    """Produce one message and wait for its delivery report, raising on
    failure — so a caller that commits offsets afterwards can never silently
    lose a message to a failed produce."""
    errors: list = []

    def _cb(err, msg):
        if err is not None:
            errors.append(err)

    producer.produce(topic, key=key, value=value, on_delivery=_cb)
    producer.flush()
    if errors:
        raise RuntimeError(f"produce to {topic} failed: {errors[0]}")


def make_consumer(brokers: str, group_id: str, topics: list[str], auto_commit: bool = True):
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": auto_commit,
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
    auto_commit: bool = True,
    stop: Optional[threading.Event] = None,
    idle_timeout_s: Optional[float] = None,
) -> int:
    """Run a simple consume loop, calling handler(value_str) per message.

    With auto_commit=False the offset is committed synchronously after the
    handler returns, so an unprocessed message is redelivered after a crash
    (at-least-once). `stop` lets a signal handler end the loop cleanly — the
    consumer then leaves its group on close(), so a restart is not stalled by
    a session timeout. `idle_timeout_s` ends the loop after that many seconds
    without a message (used by replay). Returns the number of messages
    processed; `max_messages` bounds the loop for tests.
    """
    c = make_consumer(brokers, group_id, topics, auto_commit=auto_commit)
    n = 0
    last_msg = time.monotonic()
    try:
        while max_messages is None or n < max_messages:
            if stop is not None and stop.is_set():
                break
            msg = c.poll(1.0)
            if msg is None:
                if idle_timeout_s is not None and time.monotonic() - last_msg >= idle_timeout_s:
                    break
                continue
            if msg.error():
                # consumer events (not records): nothing to commit or dead-letter
                print(f"[consume error] {msg.error()}")
                continue
            last_msg = time.monotonic()
            handler(msg.value().decode("utf-8"))
            if not auto_commit:
                c.commit(message=msg, asynchronous=False)
            n += 1
    finally:
        c.close()
    return n
