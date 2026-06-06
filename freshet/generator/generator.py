"""Synthetic operational-event generator.

Produces a stream of background "noise" events across several services, with the
scripted incident (see scenarios.py) injected at a chosen point. Deterministic
under a fixed seed so tests and demos are reproducible.

Two sinks:
  - JsonlSink: writes events to a .jsonl file. No broker needed -> used in tests
    and as a replay file.
  - KafkaSink: publishes to a Kafka topic (imported lazily so tests don't need
    a Kafka client installed).
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from common.schemas import Event, EventSource, EventType, Severity
from generator.scenarios import build_scenario

SERVICES = [
    "scheduler-api",
    "task-queue",
    "auth-service",
    "billing-api",
    "notification-worker",
]

_NOISE_TEMPLATES = [
    (EventSource.METRIC, EventType.METRIC_SAMPLE, "cpu {v}% on {svc}"),
    (EventSource.METRIC, EventType.METRIC_SAMPLE, "p99 latency {v}ms on {svc}"),
    (EventSource.DEPLOY, EventType.DEPLOY_FINISHED, "routine deploy of {svc} finished"),
    (EventSource.CHAT, EventType.MESSAGE, "standup note about {svc} from on-call"),
    (EventSource.ALERT, EventType.HEALTHY, "{svc} health check passing"),
    (EventSource.METRIC, EventType.SCALE, "{svc} autoscaled to {v} replicas"),
]


def _noise_event(rng: random.Random, ts: datetime) -> Event:
    svc = rng.choice(SERVICES)
    source, etype, tmpl = rng.choice(_NOISE_TEMPLATES)
    v = rng.randint(2, 95)
    return Event(
        ts=ts,
        service=svc,
        source=source,
        type=etype,
        text=tmpl.format(svc=svc, v=v),
        structured={"synthetic": True, "noise": True},
    )


class EventGenerator:
    def __init__(
        self,
        seed: int = 1,
        start: datetime | None = None,
        spacing_s: float = 5.0,
        incident_after: int = 20,
        incident_id: str = "INC-DEMO-0001",
    ):
        self.rng = random.Random(seed)
        self.start = start or datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)
        self.spacing_s = spacing_s
        self.incident_after = incident_after
        self.incident_id = incident_id

    def _mint_id(self) -> str:
        # deterministic under a fixed seed, unlike the schema's uuid4 default
        return f"evt_{self.rng.getrandbits(48):012x}"

    def stream(self, count: int) -> Iterator[Event]:
        """Yield `count` noise events, injecting the scripted incident inline.

        Total emitted == count (noise) + len(scenario). Event ids are minted
        from the seeded RNG so the entire stream is byte-reproducible.
        """
        scenario_events = build_scenario(
            self.start + timedelta(seconds=self.incident_after * self.spacing_s),
            self.incident_id,
        )
        for sev in scenario_events:
            sev.event_id = self._mint_id()

        emitted = 0
        while emitted < count:
            ts = self.start + timedelta(seconds=emitted * self.spacing_s)
            if emitted == self.incident_after:
                for sev in scenario_events:
                    yield sev
            ev = _noise_event(self.rng, ts)
            ev.event_id = self._mint_id()
            yield ev
            emitted += 1


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
class JsonlSink:
    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "w")

    def write(self, event: Event) -> None:
        self._fh.write(event.model_dump_json() + "\n")

    def close(self) -> None:
        self._fh.close()


class KafkaSink:
    """Lazy Kafka producer sink. Requires confluent-kafka + a running broker."""

    def __init__(self, brokers: str, topic: str):
        from common.kafka_io import make_producer  # lazy import

        self.topic = topic
        self.producer = make_producer(brokers)

    def write(self, event: Event) -> None:
        # key by service to preserve per-service ordering across partitions
        self.producer.produce(
            self.topic, key=event.service, value=event.model_dump_json()
        )
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush()


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet synthetic event generator")
    p.add_argument("--sink", choices=["jsonl", "kafka"], default="jsonl")
    p.add_argument("--out", default="events.jsonl", help="JSONL output path")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--topic", default="raw.events")
    p.add_argument("--count", type=int, default=200)
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args()

    gen = EventGenerator(seed=args.seed)
    sink = (
        JsonlSink(args.out)
        if args.sink == "jsonl"
        else KafkaSink(args.brokers, args.topic)
    )
    n = 0
    try:
        for ev in gen.stream(args.count):
            sink.write(ev)
            n += 1
    finally:
        sink.close()
    print(f"wrote {n} events via {args.sink} sink")


if __name__ == "__main__":
    main()
