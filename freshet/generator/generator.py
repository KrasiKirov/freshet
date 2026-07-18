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
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from freshet.common.schemas import Event, EventSource, EventType
from freshet.generator.scenarios import ARCHETYPES, build_runbooks, build_scenario

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


def build_corpus_events(seed: int = 1, n_incidents: int = 5, noise_between: int = 8,
                        start: datetime | None = None, spacing_s: float = 5.0) -> list[Event]:
    """A richer, seeded corpus: one runbook per service, then N incident arcs on
    rotating services, each preceded by background noise. Deterministic under seed;
    event ids minted from the seeded RNG so the whole corpus is byte-reproducible."""
    rng = random.Random(seed)
    start = start or datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC)

    def mint() -> str:
        return f"evt_{rng.getrandbits(48):012x}"

    events: list[Event] = []
    for rb in build_runbooks(start, SERVICES):
        rb.event_id = mint()
        events.append(rb)

    t = start
    for i in range(n_incidents):
        for _ in range(noise_between):
            ev = _noise_event(rng, t)
            ev.event_id = mint()
            events.append(ev)
            t += timedelta(seconds=spacing_s)
        service = SERVICES[i % len(SERVICES)]
        for ev in build_scenario(t, f"INC-{i + 1:04d}", service=service):
            ev.event_id = mint()
            events.append(ev)
        t += timedelta(seconds=3700)   # clear the +3600s postmortem before the next arc
    return events


def incident_ground_truth(events: list[Event]) -> dict[str, tuple[str, str]]:
    """Per incident: (causing-deploy event_id, rollback event_id). Derived from the
    arcs we authored, so the completeness eval needs no manual labels."""
    gt: dict[str, list[str | None]] = {}
    for e in events:
        if not e.incident_id:
            continue
        slot = gt.setdefault(e.incident_id, [None, None])
        if e.type == EventType.DEPLOY_STARTED:
            slot[0] = e.event_id
        elif e.type == EventType.ROLLBACK:
            slot[1] = e.event_id
    return {iid: (c, f) for iid, (c, f) in gt.items() if c and f}


@dataclass
class IncidentTruth:
    incident_id: str
    service: str
    archetype: str
    cause_id: str
    fix_id: str
    spike_id: str


def build_benchmark(seed: int = 1, n_incidents: int = 40, noise_between: int = 6,
                    start: datetime | None = None, spacing_s: float = 5.0
                    ) -> tuple[list[Event], list[IncidentTruth]]:
    """A varied, benchmark-scale corpus: one runbook per service, then N incidents
    rotating across the archetype registry and services, each preceded by noise.
    Deterministic under seed; records per-incident cause/fix/spike ids as it builds."""
    rng = random.Random(seed)
    start = start or datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC)

    def mint() -> str:
        return f"evt_{rng.getrandbits(48):012x}"

    events: list[Event] = []
    for rb in build_runbooks(start, SERVICES):
        rb.event_id = mint()
        events.append(rb)

    truths: list[IncidentTruth] = []
    t = start
    for i in range(n_incidents):
        for _ in range(noise_between):
            ev = _noise_event(rng, t)
            ev.event_id = mint()
            events.append(ev)
            t += timedelta(seconds=spacing_s)

        archetype = ARCHETYPES[i % len(ARCHETYPES)]
        # unique service per incident so a service-scoped root-cause query targets
        # exactly one incident (the completeness eval is incident-specific); the
        # whole-corpus retrieval eval still searches across all of them.
        service = f"{SERVICES[i % len(SERVICES)]}-{i:02d}"
        incident_id = f"INC-{i + 1:04d}"
        cause_id = fix_id = spike_id = None
        for step in archetype.steps:
            ev = Event(
                ts=t + timedelta(seconds=step.offset_s),
                incident_id=incident_id,
                service=service,
                source=step.source,
                type=step.type,
                severity=step.severity,
                text=step.text.format(service=service),
            )
            ev.event_id = mint()
            events.append(ev)
            if step.role == "change":
                cause_id = ev.event_id
            elif step.role == "remediation":
                fix_id = ev.event_id
            elif step.role == "spike":
                spike_id = ev.event_id
        # every archetype defines a change, remediation, and spike step, so all
        # three are set by the loop above (asserted so the invariant is explicit)
        assert cause_id and fix_id and spike_id
        truths.append(IncidentTruth(incident_id, service, archetype.name,
                                    cause_id, fix_id, spike_id))
        t += timedelta(seconds=3700)
    return events, truths


def build_hard_benchmark(seed: int = 1, n_incidents: int = 40, n_volume: int = 10,
                         start: datetime | None = None
                         ) -> tuple[list[Event], list[IncidentTruth]]:
    """The `hard` benchmark tier: like build_benchmark but each incident carries
    decoy causes (benign same-service changes for retrieval volume, plus a benign
    change interposed between the true cause and the spike). Ground truth records the
    BAD change as cause_id. Deterministic under seed. Shared build_benchmark is
    untouched — this is a separate tier, so only the rootcause eval moves."""
    from freshet.generator.scenarios import hard_incident_events

    rng = random.Random(seed)
    start = start or datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC)

    def mint() -> str:
        return f"evt_{rng.getrandbits(48):012x}"

    events: list[Event] = []
    for rb in build_runbooks(start, SERVICES):
        rb.event_id = mint()
        events.append(rb)

    truths: list[IncidentTruth] = []
    t = start
    for i in range(n_incidents):
        for _ in range(6):
            ev = _noise_event(rng, t)
            ev.event_id = mint()
            events.append(ev)
            t += timedelta(seconds=5.0)
        archetype = ARCHETYPES[i % len(ARCHETYPES)]
        service = f"{SERVICES[i % len(SERVICES)]}-{i:02d}"
        incident_id = f"INC-{i + 1:04d}"
        inc_events, cause_id, fix_id, spike_id = hard_incident_events(
            archetype, service, t + timedelta(seconds=600), incident_id, mint, n_volume)
        events.extend(inc_events)
        truths.append(IncidentTruth(incident_id, service, archetype.name,
                                    cause_id, fix_id, spike_id))
        t += timedelta(seconds=4300)   # clear volume(-600s..) + postmortem(+3600s)
    return events, truths


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
        self.start = start or datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC)
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


def live_stream(gen: EventGenerator, count: int, spacing_s: float) -> Iterator[Event]:
    """Re-stamp ts to wall-clock now and pace emission.

    The default stream uses fixed historical timestamps for reproducibility;
    freshness (indexed_at - ts) is only meaningful when ts is real time, so
    demos and the slice run use this wrapper.
    """
    for ev in gen.stream(count):
        ev.ts = datetime.now(UTC)
        yield ev
        if spacing_s > 0:
            time.sleep(spacing_s)


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
class JsonlSink:
    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "w")  # noqa: SIM115 — handle owned for the sink's lifetime; close() below

    def write(self, event: Event) -> None:
        self._fh.write(event.model_dump_json() + "\n")

    def close(self) -> None:
        self._fh.close()


class KafkaSink:
    """Lazy Kafka producer sink. Requires confluent-kafka + a running broker."""

    def __init__(self, brokers: str, topic: str):
        from freshet.common.kafka_io import make_producer  # lazy import

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
    p.add_argument("--live", action="store_true", help="stamp ts=now and pace emission (for freshness demos)")
    p.add_argument("--live-spacing", type=float, default=0.2, help="seconds between events in --live mode")
    args = p.parse_args()

    gen = EventGenerator(seed=args.seed)
    sink = (
        JsonlSink(args.out)
        if args.sink == "jsonl"
        else KafkaSink(args.brokers, args.topic)
    )
    stream = live_stream(gen, args.count, args.live_spacing) if args.live else gen.stream(args.count)
    n = 0
    try:
        for ev in stream:
            sink.write(ev)
            n += 1
    finally:
        sink.close()
    print(f"wrote {n} events via {args.sink} sink")


if __name__ == "__main__":
    main()
