"""M4 hardening tests against the real stack: dead-lettering, incident
correlation, and chunked replay idempotency. Run via: make test-integration."""

import json
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

BROKERS = os.environ.get("FRESHET_BROKERS", "localhost:9092")


@pytest.fixture
def conn():
    from freshet.common.db import connect

    c = connect()
    c.execute("DELETE FROM vector_records")
    c.execute("DELETE FROM incidents")
    yield c
    c.close()


def _drain(topic: str, group: str, n: int) -> list[str]:
    from freshet.common.kafka_io import consume_loop

    out: list[str] = []
    consume_loop(BROKERS, group, [topic], out.append, max_messages=n, idle_timeout_s=15)
    return out


def test_poison_message_is_dead_lettered(conn):
    from freshet.common.kafka_io import make_producer
    from freshet.generator.generator import EventGenerator, live_stream
    from freshet.pipeline import normalizer

    run_id = uuid.uuid4().hex[:8]
    raw, norm, dl = (f"{t}.it{run_id}" for t in ("raw.events", "normalized.events", "deadletter.events"))

    producer = make_producer(BROKERS)
    events = list(live_stream(EventGenerator(seed=5, incident_after=0), count=2, spacing_s=0))
    producer.produce(raw, value=events[0].model_dump_json())
    producer.produce(raw, value=b"this is not json")
    for ev in events[1:]:
        producer.produce(raw, value=ev.model_dump_json())
    producer.flush()
    total = len(events) + 1

    n = normalizer.run(
        BROKERS, group=f"n-{run_id}", max_messages=total,
        raw_topic=raw, normalized_topic=norm, deadletter_topic=dl,
    )
    assert n == total

    dead = _drain(dl, f"dl-{run_id}", 1)
    assert len(dead) == 1
    envelope = json.loads(dead[0])
    assert envelope["payload"] == "this is not json"
    assert envelope["source_topic"] == raw
    assert "error" in envelope

    normalized = _drain(norm, f"nv-{run_id}", len(events))
    assert len(normalized) == len(events)


def test_scenario_incident_is_correlated_and_resolved(conn):
    from freshet.generator.generator import EventGenerator, KafkaSink, live_stream
    from freshet.pipeline import normalizer

    run_id = uuid.uuid4().hex[:8]
    raw, norm = f"raw.events.ic{run_id}", f"normalized.events.ic{run_id}"

    sink = KafkaSink(BROKERS, raw)
    produced = 0
    for ev in live_stream(EventGenerator(seed=7, incident_after=0), count=1, spacing_s=0):
        sink.write(ev)
        produced += 1
    sink.close()
    assert produced == 10  # 1 noise + 9 scripted incident events

    normalizer.run(BROKERS, group=f"n-{run_id}", max_messages=10, raw_topic=raw, normalized_topic=norm)

    row = conn.execute(
        "SELECT services, event_ids, resolved_at, resolution_summary FROM incidents"
        " WHERE incident_id = 'INC-DEMO-0001'"
    ).fetchone()
    assert row is not None
    services, event_ids, resolved_at, summary = row
    assert services == ["scheduler-api"]
    assert len(event_ids) == 9
    assert resolved_at is not None
    assert summary is not None and summary.startswith("Postmortem")

    # idempotency: a fresh group replays everything; state must not change
    normalizer.run(BROKERS, group=f"n2-{run_id}", max_messages=10, raw_topic=raw, normalized_topic=norm)
    event_ids2 = conn.execute(
        "SELECT event_ids FROM incidents WHERE incident_id = 'INC-DEMO-0001'"
    ).fetchone()[0]
    assert len(event_ids2) == 9


def test_long_text_chunks_and_replay_is_idempotent(conn):
    from freshet.common.kafka_io import make_producer
    from freshet.common.schemas import Event, EventSource
    from freshet.pipeline import embedder
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    norm = f"normalized.events.ch{run_id}"

    long_text = " ".join(f"finding{i}" for i in range(300))
    ev = Event(service="scheduler-api", source=EventSource.POSTMORTEM, type="rca", text=long_text)
    producer = make_producer(BROKERS)
    producer.produce(norm, value=ev.model_dump_json())
    producer.flush()

    embedder.run(BROKERS, group=f"e-{run_id}", max_messages=1, topic=norm, embedder=StubEmbedder())
    chunks = conn.execute(
        "SELECT count(*) FROM vector_records WHERE event_id = %s", (ev.event_id,)
    ).fetchone()[0]
    assert chunks > 1

    # replay with a fresh group (the make replay path): row count unchanged
    embedder.run(BROKERS, group=f"e2-{run_id}", max_messages=1, topic=norm, embedder=StubEmbedder())
    assert conn.execute(
        "SELECT count(*) FROM vector_records WHERE event_id = %s", (ev.event_id,)
    ).fetchone()[0] == chunks
