from datetime import datetime, timedelta, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import to_vector_record


def test_to_vector_record_has_deterministic_chunk_id():
    ev = Event(
        service="scheduler-api",
        source=EventSource.ALERT,
        type="error_spike",
        text="5xx spike",
        incident_id="INC-1",
    )
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    a = to_vector_record(ev, now=now)
    b = to_vector_record(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_to_vector_record_copies_fields_and_stamps_indexed_at():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    rec = to_vector_record(ev, now=now)
    assert rec.event_id == ev.event_id
    assert rec.service == "s"
    assert rec.ts == ev.ts
    assert rec.indexed_at == now
    assert rec.text == "hello"
    assert rec.source is EventSource.CHAT
    assert rec.incident_id is None


def test_observe_indexed_records_freshness():
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    now = ev.ts + timedelta(seconds=2.5)
    rec = to_vector_record(ev, now=now)

    events_before = REGISTRY.get_sample_value("freshet_embedder_events_total") or 0
    sum_before = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") or 0

    observe_indexed(rec)

    assert REGISTRY.get_sample_value("freshet_embedder_events_total") == events_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_freshness_seconds_sum") - sum_before - 2.5) < 1e-6
