from datetime import datetime, timedelta, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import records_for_event


def test_records_have_deterministic_chunk_ids():
    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="5xx spike", incident_id="INC-1")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    [a] = records_for_event(ev, now=now)
    [b] = records_for_event(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_long_text_yields_multiple_records():
    text = " ".join(f"word{i}" for i in range(300))
    ev = Event(service="s", source=EventSource.POSTMORTEM, type="rca", text=text)
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    records = records_for_event(ev, now=now)
    assert len(records) > 1
    assert [r.chunk_id for r in records] == [f"chk_{ev.event_id}_{i}" for i in range(len(records))]
    assert all(r.indexed_at == now for r in records)
    assert " ".join(r.text for r in records) == text


def test_records_copy_fields_and_blank_text_is_empty():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)
    [rec] = records_for_event(ev, now=now)
    assert rec.event_id == ev.event_id
    assert rec.service == "s"
    assert rec.ts == ev.ts
    assert rec.indexed_at == now
    assert rec.text == "hello"
    assert rec.source is EventSource.CHAT
    assert rec.incident_id is None
    assert records_for_event(Event(service="s", source=EventSource.CHAT, type="message", text="  "), now=now) == []


def test_observe_indexed_records_freshness():
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    now = ev.ts + timedelta(seconds=2.5)
    [rec] = records_for_event(ev, now=now)

    events_before = REGISTRY.get_sample_value("freshet_embedder_events_total") or 0
    sum_before = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") or 0

    observe_indexed(rec)

    assert REGISTRY.get_sample_value("freshet_embedder_events_total") == events_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_freshness_seconds_sum") - sum_before - 2.5) < 1e-6
