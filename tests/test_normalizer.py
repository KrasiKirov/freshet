from datetime import datetime, timedelta, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.normalizer import normalize


def test_normalize_stamps_ingested_at_and_preserves_event():
    ev = Event(service="scheduler-api", source=EventSource.ALERT, type="error_spike", text="boom")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    out = normalize(ev.model_dump_json(), now=now)
    assert out is not None
    assert out.ingested_at == now
    assert out.event_id == ev.event_id
    assert out.ts == ev.ts
    assert out.text == "boom"


def test_normalize_rejects_invalid_payloads():
    assert normalize("not json at all") is None
    assert normalize('{"service": "s"}') is None  # missing required fields


def test_observe_normalized_records_metrics():
    from prometheus_client import REGISTRY

    from freshet.pipeline.normalizer import observe_normalized

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    ev.ingested_at = ev.ts + timedelta(seconds=0.7)

    events_before = REGISTRY.get_sample_value("freshet_normalizer_events_total") or 0
    lag_count_before = REGISTRY.get_sample_value("freshet_ingest_lag_seconds_count") or 0
    lag_sum_before = REGISTRY.get_sample_value("freshet_ingest_lag_seconds_sum") or 0

    observe_normalized(ev)

    assert REGISTRY.get_sample_value("freshet_normalizer_events_total") == events_before + 1
    assert REGISTRY.get_sample_value("freshet_ingest_lag_seconds_count") == lag_count_before + 1
    assert abs(REGISTRY.get_sample_value("freshet_ingest_lag_seconds_sum") - lag_sum_before - 0.7) < 1e-6
