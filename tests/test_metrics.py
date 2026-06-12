from prometheus_client import REGISTRY

from freshet.pipeline.metrics import (
    FRESHNESS,
    INDEXED_EVENTS,
    INGEST_LAG,
    INVALID_EVENTS,
    NORMALIZED_EVENTS,
    start_metrics_server,
)


def _value(name: str, labels=None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def test_counters_increment():
    before = _value("freshet_normalizer_events_total")
    NORMALIZED_EVENTS.inc()
    assert _value("freshet_normalizer_events_total") == before + 1

    before = _value("freshet_normalizer_invalid_total")
    INVALID_EVENTS.inc()
    assert _value("freshet_normalizer_invalid_total") == before + 1

    before = _value("freshet_embedder_events_total")
    INDEXED_EVENTS.inc()
    assert _value("freshet_embedder_events_total") == before + 1


def test_histograms_observe_into_buckets():
    before = _value("freshet_freshness_seconds_bucket", {"le": "5.0"})
    FRESHNESS.observe(2.5)
    assert _value("freshet_freshness_seconds_bucket", {"le": "5.0"}) == before + 1

    before = _value("freshet_ingest_lag_seconds_count")
    INGEST_LAG.observe(0.3)
    assert _value("freshet_ingest_lag_seconds_count") == before + 1


def test_metrics_server_port_zero_is_disabled():
    # must be a no-op, not an error — unit tests and library callers use 0
    start_metrics_server(0)
