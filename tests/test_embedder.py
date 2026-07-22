from datetime import UTC, datetime, timedelta

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import records_for_event


def test_records_have_deterministic_chunk_ids():
    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="5xx spike", incident_id="INC-1")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
    [a] = records_for_event(ev, now=now)
    [b] = records_for_event(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_long_text_yields_multiple_records():
    text = " ".join(f"word{i}" for i in range(300))
    ev = Event(service="s", source=EventSource.POSTMORTEM, type="rca", text=text)
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
    records = records_for_event(ev, now=now)
    assert len(records) > 1
    assert [r.chunk_id for r in records] == [f"chk_{ev.event_id}_{i}" for i in range(len(records))]
    assert all(r.indexed_at == now for r in records)
    assert " ".join(r.text for r in records) == text


def test_records_copy_fields_and_blank_text_is_empty():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
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


def test_observe_indexed_records_pipeline_latency_separately():
    """Pipeline latency (ingested -> indexed) must be measured independently of
    end-to-end freshness (ts -> indexed). On replayed or status-feed data `ts` is
    days old, so only pipeline latency reflects how fast the pipeline actually is."""
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    # an event that HAPPENED 3 days ago but was received 1.5s before indexing
    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    ev.ts = ev.ts - timedelta(days=3)
    indexed = ev.ts + timedelta(days=3)
    ev.ingested_at = indexed - timedelta(seconds=1.5)
    [rec] = records_for_event(ev, now=indexed)

    lat_before = REGISTRY.get_sample_value("freshet_pipeline_latency_seconds_sum") or 0
    fresh_before = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") or 0

    observe_indexed(rec, ingested_at=ev.ingested_at)

    lat = REGISTRY.get_sample_value("freshet_pipeline_latency_seconds_sum") - lat_before
    fresh = REGISTRY.get_sample_value("freshet_freshness_seconds_sum") - fresh_before
    assert abs(lat - 1.5) < 1e-6                    # the pipeline took 1.5s
    assert abs(fresh - 3 * 86400) < 1e-6            # the news was 3 days old


def test_observe_indexed_skips_latency_without_ingested_at():
    from prometheus_client import REGISTRY

    from freshet.pipeline.embedder import observe_indexed

    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="x")
    [rec] = records_for_event(ev, now=ev.ts + timedelta(seconds=1))
    before = REGISTRY.get_sample_value("freshet_pipeline_latency_seconds_count") or 0
    observe_indexed(rec)  # no ingested_at available
    assert REGISTRY.get_sample_value("freshet_pipeline_latency_seconds_count") == before


class _FakeProducer:
    """Collects (topic, value) pairs; compatible with produce_sync."""

    def __init__(self):
        self.messages = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.messages.append((topic, value))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


class _FakeConn:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class _FlakyEmbedder:
    """Fails the first n encode calls, then behaves like the stub."""

    def __init__(self, failures):
        self.failures = failures
        self.calls = 0

    def encode(self, texts):
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("model exploded")
        from freshet.pipeline.embedding import StubEmbedder
        return StubEmbedder().encode(texts)


def _event_json():
    ev = Event(service="s", source=EventSource.ALERT, type="error_spike", text="5xx spike")
    return ev.model_dump_json()


def test_poison_message_dead_letters_after_retries():
    from freshet.pipeline.embedder import make_handler

    producer, conn = _FakeProducer(), _FakeConn()
    naps = []
    handle = make_handler(conn, _FlakyEmbedder(failures=99), producer,
                          attempts=3, sleep=naps.append)
    handle(_event_json())  # must not raise: the message dead-letters instead
    assert len(producer.messages) == 1
    topic, value = producer.messages[0]
    assert topic == "deadletter.events" and "3 attempts" in value
    assert conn.executed == []          # nothing indexed
    assert len(naps) == 2               # slept between attempts, not after the last


def test_transient_embed_failure_recovers_without_deadletter():
    from freshet.pipeline.embedder import make_handler

    producer, conn = _FakeProducer(), _FakeConn()
    handle = make_handler(conn, _FlakyEmbedder(failures=1), producer,
                          attempts=3, sleep=lambda s: None)
    handle(_event_json())
    assert producer.messages == []      # no dead-letter
    assert len(conn.executed) == 1      # the chunk was upserted


def test_db_failure_still_propagates():
    """Upsert failures are infrastructure, not message poison: dead-lettering
    them during a DB outage would drain the stream into the DLQ."""
    import pytest

    from freshet.pipeline.embedder import make_handler

    class _BrokenConn(_FakeConn):
        def execute(self, sql, params=None):
            raise RuntimeError("db down")

    producer = _FakeProducer()
    handle = make_handler(_BrokenConn(), _FlakyEmbedder(failures=0), producer,
                          attempts=3, sleep=lambda s: None)
    with pytest.raises(RuntimeError, match="db down"):
        handle(_event_json())
    assert producer.messages == []
