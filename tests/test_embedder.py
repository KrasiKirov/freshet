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

    def transaction(self):
        import contextlib
        return contextlib.nullcontext()


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
    assert producer.messages == []
    assert any("INSERT INTO vector_records" in q for q, _ in conn.executed)


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


def test_handler_writes_incident_events_and_drops_orphan_chunks():
    from freshet.common.schemas import Event, EventSource
    from freshet.pipeline.embedder import make_handler

    ev = Event(service="s", source=EventSource.ALERT, type="status_update",
               text="short", incident_id="INC-9")
    producer, conn = _FakeProducer(), _FakeConn()
    make_handler(conn, _FlakyEmbedder(failures=0), producer, attempts=1,
                 sleep=lambda s: None)(ev.model_dump_json())
    sql = " ".join(q for q, _ in conn.executed)
    assert "incident_events" in sql
    assert "DELETE FROM vector_records" in sql


def test_handler_counts_the_kafka_message_once():
    from prometheus_client import REGISTRY

    from freshet.common.schemas import Event, EventSource
    from freshet.pipeline.embedder import make_handler

    msg_before = REGISTRY.get_sample_value("freshet_embedder_messages_total") or 0
    ev = Event(service="s", source=EventSource.ALERT, type="status_update", text="hello")
    make_handler(_FakeConn(), _FlakyEmbedder(failures=0), _FakeProducer(),
                 attempts=1, sleep=lambda s: None)(ev.model_dump_json())
    assert REGISTRY.get_sample_value("freshet_embedder_messages_total") == msg_before + 1


def test_offsets_commit_in_batches_because_every_write_is_idempotent():
    """consume_loop defaults to a synchronous offset commit per message, and the
    embedder never overrode it — that round trip was the throughput ceiling.
    chunk_id derives from event_id, so redelivering a batch overwrites its own
    rows: at-least-once plus idempotent is still effectively once in the index."""
    import inspect

    from freshet.pipeline import embedder

    src = inspect.getsource(embedder.run)
    assert "commit_every=" in src, "run() must set it; the consume_loop default is 1"
    assert embedder.DEFAULT_COMMIT_EVERY >= 10
    assert "pre_commit=" in src, \
        "a batched commit must flush the dead-letter producer first, or an offset " \
        "can commit past an unacknowledged dead-letter"


def test_all_of_one_events_chunks_are_encoded_in_a_single_call():
    """One encode call per MESSAGE meant a batch size of 1-3 — the 'batches -> bge'
    the module promises never happened at the chunk level either."""
    from freshet.pipeline.embedder import make_handler
    from freshet.pipeline.embedding import StubEmbedder

    calls: list[int] = []

    class _Counting(StubEmbedder):
        name = "counting"

        def encode(self, texts):
            calls.append(len(texts))
            return super().encode(texts)

    long_text = ". ".join(f"Sentence number {i} about the outage" for i in range(60))
    ev = Event(service="s", source=EventSource.ALERT, type="status_update",
               text=long_text + ".")
    make_handler(_FakeConn(), _Counting(), _FakeProducer())(ev.model_dump_json())
    assert len(calls) == 1 and calls[0] > 1, "all of one event's chunks in one call"


def test_a_systemic_embed_failure_stops_instead_of_emptying_the_topic():
    """An OOM, missing weights or a bad torch thread setting dead-letters EVERY
    message as fast as the consumer can poll. The upsert path already refuses to
    dead-letter infrastructure failures for exactly this reason; the embed path
    needs the same guard. Crash-looping under supervision is recoverable, a
    drained topic is not."""
    import pytest

    from freshet.pipeline.embedder import (
        MAX_CONSECUTIVE_DEADLETTERS,
        EmbedderUnhealthy,
        make_handler,
    )

    producer, conn = _FakeProducer(), _FakeConn()
    handle = make_handler(conn, _FlakyEmbedder(failures=999), producer,
                          attempts=1, sleep=lambda s: None)
    with pytest.raises(EmbedderUnhealthy):
        for _ in range(MAX_CONSECUTIVE_DEADLETTERS + 5):
            handle(_event_json())
    assert len(producer.messages) == MAX_CONSECUTIVE_DEADLETTERS, \
        "the breaker trips ON the threshold, not after another lap of the topic"


def test_one_success_resets_the_streak():
    """Nine failures, one success, then more failures must not trip the breaker:
    it fires on a RUN of failures, not a lifetime total."""
    from freshet.pipeline.embedder import MAX_CONSECUTIVE_DEADLETTERS, make_handler

    producer, conn = _FakeProducer(), _FakeConn()
    handle = make_handler(conn, _FlakyEmbedder(failures=MAX_CONSECUTIVE_DEADLETTERS - 1),
                          producer, attempts=1, sleep=lambda s: None)
    for _ in range(MAX_CONSECUTIVE_DEADLETTERS - 1):
        handle(_event_json())            # dead-letters; streak climbs to N-1
    handle(_event_json())                # succeeds; streak resets
    assert len(producer.messages) == MAX_CONSECUTIVE_DEADLETTERS - 1


def test_a_malformed_message_is_poison_and_never_trips_the_breaker():
    """A parse failure really is one bad message. Counting it toward the systemic
    streak would let a run of junk records take down a healthy worker."""
    from freshet.pipeline.embedder import MAX_CONSECUTIVE_DEADLETTERS, make_handler

    producer, conn = _FakeProducer(), _FakeConn()
    handle = make_handler(conn, _FlakyEmbedder(failures=0), producer,
                          attempts=1, sleep=lambda s: None)
    for _ in range(MAX_CONSECUTIVE_DEADLETTERS + 5):
        handle("{not json at all")       # must not raise
    assert len(producer.messages) == MAX_CONSECUTIVE_DEADLETTERS + 5

def test_blank_text_still_registers_the_incident():
    """No chunks means nothing to index — but the incidents row is what
    autopilot claims against, and without it the incident is never briefed and
    nothing anywhere reports an error."""
    from freshet.common.incidents import ENSURE_INCIDENT_SQL
    from freshet.pipeline.embedder import make_handler
    from freshet.pipeline.embedding import StubEmbedder

    ev = Event(event_id="prov:inc1:u1", incident_id="inc1", service="prov",
               source=EventSource.ALERT, type="status_update", ts=datetime.now(UTC),
               text="   ", title="Some incident")
    conn, producer = _FakeConn(), _FakeProducer()
    make_handler(conn, StubEmbedder(), producer)(ev.model_dump_json())

    ensures = [(s, p) for s, p in conn.executed if s == ENSURE_INCIDENT_SQL]
    assert len(ensures) == 1, "a blank update must still register its incident"
    assert ensures[0][1][0] == "inc1"
    assert not any("vector_records" in s for s, _ in conn.executed), \
        "blank text must still index no chunks"
    assert producer.messages == [], "a blank update is not poison — it must not dead-letter"


def test_wrong_dimension_vectors_fail_with_a_named_error():
    """A 384-dim embedder against the vector(768) schema used to fail deep in
    psycopg as an infrastructure error. It is a configuration error and should
    say so — and it RAISES rather than dead-lettering, so it never trips the
    consecutive-dead-letter breaker; it stops the worker with a better message."""
    import pytest

    from freshet.pipeline.embedder import make_handler

    class _ShortEmb:
        name = "short"
        min_similarity = 0.3
        min_similarity_centered = None

        def encode(self, texts):
            return [[0.0] * 384 for _ in texts]

    ev = Event(event_id="prov:inc1:u1", incident_id="inc1", service="prov",
               source=EventSource.ALERT, type="status_update", ts=datetime.now(UTC),
               text="Some incident: the API returned 500s", title="Some incident")
    producer = _FakeProducer()
    with pytest.raises(RuntimeError, match="384.*768"):
        make_handler(_FakeConn(), _ShortEmb(), producer)(ev.model_dump_json())
    assert producer.messages == [], "config errors raise, they do not dead-letter"
