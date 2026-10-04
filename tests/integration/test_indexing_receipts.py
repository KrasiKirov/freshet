"""Completion is after acknowledged writes; replay cannot move first visibility."""
from uuid import uuid4

import pytest

from freshet.common.schemas import Event, EventSource
from freshet.pipeline import embedder
from freshet.pipeline.embedding import StubEmbedder

pytestmark = pytest.mark.integration


@pytest.fixture
def event(conn):
    ev = Event(event_id=f"receipt:{uuid4().hex}", service="test", source=EventSource.ALERT,
               type="status_update", text="The service is unavailable. " * 40)
    yield ev
    conn.execute("DELETE FROM vector_records WHERE event_id = %s", (ev.event_id,))
    conn.execute("DELETE FROM event_indexing WHERE event_id = %s", (ev.event_id,))


def receipt(conn, event):
    return conn.execute(
        "SELECT first_queryable_at, last_queryable_at FROM event_indexing WHERE event_id = %s",
        (event.event_id,)).fetchone()


def test_completion_follows_embedding_and_every_chunk_write(conn, event, monkeypatch):
    checkpoints = []
    class MeasuredEmbedder(StubEmbedder):
        def encode(self, texts):
            vectors = super().encode(texts)
            checkpoints.append(conn.execute("SELECT clock_timestamp()").fetchone()[0])
            return vectors
    original = embedder.upsert_record
    def measured_upsert(*args, **kwargs):
        original(*args, **kwargs)
        checkpoints.append(conn.execute("SELECT clock_timestamp()").fetchone()[0])
    monkeypatch.setattr(embedder, "upsert_record", measured_upsert)
    embedder.make_handler(conn, MeasuredEmbedder(), None)(event.model_dump_json())
    first, last = receipt(conn, event)
    assert len(checkpoints) > 2  # actual multi-chunk event
    assert first == last and first >= max(checkpoints)


def test_replay_preserves_first_completion_and_advances_last(conn, event):
    handle = embedder.make_handler(conn, StubEmbedder(), None)
    handle(event.model_dump_json())
    first, last = receipt(conn, event)
    handle(event.model_dump_json())
    replay_first, replay_last = receipt(conn, event)
    assert replay_first == first and replay_last > last


def test_legacy_rows_do_not_acquire_a_fabricated_first_completion(conn, event):
    emb = StubEmbedder()
    for rec in embedder.records_for_event(event):
        embedder.upsert_record(conn, rec, emb.encode([rec.text])[0], emb.name)
    embedder.make_handler(conn, emb, None)(event.model_dump_json())
    first, last = receipt(conn, event)
    assert first is None and last is not None


def test_partial_write_failure_has_no_receipt_until_successful_retry(conn, event, monkeypatch):
    original = embedder.upsert_record
    calls = 0
    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("database unavailable")
        original(*args, **kwargs)
    monkeypatch.setattr(embedder, "upsert_record", fail_second)
    handle = embedder.make_handler(conn, StubEmbedder(), None)
    with pytest.raises(RuntimeError, match="database unavailable"):
        handle(event.model_dump_json())
    assert receipt(conn, event) == (None, None)
    monkeypatch.setattr(embedder, "upsert_record", original)
    handle(event.model_dump_json())
    first, last = receipt(conn, event)
    assert first is not None and first == last


@pytest.mark.parametrize("window_args", [[], ["--since-minutes", "5"]])
def test_freshness_report_scores_first_completion_and_excludes_legacy(
        conn, monkeypatch, tmp_path, window_args):
    import json
    import sys

    from freshet.autopilot.replay_demo import isolate
    from freshet.common import db, heartbeat
    from freshet.eval import freshness

    isolate(conn)
    ev = Event(event_id="receipt:new", service="test", source=EventSource.ALERT,
               type="status_update", text="The service is unavailable. " * 40)
    emb = StubEmbedder()
    handle = embedder.make_handler(conn, emb, None)
    handle(ev.model_dump_json())
    first, _ = receipt(conn, ev)
    handle(ev.model_dump_json())  # replay must not change the scored timestamp
    legacy = ev.model_copy(update={"event_id": "receipt:legacy"})
    for rec in embedder.records_for_event(legacy):
        embedder.upsert_record(conn, rec, emb.encode([rec.text])[0], emb.name)
    handle(legacy.model_dump_json())

    class BorrowedConnection:
        execute = conn.execute

        def close(self):
            pass  # the fixture owns this session

    monkeypatch.setattr(db, "connect", BorrowedConnection)
    monkeypatch.setattr(heartbeat, "continuous_run_start", lambda _: ev.ts)
    monkeypatch.setattr(sys, "argv", ["freshness", *window_args])
    monkeypatch.chdir(tmp_path)
    freshness.main()
    report = json.loads((tmp_path / "results/freshness.json").read_text())
    assert report["measurement_version"] == 2
    assert report["n"] == 1
    assert report["n_rows"] > 1
    assert report["streaming_mean_s"] == round((first - ev.ts).total_seconds(), 2)
