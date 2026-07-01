"""M16a integration: poll_once (monkeypatched fetch → fixture) into a run-unique raw
topic, run the pipeline, and assert the status events become queryable. Run via:
make test-integration."""
import json
import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

BROKERS = os.environ.get("FRESHET_BROKERS", "localhost:9092")
FIX = Path("freshet/ingest/fixtures/status/sample_incidents.json")


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    c.execute("DELETE FROM vector_records")
    c.execute("DELETE FROM incidents")
    yield c
    c.close()


def test_status_events_flow_through_pipeline(conn, monkeypatch):
    run_id = uuid.uuid4().hex[:8]
    raw, norm = f"raw.events.sp{run_id}", f"normalized.events.sp{run_id}"

    from freshet.common.kafka_io import make_producer
    from freshet.ingest import status_poller as sp
    from freshet.pipeline import embedder, normalizer
    from freshet.pipeline.embedding import StubEmbedder

    data = json.loads(FIX.read_text())
    monkeypatch.setattr(sp, "fetch", lambda url, timeout=10.0: data)

    producer = make_producer(BROKERS)
    produced = sp.poll_once([("cloudflare", "http://x")], producer, raw)
    assert produced == 3

    n = normalizer.run(BROKERS, group=f"n-{run_id}", max_messages=produced,
                       raw_topic=raw, normalized_topic=norm)
    assert n == produced
    n = embedder.run(BROKERS, group=f"e-{run_id}", max_messages=produced,
                     topic=norm, embedder=StubEmbedder())
    assert n == produced

    rows = conn.execute(
        "SELECT count(*) FROM vector_records WHERE service = 'cloudflare'"
    ).fetchone()[0]
    assert rows == produced
