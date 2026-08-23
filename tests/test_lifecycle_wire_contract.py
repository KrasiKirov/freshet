"""The lifecycle JSON on the wire is a contract between Flink SQL and Python.

Nothing in the round-trip tests could catch a mismatch: they build a
LifecycleEvent, serialise it, and parse it back, so both sides share whatever
schema the dataclass happens to have. The real producer is the Flink sink, whose
JSON field names are its COLUMN names — and when those drifted from the
consumer's, the autopilot raised KeyError on its first message and never posted
a brief. These tests pin the two representations against each other.
"""
import json
import re
from pathlib import Path

import pytest

from freshet.pipeline.lifecycle import LifecycleEvent

SQL = (Path(__file__).resolve().parents[1] / "freshet/stream/dedup_job.sql").read_text()


def _sink_columns() -> list[str]:
    block = re.search(r"CREATE TABLE incident_lifecycle \((.*?)\) WITH", SQL, re.S)
    assert block, "no incident_lifecycle table in dedup_job.sql"
    cols = []
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        cols.append(line.split()[0].strip("`,"))
    return cols


# One sample value per sink column. Looked up by NAME, not position, so adding a
# column to the sink does not silently shift every value one field to the left.
_SAMPLES = {"incident_id": "INC-1", "service": "cloudflare", "type": "opened",
            "ts": "2026-08-19T06:30:00Z", "title": "Elevated errors"}


def test_consumer_reads_a_payload_shaped_like_the_flink_sink():
    """Build the exact JSON the sink emits — one field per column — and parse it."""
    columns = _sink_columns()
    payload = {c: _SAMPLES.get(c, "unmodelled") for c in columns}
    ev = LifecycleEvent.from_json(json.dumps(payload))
    assert ev.incident_id == "INC-1"
    assert ev.service == "cloudflare"
    assert ev.type == "opened"          # the field the consumer branches on


def test_every_field_the_consumer_needs_is_a_sink_column():
    columns = set(_sink_columns())
    for required in ("incident_id", "service", "type", "ts"):
        assert required in columns, (
            f"LifecycleEvent needs {required!r}, but the Flink sink emits {sorted(columns)}")


def test_legacy_status_payloads_still_parse():
    # Records produced before the column rename are still retained on the topic;
    # a replay must not poison-pill the consumer.
    raw = json.dumps({"incident_id": "INC-2", "service": "zoom",
                      "status": "resolved", "ts": "2026-08-19T06:30:00Z"})
    assert LifecycleEvent.from_json(raw).type == "resolved"


def test_a_payload_with_neither_field_is_rejected_clearly():
    raw = json.dumps({"incident_id": "INC-3", "service": "zoom", "ts": "2026-08-19T06:30:00Z"})
    with pytest.raises(KeyError, match="neither"):
        LifecycleEvent.from_json(raw)


def test_the_sql_projections_namespace_incident_id_by_provider():
    """incidents.incident_id is a PRIMARY KEY shared across 42 tenants. Statuspage
    ids are per-tenant, so an unqualified id silently merges two providers'
    incidents into one row — and the brief then cites the wrong provider."""
    sql = Path("freshet/stream/dedup_job.sql").read_text()
    normalized = sql.split("INSERT INTO normalized_updates")[1].split("FROM (")[0]
    assert "provider || ':' || incident_id AS incident_id" in normalized

    for branch in sql.split("INSERT INTO incident_lifecycle")[1:]:
        head = branch.split("FROM (")[0]
        assert "provider || ':' || incident_id AS incident_id" in head, \
            "the lifecycle key must match what the embedder writes, or no brief can claim"


def test_dedup_partitions_on_content_but_keys_output_on_identity():
    """Content in the dedup tuple lets an EDIT through exactly once; identity in the
    event_id lets the embedder overwrite the same rows instead of adding new ones.

    The tempting alternative — keep-LAST dedup — is wrong here and must not come
    back: the poller is stateless and re-emits every update every 60s, so keep-last
    would re-emit and re-embed the entire corpus every minute, which is precisely
    what deduping upstream of the embedder exists to prevent."""
    sql = Path("freshet/stream/dedup_job.sql").read_text()
    dedup = sql.split("INSERT INTO normalized_updates")[1]
    assert re.search(r"PARTITION BY provider, incident_id, update_id,\s*"
                     r"(coalesce\()?body_digest", dedup), \
        "the content digest must be part of the dedup tuple"
    assert "ORDER BY proc_time ASC" in dedup, "keep-FIRST on the content tuple"
    head = dedup.split("FROM (")[0]
    assert "provider || ':' || incident_id || ':' || update_id AS event_id" in head
    assert "body_digest" not in head, \
        "the digest must not leak into event_id, or the upsert stops overwriting"


def test_both_lifecycle_transitions_are_guarded_by_the_same_recency_window():
    """The opened branch had a 24h guard; the resolved branch did not, so a cold
    replay flagged every historical incident postmortem_needed. The resolving
    update's own created_at is 'now', so the guard is safe for long incidents."""
    sql = Path("freshet/stream/dedup_job.sql").read_text()
    lifecycle = sql.split("CREATE TEMPORARY VIEW recent_transitions")[1]
    assert lifecycle.count("CURRENT_TIMESTAMP - INTERVAL '24' HOUR") == 1, \
        "one shared guard covering both transitions"


def test_no_watermark_is_declared_because_nothing_orders_by_event_time():
    """A watermark no operator consumes is dead weight that invited three mutually
    contradictory comments about its value (30s, 90s, and the actual 7 days)."""
    sql = Path("freshet/stream/dedup_job.sql").read_text()
    assert "WATERMARK FOR" not in sql.upper(), "no watermark may be declared"
    assert "90s watermark" not in sql, "and no comment may claim one exists"
    assert "SET 'table.exec.source.idle-timeout'" not in sql, \
        "idle-timeout only advances watermarks; without one it is a no-op"


def test_the_resolved_predicate_covers_the_wording_providers_actually_use():
    """hashicorp posts status 'complete', not 'completed'. Measured on the live
    feed: without it those incidents never emit a resolved lifecycle event and
    never get a postmortem."""
    sql = Path("freshet/stream/dedup_job.sql").read_text()
    for word in ("'resolved'", "'completed'", "'complete'"):
        assert word in sql, f"resolved predicate is missing {word}"
