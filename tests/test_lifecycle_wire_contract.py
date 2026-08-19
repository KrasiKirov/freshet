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
