"""The lifecycle topic MUST be keyed by incident_id.

Kafka orders records within a partition only. `make up` creates
incident.lifecycle with 3 partitions, so an unkeyed sink can deliver an
incident's 'resolved' before its 'opened'. The consumer skips a postmortem for
an incident it never briefed, so that incident's postmortem is lost — silently,
and only on multi-partition topics, which is exactly the kind of regression a
config file loses without a test watching it.
"""
import re
from pathlib import Path

SQL = (Path(__file__).resolve().parents[1] / "freshet/stream/dedup_job.sql").read_text()


def _table_options(name: str) -> dict[str, str]:
    """Extract the WITH (...) options of one CREATE TABLE."""
    block = re.search(rf"CREATE TABLE {name} \(.*?\) WITH \((.*?)\n\);", SQL, re.S)
    assert block, f"no CREATE TABLE {name} in dedup_job.sql"
    return dict(re.findall(r"'([\w.-]+)'\s*=\s*'([^']*)'", block.group(1)))


def test_lifecycle_sink_is_keyed_by_incident_id():
    opts = _table_options("incident_lifecycle")
    assert opts["key.fields"] == "incident_id"
    assert opts["key.format"] == "json"


def test_lifecycle_sink_still_declares_its_value_format():
    # Adding a key means 'format' must become 'value.format'; leaving the bare
    # 'format' alongside a key config makes Flink reject the table outright.
    opts = _table_options("incident_lifecycle")
    assert opts["value.format"] == "json"
    assert opts["value.json.timestamp-format.standard"] == "ISO-8601"
    assert "format" not in opts
