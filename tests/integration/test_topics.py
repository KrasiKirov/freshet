"""The broker's topology must match what the SQL job and the workers assume.

`make up` used `rpk topic create ... -p 3 || true`, which no-ops against an existing
topic — so the 3 partitions it claimed were never applied to topics Redpanda had
already auto-created at 1. dedup_job.sql reasons explicitly about a 3-partition
incident.lifecycle; measured, it had 1.
"""
import subprocess

import pytest

pytestmark = pytest.mark.integration

EXPECTED = {
    "raw.incidents": 3,
    "normalized.updates": 3,
    "incident.lifecycle": 3,
    "deadletter.events": 3,
    "deadletter.raw": 3,
    "deadletter.unusable": 1,
}
GONE = {"raw.events", "normalized.events"}
# 30 days. The Flink source reads from earliest-offset, so at the 7-day default a
# job resubmitted after a week replays a silently truncated history.
MIN_RETENTION_MS = 2_592_000_000


def _rpk(*args: str) -> str:
    return subprocess.run(["docker", "exec", "freshet-redpanda", "rpk", *args],
                          capture_output=True, text=True, check=True).stdout


def _topics() -> dict[str, int]:
    found = {}
    for line in _rpk("topic", "list").splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            found[parts[0]] = int(parts[1])
    return found


def test_every_topic_exists_with_the_declared_partition_count():
    found = _topics()
    for name, partitions in EXPECTED.items():
        assert name in found, f"{name} missing; deploy/topics.sh did not run"
        assert found[name] == partitions, \
            f"{name} has {found[name]} partitions, want {partitions}"


def test_the_v1_topics_are_gone():
    assert GONE.isdisjoint(_topics()), "v1 topics still on the broker"


def test_raw_incidents_retains_longer_than_the_flink_job_may_be_down():
    line = next(line for line in _rpk("topic", "describe", "raw.incidents", "-c").splitlines()
                if line.startswith("retention.ms"))
    assert int(line.split()[1]) >= MIN_RETENTION_MS, \
        "the Flink source reads from earliest; 7 days silently truncates a replay"
