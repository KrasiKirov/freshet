"""Keying incident.lifecycle by incident_id must actually order each incident.

The config guard (tests/test_stream_lifecycle_key.py) proves the Flink sink
declares the key. This proves the consequence on a REAL multi-partition topic:
every incident's 'opened' and 'resolved' land in one partition, in that order.

Scope, stated honestly: this produces the keyed records directly rather than
running the Flink job, so it verifies the guarantee the key buys, not Flink's
emission of it. Those two tests together cover the failure; a Flink MiniCluster
run inside the suite would not be worth its cost here.
"""
import json
import uuid

import pytest

from freshet.common.kafka_io import make_consumer, make_producer, produce_sync

pytestmark = pytest.mark.integration

BROKERS = "localhost:9092"
PARTITIONS = 3
INCIDENTS = 12   # enough that random assignment across 3 partitions would show


@pytest.fixture
def topic():
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BROKERS})
    name = f"test.lifecycle.{uuid.uuid4().hex[:8]}"
    for fut in admin.create_topics([NewTopic(name, PARTITIONS, 1)]).values():
        fut.result(timeout=30)
    yield name
    for fut in admin.delete_topics([name]).values():
        fut.result(timeout=30)


def _drain(name, expected):
    c = make_consumer(BROKERS, f"g-{uuid.uuid4().hex[:8]}", [name], auto_commit=False)
    got = []
    try:
        while len(got) < expected:
            m = c.poll(10.0)
            if m is None:
                break
            assert not m.error(), m.error()
            got.append((m.partition(), json.loads(m.value().decode())))
    finally:
        c.close()
    return got


def test_opened_and_resolved_share_a_partition_and_stay_ordered(topic):
    ids = [f"INC-{i}" for i in range(INCIDENTS)]
    p = make_producer(BROKERS)
    # Interleave incidents so ordering cannot come from producing each pair together.
    for status in ("opened", "resolved"):
        for i in ids:
            produce_sync(p, topic, json.dumps({"incident_id": i, "status": status}), key=i)
    p.flush()

    records = _drain(topic, len(ids) * 2)
    assert len(records) == len(ids) * 2, "not every lifecycle event was delivered"

    seen: dict[str, list[tuple[int, str]]] = {}
    for partition, value in records:
        seen.setdefault(value["incident_id"], []).append((partition, value["status"]))

    for incident, events in seen.items():
        partitions = {p for p, _ in events}
        assert len(partitions) == 1, f"{incident} split across partitions {partitions}"
        assert [s for _, s in events] == ["opened", "resolved"], \
            f"{incident} arrived out of order: {events}"

    # Guard against a vacuous pass: on a 1-partition topic co-location is free and
    # this test would prove nothing. The keys must genuinely spread.
    assert len({p for p, _ in records}) > 1, "keys did not spread across partitions"
