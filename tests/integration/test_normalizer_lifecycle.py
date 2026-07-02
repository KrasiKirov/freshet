"""Integration: an opening event flowing through the normalizer produces exactly
one 'opened' lifecycle message."""
import uuid

import pytest

from freshet.common.kafka_io import consume_loop, make_producer, produce_sync
from freshet.common.schemas import Event, EventSource, Severity
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC, LifecycleEvent

pytestmark = pytest.mark.integration
BROKERS = "localhost:9092"


def test_open_produces_one_lifecycle_event():
    from freshet.pipeline.normalizer import run
    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.events.lc{run_id}"
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    svc = f"svc-{uuid.uuid4().hex[:6]}"
    ev = Event(service=svc, source=EventSource.ALERT, type="error_spike",
               text="boom", incident_id=iid, severity=Severity.SEV1)

    # feed one raw event (on a run-unique topic, per repo convention, so a
    # fresh consumer group reading from earliest can't pick up backlog from
    # other tests) and run the normalizer for exactly one message
    prod = make_producer(BROKERS)
    produce_sync(prod, raw_topic, ev.model_dump_json(), key=svc)
    run(brokers=BROKERS, group=f"norm-{run_id}", max_messages=1, raw_topic=raw_topic)

    # drain the lifecycle topic and find our event
    seen = []
    consume_loop(BROKERS, f"life-{uuid.uuid4().hex[:6]}", [LIFECYCLE_TOPIC],
                 lambda v: seen.append(LifecycleEvent.from_json(v)),
                 idle_timeout_s=5.0)
    ours = [e for e in seen if e.incident_id == iid]
    assert len(ours) == 1 and ours[0].type == "opened" and ours[0].service == svc
