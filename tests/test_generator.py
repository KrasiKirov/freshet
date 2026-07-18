from datetime import UTC, datetime

from freshet.common.schemas import Event, EventSource, EventType
from freshet.generator.generator import EventGenerator
from freshet.generator.scenarios import BAD_VERSION, GOOD_VERSION, SERVICE, build_scenario


def _collect(seed=1, count=40):
    return list(EventGenerator(seed=seed).stream(count))


def test_generator_is_deterministic():
    a = [e.model_dump_json() for e in _collect(seed=7)]
    b = [e.model_dump_json() for e in _collect(seed=7)]
    assert a == b
    # different seed -> different noise
    c = [e.text for e in _collect(seed=8)]
    assert [e.text for e in _collect(seed=7)] != c


def test_all_events_validate():
    for e in _collect():
        assert isinstance(e, Event)
        assert e.service
        assert e.source in EventSource


def test_scenario_is_injected_and_coherent():
    events = _collect(count=40)
    incident = [e for e in events if e.incident_id == "INC-DEMO-0001"]
    types = [e.type for e in incident]
    # the story must contain its key beats, in order
    assert EventType.DEPLOY_STARTED in types
    assert EventType.ERROR_SPIKE in types
    assert EventType.ROLLBACK in types
    assert EventType.RCA in types
    assert types.index(EventType.DEPLOY_STARTED) < types.index(EventType.ERROR_SPIKE)
    assert types.index(EventType.ERROR_SPIKE) < types.index(EventType.ROLLBACK)
    # all incident events are on the affected service
    assert all(e.service == SERVICE for e in incident)


def test_scenario_versions_consistent():
    inc = build_scenario(datetime(2026, 6, 6, tzinfo=UTC), "INC-X")
    rollback = next(e for e in inc if e.type == EventType.ROLLBACK)
    assert rollback.structured["from"] == BAD_VERSION
    assert rollback.structured["to"] == GOOD_VERSION


def test_timestamps_monotonic_in_noise():
    events = _collect(count=30)
    noise = [e for e in events if e.structured.get("noise")]
    ts = [e.ts for e in noise]
    assert ts == sorted(ts)


def test_live_stream_stamps_wall_clock_and_preserves_count():
    from freshet.generator.generator import live_stream

    gen = EventGenerator(seed=1, incident_after=0)
    before = datetime.now(UTC)
    events = list(live_stream(gen, count=5, spacing_s=0))
    after = datetime.now(UTC)
    assert len(events) == 5 + 9  # noise + scripted incident
    assert all(before <= e.ts <= after for e in events)
