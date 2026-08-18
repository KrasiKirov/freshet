from datetime import UTC, datetime

from freshet.ingest.sources import IncidentSource, IncidentUpdate


def _u(**kw):
    base = {"provider": "github", "incident_id": "inc1", "update_id": "u1",
            "created_at": datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
            "status": "investigating", "text": "looking into it",
            "incident_name": "Elevated errors"}
    base.update(kw)
    return IncidentUpdate(**base)


def test_dedup_key_is_stable_and_unique_per_update():
    assert _u().dedup_key == "github:inc1:u1"
    assert _u(update_id="u2").dedup_key != _u().dedup_key
    assert _u(provider="reddit").dedup_key != _u().dedup_key


def test_incident_update_is_frozen():
    u = _u()
    try:
        u.text = "mutated"          # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("IncidentUpdate must be immutable")


def test_fixture_source_satisfies_the_protocol():
    class FixtureSource:
        name = "fixture"

        def fetch(self) -> list[IncidentUpdate]:
            return [_u()]

    src: IncidentSource = FixtureSource()
    assert [u.dedup_key for u in src.fetch()] == ["github:inc1:u1"]
