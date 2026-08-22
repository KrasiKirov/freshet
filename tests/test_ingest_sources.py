from datetime import UTC, datetime

from freshet.ingest.sources import IncidentUpdate


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


def test_partition_key_groups_an_incidents_updates_together():
    """Kafka orders within a partition only. Keyed by the per-update dedup_key,
    one incident's updates scatter across partitions and the keep-first lifecycle
    projection picks a nondeterministic 'first open'."""
    a, b = _u(update_id="u1"), _u(update_id="u2", status="resolved")
    assert a.partition_key == b.partition_key == "github:inc1"
    assert a.dedup_key != b.dedup_key, "identity stays per-update"


def test_partition_key_matches_the_namespaced_incident_id():
    """It must be byte-identical to what the Flink projection writes, or the
    lifecycle key and the embedder's incidents row disagree."""
    u = _u(provider="openai", incident_id="01K9")
    assert u.partition_key == "openai:01K9"
    assert u.dedup_key.startswith(u.partition_key + ":")


def test_incident_update_is_frozen():
    u = _u()
    try:
        u.text = "mutated"          # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("IncidentUpdate must be immutable")


