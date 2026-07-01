"""Unit tests for status-feed incident mapping (keyless, fixture-based)."""
import json
from pathlib import Path

FIX = Path("freshet/ingest/fixtures/status/sample_incidents.json")


def _incident():
    return json.loads(FIX.read_text())["incidents"][0]


def test_one_event_per_update():
    from freshet.ingest.status_poller import map_incident
    inc = _incident()
    events = map_incident("cloudflare", inc)
    assert len(events) == len(inc["incident_updates"])


def test_event_fields_and_severity():
    from freshet.ingest.status_poller import map_incident
    e = map_incident("cloudflare", _incident())[0]
    assert e.service == "cloudflare"
    assert e.source.value == "alert"
    assert e.severity.value == "SEV2"          # impact "major"
    assert e.event_id.startswith("sp_")
    assert e.incident_id == "cloudflare:inc_100"
    assert e.type == "investigating"


def test_event_ids_are_stable():
    from freshet.ingest.status_poller import map_incident
    a = [e.event_id for e in map_incident("cloudflare", _incident())]
    b = [e.event_id for e in map_incident("cloudflare", _incident())]
    assert a == b and len(set(a)) == len(a)


def test_null_safe():
    from freshet.ingest.status_poller import map_incident
    assert map_incident("x", {}) == []
    assert map_incident("x", {"incident_updates": [None]}) == []
