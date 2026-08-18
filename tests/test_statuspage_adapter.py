from datetime import UTC, datetime

from freshet.ingest.statuspage import parse_statuspage

PAYLOAD = {
    "incidents": [
        {
            "id": "inc1",
            "name": "Elevated API errors",
            "incident_updates": [
                {"id": "u2", "status": "resolved",
                 "body": "This incident has been resolved.",
                 "created_at": "2026-08-18T12:30:00.000Z"},
                {"id": "u1", "status": "investigating",
                 "body": "We are investigating elevated errors.",
                 "created_at": "2026-08-18T12:00:00.000Z"},
            ],
        }
    ]
}


def test_parses_every_update_of_every_incident():
    got = parse_statuspage("github", PAYLOAD)
    assert [u.update_id for u in got] == ["u1", "u2"], "must be sorted oldest-first"
    assert got[0].provider == "github"
    assert got[0].incident_name == "Elevated API errors"
    assert got[0].status == "investigating"
    assert got[0].created_at == datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def test_timestamps_are_timezone_aware_utc():
    for u in parse_statuspage("github", PAYLOAD):
        assert u.created_at.tzinfo is not None
        assert u.created_at.utcoffset().total_seconds() == 0


def test_empty_and_malformed_payloads_yield_nothing_rather_than_raising():
    assert parse_statuspage("x", {}) == []
    assert parse_statuspage("x", {"incidents": []}) == []
    assert parse_statuspage("x", {"incidents": [{"id": "i", "incident_updates": None}]}) == []


def test_updates_missing_required_fields_are_skipped():
    bad = {"incidents": [{"id": "i", "name": "n",
                          "incident_updates": [{"status": "x", "body": "b"}]}]}
    assert parse_statuspage("x", bad) == []
