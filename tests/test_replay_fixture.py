import json
from pathlib import Path

FIXTURE = Path("freshet/eval/fixtures/replay.jsonl")


def test_replay_fixture_is_usable():
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    assert len(rows) >= 50, "fixture too small to drive the freshness eval"
    required = {"provider", "incident_id", "update_id", "created_at", "text"}
    for r in rows:
        assert required <= set(r), f"missing keys: {required - set(r)}"
    stamps = [r["created_at"] for r in rows]
    assert stamps == sorted(stamps), "fixture must be sorted by created_at"
