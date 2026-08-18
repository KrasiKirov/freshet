from freshet.stream.logic import (
    BURST_THRESHOLD,
    WINDOW_SECONDS,
    dedup_key,
    is_burst,
    lifecycle_for,
)


def test_dedup_key_matches_the_pollers_key():
    rec = {"provider": "github", "incident_id": "inc1", "update_id": "abc123"}
    assert dedup_key(rec) == "github:inc1:abc123"


def test_dedup_key_distinguishes_every_field():
    base = {"provider": "a", "incident_id": "b", "update_id": "c"}
    keys = {dedup_key(base),
            dedup_key({**base, "provider": "z"}),
            dedup_key({**base, "incident_id": "z"}),
            dedup_key({**base, "update_id": "z"})}
    assert len(keys) == 4


def test_burst_needs_three_distinct_providers():
    assert is_burst(["a", "b", "c"]) is True
    assert is_burst(["a", "b"]) is False
    assert is_burst(["a", "a", "a", "a"]) is False, "one noisy provider is not a burst"
    assert is_burst(["a", "a", "b", "b", "c"]) is True


def test_burst_threshold_is_configurable():
    assert is_burst(["a", "b"], threshold=2) is True
    assert is_burst(["a", "b", "c"], threshold=5) is False


def test_defaults_match_the_spec():
    assert BURST_THRESHOLD == 3
    assert WINDOW_SECONDS == 300


def test_lifecycle_is_read_from_the_feeds_own_status():
    """v1 had to INFER incident lifecycle by correlating events. The status feeds
    state it outright, so this is a lookup, not a heuristic."""
    assert lifecycle_for("investigating") == "opened"
    assert lifecycle_for("identified") == "opened"
    assert lifecycle_for("resolved") == "resolved"
    assert lifecycle_for("completed") == "resolved"


def test_intermediate_and_unknown_statuses_signal_nothing():
    for status in ("monitoring", "in_progress", "verifying", "unknown", "", "wat"):
        assert lifecycle_for(status) is None, status


def test_lifecycle_lookup_is_case_and_whitespace_insensitive():
    assert lifecycle_for("  Resolved  ") == "resolved"
    assert lifecycle_for("INVESTIGATING") == "opened"
