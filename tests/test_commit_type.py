from freshet.common.schemas import CHANGE_TYPES, EventType


def test_commit_event_type_exists():
    assert EventType.COMMIT.value == "commit"


def test_change_types_unchanged_no_commit():
    # CHANGE_TYPES stays == the 6 archetype change types; "commit" lives only in
    # synthesis._CAUSE_TYPES, so the archetype single-source-of-truth is preserved.
    assert "commit" not in CHANGE_TYPES
    assert len(CHANGE_TYPES) == 6
