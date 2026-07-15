"""Unit tests for agent_eval: sample_incidents, aggregate, _single_shot (mocked)."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _truth(incident_id, archetype, cause_id="c", fix_id="f", service="svc"):
    return SimpleNamespace(
        incident_id=incident_id,
        archetype=archetype,
        cause_id=cause_id,
        fix_id=fix_id,
        service=service,
    )


_ARCHETYPES_6 = [
    "deploy_regression", "config_change", "dependency_outage",
    "resource_exhaustion", "cert_expiry", "bad_migration",
]


def _make_truths(n=40):
    return [
        _truth(
            f"INC-{i+1:04d}",
            _ARCHETYPES_6[i % 6],
            f"cause_{i}",
            f"fix_{i}",
            f"svc-{i:02d}",
        )
        for i in range(n)
    ]


def test_sample_incidents_returns_n_per_archetype():
    from freshet.eval.agent_eval import sample_incidents

    truths = _make_truths(40)
    sample = sample_incidents(truths, n_per_archetype=2)
    assert len(sample) == 12
    by_archetype: dict = {}
    for t in sample:
        by_archetype.setdefault(t.archetype, []).append(t)
    for arch, items in by_archetype.items():
        assert len(items) == 2, f"{arch} has {len(items)}, expected 2"


def test_sample_incidents_deterministic():
    from freshet.eval.agent_eval import sample_incidents

    truths = _make_truths(40)
    s1 = [t.incident_id for t in sample_incidents(truths, n_per_archetype=2)]
    s2 = [t.incident_id for t in sample_incidents(truths, n_per_archetype=2)]
    assert s1 == s2


def test_aggregate_basic():
    from freshet.eval.agent_eval import aggregate

    records = [
        {"cause_hit": True, "fix_hit": True},
        {"cause_hit": True, "fix_hit": False},
        {"cause_hit": False, "fix_hit": False},
        {"cause_hit": False, "fix_hit": True},
    ]
    result = aggregate(records)
    assert result["cause_recall"] == 0.5
    assert result["fix_recall"] == 0.5
    assert result["n"] == 4


def test_aggregate_all_hit():
    from freshet.eval.agent_eval import aggregate

    records = [{"cause_hit": True, "fix_hit": True}] * 5
    result = aggregate(records)
    assert result["cause_recall"] == 1.0
    assert result["fix_recall"] == 1.0


def test_aggregate_empty():
    from freshet.eval.agent_eval import aggregate

    result = aggregate([])
    assert result["cause_recall"] == 0.0
    assert result["fix_recall"] == 0.0
    assert result["n"] == 0


def test_single_shot_returns_cause_and_fix_ids():
    from freshet.eval.agent_eval import _single_shot

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    truth = _truth("INC-0001", "deploy_regression", cause_id="c1", fix_id="f1", service="svc-00")

    fake_cause = SimpleNamespace(event_id="c1")
    fake_fix = SimpleNamespace(event_id="f1")
    fake_timeline = SimpleNamespace(cause=fake_cause, fix=fake_fix)

    with patch("freshet.eval.agent_eval.hybrid_search") as mock_hs, \
         patch("freshet.eval.agent_eval.build_timeline") as mock_tl:
        mock_hs.return_value = MagicMock(hits=[])
        mock_tl.return_value = fake_timeline
        result = _single_shot(mock_conn, mock_embedder, truth)

    assert result["cause_id"] == "c1"
    assert result["fix_id"] == "f1"


def test_fixed_two_step_anchors_on_spike_and_uses_temporal_lookup():
    from datetime import datetime, timedelta, timezone
    from freshet.eval.agent_eval import _fixed_two_step

    t0 = datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)
    spike_hit = SimpleNamespace(event_id="s1", type="error_spike",
                                service="svc-00", ts=t0)
    neighbors = [
        SimpleNamespace(event_id="c1", ts=t0 - timedelta(minutes=5),
                        type="deploy_started", text="deploy v2"),
        SimpleNamespace(event_id="d1", ts=t0 - timedelta(minutes=20),
                        type="deploy_started", text="older deploy"),
        SimpleNamespace(event_id="f1", ts=t0 + timedelta(minutes=10),
                        type="rollback", text="rolled back"),
    ]
    truth = _truth("INC-0001", "deploy_regression", cause_id="c1", fix_id="f1",
                   service="svc-00")
    with patch("freshet.eval.agent_eval.hybrid_search") as mock_hs, \
         patch("freshet.eval.agent_eval.events_around") as mock_ea:
        mock_hs.return_value = MagicMock(hits=[spike_hit])
        mock_ea.return_value = neighbors
        result = _fixed_two_step(MagicMock(), MagicMock(), truth)

    # latest change before the spike wins (not the older deploy); first
    # remediation after the spike is the fix
    assert result["cause_id"] == "c1"
    assert result["fix_id"] == "f1"


def test_fixed_two_step_abstains_without_a_spike():
    from freshet.eval.agent_eval import _fixed_two_step

    truth = _truth("INC-0001", "deploy_regression", service="svc-00")
    with patch("freshet.eval.agent_eval.hybrid_search") as mock_hs, \
         patch("freshet.eval.agent_eval.events_around") as mock_ea:
        mock_hs.return_value = MagicMock(hits=[])
        result = _fixed_two_step(MagicMock(), MagicMock(), truth)

    assert result == {"cause_id": None, "fix_id": None}
    mock_ea.assert_not_called()


def test_single_shot_returns_none_when_timeline_empty():
    from freshet.eval.agent_eval import _single_shot

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    truth = _truth("INC-0001", "deploy_regression", service="svc-00")

    fake_timeline = SimpleNamespace(cause=None, fix=None)

    with patch("freshet.eval.agent_eval.hybrid_search") as mock_hs, \
         patch("freshet.eval.agent_eval.build_timeline") as mock_tl:
        mock_hs.return_value = MagicMock(hits=[])
        mock_tl.return_value = fake_timeline
        result = _single_shot(mock_conn, mock_embedder, truth)

    assert result["cause_id"] is None
    assert result["fix_id"] is None
