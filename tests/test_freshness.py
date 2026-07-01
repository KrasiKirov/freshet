import pytest

from freshet.eval.freshness import freshness_report, percentile


def test_percentile_nearest_rank():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    assert percentile(vals, 50) == 5.0
    assert percentile(vals, 95) == 10.0
    assert percentile(vals, 99) == 10.0
    assert percentile([42.0], 50) == 42.0


def test_freshness_report_shape():
    rep = freshness_report([2.5, 0.5, 1.5])
    assert rep["count"] == 3
    assert rep["p50_s"] == 1.5
    assert rep["p95_s"] == 2.5


def test_freshness_report_empty_raises():
    with pytest.raises(ValueError):
        freshness_report([])
