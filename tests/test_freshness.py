import pytest

from freshet.eval.freshness import (
    batch_staleness,
    percentile,
    streaming_staleness,
    summarize,
)


def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4], 50) == 2
    assert percentile([1, 2, 3, 4], 100) == 4


def test_streaming_staleness_is_queryable_minus_posted():
    assert streaming_staleness(posted_at=100.0, queryable_at=131.0) == 31.0


def test_batch_staleness_waits_for_the_next_refresh_boundary():
    # posted 100s after a refresh on an hourly cadence -> waits the remaining 3500s
    assert batch_staleness(posted_at=100.0, interval_s=3600.0) == 3500.0
    # posted exactly on a boundary -> waits a full interval for the next one
    assert batch_staleness(posted_at=0.0, interval_s=3600.0) == 3600.0


def test_batch_staleness_averages_to_half_the_interval():
    """Sanity: uniformly-arriving events wait interval/2 on average. This is what
    makes the ~1800s hourly-batch arm a derivation rather than a guess."""
    mean = sum(batch_staleness(t, 3600.0) for t in range(0, 3600, 10)) / 360
    assert 1750 < mean < 1850


def test_summarize_reports_the_ratio():
    got = summarize(streaming=[31.0, 31.0], batch=[1800.0, 1800.0])
    assert got["streaming_mean_s"] == 31.0
    assert got["batch_mean_s"] == 1800.0
    assert round(got["ratio"], 1) == 58.1
    assert got["n"] == 2


def test_summarize_reports_percentiles_not_just_the_mean():
    got = summarize(streaming=[10.0, 20.0, 30.0, 400.0], batch=[1800.0] * 4)
    assert got["streaming_p50_s"] == 20.0
    assert got["streaming_p95_s"] == 400.0


def test_summarize_handles_the_empty_case_without_dividing_by_zero():
    got = summarize(streaming=[], batch=[])
    assert got["n"] == 0 and got["ratio"] == 0.0


def test_percentile_on_empty_raises():
    with pytest.raises(ValueError):
        percentile([], 50)
