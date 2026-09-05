import pytest

from freshet.eval.freshness import (
    batch_alignment_sweep,
    batch_staleness,
    distinct_updates,
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


def test_batch_staleness_takes_the_boundarys_phase():
    """The refresh boundary need not sit at HH:00:00 exactly. `phase_s` shifts
    where the schedule's boundary sits within the interval; offset 0 is only one
    of interval_s possible choices."""
    # boundary at :00:00 exactly (phase 0): posted 100s after -> waits 3500s
    assert batch_staleness(100.0, 3600.0, phase_s=0.0) == 3500.0
    # boundary at :00:50 (phase 50): posted 100s after the hour is 50s past that
    # boundary -> waits the remaining 3550s
    assert batch_staleness(100.0, 3600.0, phase_s=50.0) == 3550.0


def test_batch_staleness_averages_to_half_the_interval():
    """Sanity: uniformly-arriving events wait interval/2 on average. This is what
    makes the ~1800s hourly figure a derivation from the cadence, not a guess."""
    mean = sum(batch_staleness(t, 3600.0) for t in range(0, 3600, 10)) / 360
    assert 1750 < mean < 1850


def test_batch_alignment_sweep_returns_one_mean_per_second_offset():
    """The boundary's phase is an arbitrary choice out of interval_s possible
    second-offsets. The sweep must score all of them, not assume phase 0."""
    got = batch_alignment_sweep([100.0, 200.0], interval_s=3600.0)
    assert len(got) == 3600


def test_reported_batch_arm_is_the_mean_across_alignments_not_a_single_phase():
    """Scoring only phase 0 (HH:00:00 exactly) reports one arbitrary alignment's
    number as if it were THE hourly-batch cost. A workload that happens to arrive
    right after the hour scores an inflated arm at phase 0 alone (its updates are
    each nearly a full interval away from that specific boundary); averaging over
    every phase the boundary could sit at removes that dependency on which
    alignment we happened to pick."""
    # every update posted a few seconds after the top of the hour
    clustered = [1.0, 2.0, 3.0, 4.0, 5.0]
    got = summarize(streaming=[10.0] * 5, posted_ats=clustered)
    # phase 0 alone would report ~3597s (see the sibling "does not inflate" test)
    assert got["batch_mean_s"] == pytest.approx(1800.5, abs=1.0)


def test_a_workload_clustered_on_the_hour_does_not_inflate_the_arm_the_way_offset_0_does():
    """Real arrivals cluster at the top of the hour (scheduled maintenance windows
    start on the hour by nature). Scored at offset 0 alone, that clustering reads
    as the batch arm being unusually SLOW — an artifact of which alignment was
    picked, not of the cadence. The alignment-independent figure must not carry
    that inflation."""
    clustered = [1.0, 2.0, 3.0, 4.0, 5.0]
    got = summarize(streaming=[10.0] * 5, posted_ats=clustered)
    naive_offset_0 = sum(batch_staleness(p, 3600.0, phase_s=0.0) for p in clustered) / len(clustered)
    assert naive_offset_0 == pytest.approx(3597.0)
    assert got["batch_mean_s"] < naive_offset_0 / 1.5
    assert got["batch_mean_s_top_of_hour"] == pytest.approx(naive_offset_0)


def test_a_uniformly_distributed_workload_yields_an_arm_near_half_the_interval():
    """A workload with no particular alignment to the hour should land near the
    textbook interval/2 derivation, confirming the swept figure is not itself
    an artifact of some other systematic skew."""
    uniform = list(range(0, 3600, 60))
    got = summarize(streaming=[10.0] * len(uniform), posted_ats=uniform)
    assert got["batch_mean_s"] == pytest.approx(1800.0, abs=50.0)


def test_summarize_reports_the_spread_across_alignments():
    """A single headline mean hides how sensitive the arm is to the boundary's
    phase; min/median/max make that sensitivity visible."""
    clustered = [1.0, 2.0, 3.0, 4.0, 5.0]
    got = summarize(streaming=[10.0] * 5, posted_ats=clustered)
    assert got["batch_mean_s_min"] < got["batch_mean_s"] < got["batch_mean_s_max"]
    assert got["batch_mean_s_min"] <= got["batch_mean_s_median"] <= got["batch_mean_s_max"]


def test_summarize_reports_the_ratio():
    # posted_at with a .5s fraction makes the alignment-independent mean land on
    # exactly 1800.0s (1800.5 - the fractional second), so the ratio is exact.
    got = summarize(streaming=[31.0, 31.0], posted_ats=[100.5, 200.5])
    assert got["streaming_mean_s"] == 31.0
    assert got["batch_mean_s"] == 1800.0
    assert round(got["ratio"], 1) == 58.1
    assert got["n"] == 2


def test_summarize_reports_percentiles_not_just_the_mean():
    got = summarize(streaming=[10.0, 20.0, 30.0, 400.0], posted_ats=[100.5] * 4)
    assert got["streaming_p50_s"] == 20.0
    assert got["streaming_p95_s"] == 400.0


def test_summarize_handles_the_empty_case_without_dividing_by_zero():
    got = summarize(streaming=[], posted_ats=[])
    assert got["n"] == 0 and got["ratio"] == 0.0


def test_percentile_on_empty_raises():
    with pytest.raises(ValueError):
        percentile([], 50)


def test_distinct_updates_counts_a_multi_chunk_update_once():
    """vector_records holds one row per CHUNK. Two chunks belonging to the same
    update (same event_id) must contribute a single (posted_at, queryable_at)
    entry, not two — otherwise `n` counts chunks while the docs claim it counts
    updates."""
    rows = [
        (100.0, 130.0, "incident:1"),
        (100.0, 130.0, "incident:1"),  # second chunk of the same update
        (200.0, 260.0, "incident:2"),
    ]
    got = distinct_updates(rows)
    assert len(got) == 2
    assert (100.0, 130.0) in got
    assert (200.0, 260.0) in got
