"""An empty run must not look like a measurement.

The freshness eval once reported ratio 0.06 from 33 rows that were all indexed
during a backlog drain, and n=0 runs before it emitted a 0.0 ratio — a number
shaped like a result, produced by a pipeline that was switched off.
"""
from freshet.eval.freshness import finalize_report, summarize


def test_an_empty_run_reports_status_not_zeros():
    r = finalize_report(summarize([], []))
    assert r["n"] == 0
    assert r["status"] == "not yet measured"
    for key in ("ratio", "streaming_mean_s", "batch_mean_s"):
        assert key not in r, f"{key} must not be emitted when nothing was scored"
    assert "poller" in r["explanation"]


def test_a_real_run_keeps_its_numbers():
    r = finalize_report(summarize([10.0, 20.0], [1800.0, 1800.0]))
    assert r["n"] == 2
    assert "ratio" in r and "status" not in r


def test_summarize_reports_the_ratio_it_measured():
    r = summarize([100.0, 100.0], [1800.0, 1800.0])
    assert r["n"] == 2
    assert r["ratio"] == round(1800.0 / 100.0, 2)
