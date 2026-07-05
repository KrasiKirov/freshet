from datetime import datetime, timedelta, timezone

from freshet.autopilot.impact import classify_impact, estimate_impact, max_stated_pct

T0 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)


def _span(minutes):
    return T0, T0 + timedelta(minutes=minutes)


def test_max_stated_pct_parses_and_takes_max():
    assert max_stated_pct(["crossed 5% (now 11%)", "noise"]) == 11.0


def test_max_stated_pct_ignores_non_percent_numbers():
    # versions, timestamps, "5xx" have no % and must be ignored
    assert max_stated_pct(["deploy v2.15.0 at 12:00, 5xx errors"]) is None


def test_high_when_pct_high():
    o, r = _span(20)
    assert classify_impact(["a"], o, r, ["now 40%"]) == "High"


def test_high_when_breadth_ge_3():
    o, r = _span(5)
    assert classify_impact(["a", "b", "c"], o, r, ["now 2%"]) == "High"


def test_high_when_long_duration():
    o, r = _span(90)
    assert classify_impact(["a"], o, r, ["now 8%"]) == "High"


def test_low_when_quiet_short_single_service():
    o, r = _span(5)
    assert classify_impact(["a"], o, r, ["now 2%"]) == "Low"


def test_medium_otherwise():
    o, r = _span(30)
    assert classify_impact(["a"], o, r, ["now 11%"]) == "Medium"


def test_no_stated_figure_defaults_to_medium_not_low():
    # intentional: absence of a quoted % is "unknown severity", not "small" —
    # Medium, not Low. An explicitly low % on the same shape IS Low.
    o, r = _span(5)
    assert classify_impact(["a"], o, r, ["service recovered, no numbers here"]) == "Medium"
    assert classify_impact(["a"], o, r, ["now 2%"]) == "Low"


def test_monotonic_more_services_never_lowers():
    o, r = _span(5)
    order = {"Low": 0, "Medium": 1, "High": 2}
    base = classify_impact(["a"], o, r, ["now 2%"])
    more = classify_impact(["a", "b"], o, r, ["now 2%"])
    assert order[more] >= order[base]


def test_monotonic_higher_pct_never_lowers():
    o, r = _span(5)
    order = {"Low": 0, "Medium": 1, "High": 2}
    lo = classify_impact(["a"], o, r, ["now 2%"])
    hi = classify_impact(["a"], o, r, ["now 30%"])
    assert order[hi] >= order[lo]


def test_estimate_impact_line_ongoing_and_stated():
    line = estimate_impact(["a", "b", "c"], T0, None, ["now 40%"])
    assert line.startswith("Impact: High — 3 services, ongoing")
    assert "~40% errors" in line
