"""`make stream-health` is the only way to see what json.ignore-parse-errors drops.

Those rows never become rows, so nothing downstream can count them — but the Flink
source did. These tests pin the arithmetic and the failure mode, without a cluster.
"""
import pytest

from freshet.stream import health

_JOBS = {"jobs": [{"id": "abc123", "status": "RUNNING"},
                  {"id": "old999", "status": "FINISHED"}]}
_JOB = {"vertices": [
    {"name": "Source: raw_incidents[1]", "metrics": {"read-records": 0, "write-records": 118075}},
    {"name": "Deduplicate -> normalized_updates", "metrics": {"read-records": 118075,
                                                              "write-records": 3671}},
]}


@pytest.fixture
def flink(monkeypatch):
    def _get(url: str) -> dict:
        return _JOBS if url.endswith("/jobs") else _JOB

    monkeypatch.setattr(health, "_get", _get)


def test_only_running_jobs_are_reported(flink):
    """A FINISHED job's counters are frozen; reporting them as current would make a
    dead pipeline look healthy."""
    assert health.running_job_ids(health.DEFAULT_URL) == ["abc123"]


def test_each_vertex_reports_what_it_read_and_what_it_emitted(flink):
    rows = health.vertex_rows(health.DEFAULT_URL, "abc123")
    assert rows[0] == ("Source: raw_incidents[1]", 0, 118075)
    assert rows[1] == ("Deduplicate -> normalized_updates", 118075, 3671)


def test_a_vertex_with_no_metrics_block_does_not_crash_the_report(monkeypatch):
    """Flink omits `metrics` for a vertex that has not started. The report exists to
    be run during an incident; it must not raise then."""
    monkeypatch.setattr(health, "_get", lambda url: (
        _JOBS if url.endswith("/jobs") else {"vertices": [{"name": "Sink: x"}]}))
    assert health.vertex_rows(health.DEFAULT_URL, "abc123") == [("Sink: x", 0, 0)]


def test_no_running_job_is_said_plainly_rather_than_rendered_as_zeroes(monkeypatch):
    monkeypatch.setattr(health, "_get", lambda url: {"jobs": []})
    assert health.render(health.DEFAULT_URL) == "no RUNNING job on this cluster"


def test_the_rendered_report_names_every_vertex_and_both_counts(flink):
    out = health.render(health.DEFAULT_URL)
    assert "job abc123" in out
    assert "in=    118075" in out and "out=      3671" in out
    assert "Deduplicate -> normalized_updates" in out
