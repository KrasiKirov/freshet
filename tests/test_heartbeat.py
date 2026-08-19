"""Freshness must score only a run the pipeline was demonstrably up for.

`ts >= min(indexed_at)` excluded backfilled history but not downtime catch-up:
after a 14-hour outage the burst of late-indexed updates scored 9.8-hour
staleness and reported streaming as 14x SLOWER than an hourly batch.
"""
from datetime import UTC, datetime, timedelta

from freshet.common.heartbeat import Heartbeat, continuous_run_start


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class _Conn:
    def __init__(self, beats=()):
        self.beats = list(beats)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

        class _R:
            def __init__(self, rows):
                self.rows = rows
            def fetchall(self_inner):
                return self_inner.rows
        return _R([(b,) for b in self.beats])


def test_beats_are_throttled():
    clock, conn = _Clock(), _Conn()
    hb = Heartbeat("embedder", interval_s=30, now=clock)
    assert hb.beat(conn) is True          # first beat always writes
    clock.t = 10.0
    assert hb.beat(conn) is False         # too soon
    clock.t = 40.0
    assert hb.beat(conn) is True


def _beats(*minutes_ago):
    now = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    return [now - timedelta(minutes=m) for m in minutes_ago]


def test_an_unbroken_run_starts_at_its_earliest_beat():
    conn = _Conn(_beats(0, 1, 2, 3, 4))
    assert continuous_run_start(conn) == _beats(4)[0]


def test_a_gap_ends_the_run_so_the_earlier_session_is_excluded():
    # 0..2 min ago, then a 14-hour hole, then an older session
    conn = _Conn(_beats(0, 1, 2, 840, 841, 842))
    assert continuous_run_start(conn) == _beats(2)[0], (
        "the pre-outage session must not be scored against this run")


def test_a_short_pause_does_not_end_the_run():
    conn = _Conn(_beats(0, 1, 3, 4))      # 2-minute pause, under the 5-min tolerance
    assert continuous_run_start(conn) == _beats(4)[0]


def test_no_heartbeat_means_nothing_can_be_scored():
    assert continuous_run_start(_Conn([])) is None


def test_liveness_is_proven_when_no_messages_arrive():
    """The embedder beats on the consume loop's IDLE tick, not only when it
    handles a message. Beating only on traffic made a quiet stretch look like an
    outage: at ~2 updates/hour the freshness window reset every few minutes."""
    import inspect

    from freshet.pipeline import embedder
    src = inspect.getsource(embedder.run)
    assert "idle_hook=lambda: _beat(heartbeat, conn)" in src, (
        "an idle embedder must still prove it is alive")
