from freshet.eval.batch_baseline import (
    batch_queryable_at,
    staleness_at,
    streaming_queryable_at,
)


def test_batch_queryable_at_rounds_up_to_next_interval():
    # events at 1, 10, 11 seconds; batch every 10s from t0=0
    # event at 1 -> queryable at 10; at 10 -> 10; at 11 -> 20
    assert batch_queryable_at([1.0, 10.0, 11.0], interval_s=10.0, t0=0.0) == [10.0, 10.0, 20.0]


def test_streaming_queryable_at_adds_freshness():
    assert streaming_queryable_at([1.0, 5.0], freshness_s=3.0) == [4.0, 8.0]


def test_staleness_at_query_time():
    ts = [0.0, 10.0]
    q_at = [10.0, 20.0]      # batch: first event queryable at 10, second at 20
    # at t=15: newest queryable event is ts=0 (queryable at 10); ts=10 not queryable until 20
    # staleness = 15 - 0 = 15
    assert staleness_at(15.0, ts, q_at) == 15.0
    # at t=25: newest queryable is ts=10 -> staleness = 25 - 10 = 15
    assert staleness_at(25.0, ts, q_at) == 15.0
    # before anything queryable -> None
    assert staleness_at(5.0, ts, q_at) is None
