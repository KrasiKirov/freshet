

def test_idle_hook_runs_when_a_poll_returns_nothing(monkeypatch):
    """Deferred work (draining due briefs) happens off the message path, so a
    debounce window never blocks the partition."""
    from freshet.common import kafka_io

    class _Msg:
        def error(self): return None
        def value(self): return b'{"x":1}'
        def topic(self): return "t"
        def partition(self): return 0

    polls = [None, None, _Msg()]

    class _Consumer:
        def poll(self, _t): return polls.pop(0) if polls else None
        def commit(self, **k): pass
        def close(self): pass

    ticks = []
    # monkeypatch, not assignment: a bare assignment leaks the fake into
    # every later test in the session.
    monkeypatch.setattr(kafka_io, "make_consumer", lambda *a, **k: _Consumer())
    n = kafka_io.consume_loop("b", "g", ["t"], lambda v: None, max_messages=1,
                              idle_hook=lambda: ticks.append(1))
    assert n == 1
    assert len(ticks) == 2, "the hook must fire on every empty poll"


def test_after_handler_runs_before_the_offset_commits(monkeypatch):
    from freshet.common import kafka_io

    class _Msg:
        def error(self): return None
        def value(self): return b'{"x":1}'
        def topic(self): return "t"
        def partition(self): return 0

    class _Consumer:
        def __init__(self):
            self.committed = 0
        def poll(self, _t):
            return _Msg() if self.committed == 0 else None
        def commit(self, **k):
            self.committed += 1
        def close(self): pass

    c = _Consumer()
    order = []
    monkeypatch.setattr(kafka_io, "make_consumer", lambda *a, **k: c)
    kafka_io.consume_loop(
        "b", "g", ["t"], lambda v: order.append("h"),
        max_messages=1, auto_commit=False,
        after_handler=lambda: order.append("d"),
    )
    assert order == ["h", "d"]
    assert c.committed >= 1


def _capture_consumer_config(monkeypatch) -> dict:
    """Intercept the librdkafka Consumer constructor and return its config."""
    import confluent_kafka

    captured: dict = {}

    class _Consumer:
        def __init__(self, conf):
            captured.update(conf)

        def subscribe(self, topics):
            pass

    monkeypatch.setattr(confluent_kafka, "Consumer", _Consumer)
    return captured


def test_the_consumer_tolerates_a_slow_batch_without_being_ejected(monkeypatch):
    """librdkafka ejects a consumer that has not polled within
    max.poll.interval.ms (its default is 300s). The embedder's per-message work
    is unbounded, and being ejected is strictly worse than being slow: the
    rebalance halts progress, and the heartbeat gap it opens RESETS the freshness
    run window — so a slow embedder destroys the evidence that it was slow.
    Observed in production: one fetch stalled 566s and the consumer left the
    group."""
    from freshet.common import kafka_io

    captured = _capture_consumer_config(monkeypatch)
    kafka_io.make_consumer("localhost:9092", "g", ["t"])
    assert captured["max.poll.interval.ms"] >= 600_000, \
        "must exceed the observed 566s stall with headroom"


def test_the_poll_interval_is_overridable(monkeypatch):
    from freshet.common import kafka_io

    captured = _capture_consumer_config(monkeypatch)
    monkeypatch.setenv("FRESHET_MAX_POLL_INTERVAL_MS", "120000")
    kafka_io.make_consumer("localhost:9092", "g", ["t"])
    assert captured["max.poll.interval.ms"] == 120_000


def test_a_junk_override_falls_back_to_the_default(monkeypatch):
    from freshet.common import kafka_io

    captured = _capture_consumer_config(monkeypatch)
    monkeypatch.setenv("FRESHET_MAX_POLL_INTERVAL_MS", "not-a-number")
    kafka_io.make_consumer("localhost:9092", "g", ["t"])
    assert captured["max.poll.interval.ms"] == kafka_io.DEFAULT_MAX_POLL_INTERVAL_MS
