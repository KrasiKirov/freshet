

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
