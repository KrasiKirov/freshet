"""Two tables accumulated rows nothing ever removed. `llm_budget` has had a
prune() all along — with no caller anywhere, so the 7-day retention its
docstring advertises had never once run."""
from freshet.autopilot.maintenance import Maintenance


class _Conn:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        return self


class _Composer:
    def __init__(self):
        self.pruned = 0

    def prune(self):
        self.pruned += 1


def test_a_maintenance_pass_prunes_both_tables():
    conn, composer = _Conn(), _Composer()
    assert Maintenance(interval_s=3600.0, now=lambda: 0.0)(conn, composer)
    assert composer.pruned == 1
    assert any("pipeline_heartbeat_log" in s for s in conn.sql)


def test_maintenance_is_throttled():
    conn, composer = _Conn(), _Composer()
    maintain = Maintenance(interval_s=3600.0, now=lambda: 0.0)
    assert maintain(conn, composer) is True
    assert maintain(conn, composer) is False, "a second pass inside the interval"
    assert composer.pruned == 1


def test_a_failing_maintenance_pass_never_stops_the_loop():
    class _Boom:
        def prune(self):
            raise RuntimeError("db is down")

    assert Maintenance()(_Conn(), _Boom()) is False


def test_a_composer_without_prune_is_tolerated():
    """The stdout demo path injects a plain composer."""
    class _Plain:
        pass

    conn = _Conn()
    assert Maintenance()(conn, _Plain()) is True
    assert any("pipeline_heartbeat_log" in s for s in conn.sql)
