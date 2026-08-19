"""Eval queries are real provider text, not generated questions.

Generated questions put fabricated specifics into the benchmark: 7 of 64
paraphrases named a product the incident never involved. Each incident's own
first update is the symptom, written by the provider.
"""
from freshet.eval.label_live import MAX_QUERY_CHARS, first_update_query, strip_title


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        class _R:
            def __init__(self, r):
                self.r = r
            def fetchall(self_inner):
                return self_inner.r
        return _R(self.rows)


def _row(event_id, ts, title, text):
    return (event_id, ts, title, text)


def test_the_title_prefix_is_stripped():
    assert strip_title("Big Outage: api is down", "Big Outage") == "api is down"
    assert strip_title("no prefix here", "Other") == "no prefix here"
    assert strip_title("text", None) == "text"


def test_the_earliest_update_becomes_the_query():
    conn = _Conn([_row("e2", 2, "T", "T: later text"),
                  _row("e1", 1, "T", "T: users report 500s")])
    assert first_update_query(conn, "inc", set()) == ("users report 500s", "e1")


def test_the_cause_update_is_never_used_as_its_own_query():
    """Quoting the answer to find the answer measures nothing."""
    conn = _Conn([_row("cause", 1, "T", "T: caused by a bad deploy"),
                  _row("e2", 2, "T", "T: users report 500s")])
    q, eid = first_update_query(conn, "inc", {"cause"})
    assert eid == "e2" and q == "users report 500s"


def test_an_incident_whose_only_update_is_the_cause_yields_nothing():
    conn = _Conn([_row("cause", 1, "T", "T: caused by a bad deploy")])
    assert first_update_query(conn, "inc", {"cause"}) is None


def test_a_long_first_update_is_capped_to_a_symptom_length():
    conn = _Conn([_row("e1", 1, "T", "T: " + "x" * 5000)])
    q, _ = first_update_query(conn, "inc", set())
    assert len(q) == MAX_QUERY_CHARS


def test_blank_updates_are_skipped():
    conn = _Conn([_row("e1", 1, "T", "T:   "), _row("e2", 2, "T", "T: real symptom")])
    assert first_update_query(conn, "inc", set()) == ("real symptom", "e2")
