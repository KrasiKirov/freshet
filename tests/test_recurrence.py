"""Recurrence is the one brief input that needs retrieval rather than a key.

Every other input comes back with `WHERE incident_id = %s`, which is correct
because the key is known. "Has this happened before?" has no key: the answer is
whichever past incident is semantically closest, so it goes through the retrieval
path the eval measures — and must be conservative, because inventing a link
between unrelated outages is worse than saying nothing.
"""
from datetime import UTC, datetime, timedelta

from freshet.autopilot.recurrence import Recurrence, recurrence_line

NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)


def _r(inc, days_ago, sim=0.9, title="Elevated errors"):
    return Recurrence(incident_id=inc, title=title, ts=NOW - timedelta(days=days_ago),
                      event_id=f"svc:{inc}:u1", similarity=sim)


def test_no_matches_produces_no_claim():
    assert recurrence_line([]) is None


def test_a_single_match_is_reported_singular_and_cited():
    line = recurrence_line([_r("INC-A", 30)])
    assert "1 similar prior incident," in line
    assert "[svc:INC-A:u1 @ 2026-07-20" in line, "the claim must carry a citation"


def test_several_matches_report_the_most_recent():
    line = recurrence_line([_r("INC-A", 60), _r("INC-B", 5), _r("INC-C", 30)])
    assert "3 similar prior incidents" in line
    assert "2026-08-14" in line, "most recent match, not the highest scoring"


def test_the_line_is_plain_text_a_slack_block_can_carry():
    line = recurrence_line([_r("INC-A", 2)])
    assert "\n" not in line and line.startswith("Recurrence:")


class _Conn:
    def __init__(self, mapping):
        self.mapping = mapping

    def execute(self, sql, params=None):
        rows = [(e, i, t) for e, (i, t) in self.mapping.items()]

        class _R:
            def fetchall(self_inner):
                return rows
        return _R()


class _Hit:
    def __init__(self, event_id, ts, similarity):
        self.event_id, self.ts, self.similarity = event_id, ts, similarity


def _patched_search(monkeypatch, hits):
    from freshet.rag import retrieval

    class _Res:
        def __init__(self, h):
            self.hits, self.abstained = h, False
    monkeypatch.setattr(retrieval, "hybrid_search", lambda *a, **k: _Res(hits))


def test_the_incident_is_never_its_own_precedent(monkeypatch):
    from freshet.autopilot import recurrence

    hits = [_Hit("svc:SELF:u1", NOW - timedelta(days=1), 0.95)]
    _patched_search(monkeypatch, hits)
    conn = _Conn({"svc:SELF:u1": ("SELF", "Same incident")})

    class _Emb:
        min_similarity = 0.7
    out = recurrence.find_recurrences(conn, _Emb(), service="svc", incident_id="SELF",
                                      query_text="errors", before=NOW)
    assert out == []


def test_later_incidents_are_not_recurrence(monkeypatch):
    from freshet.autopilot import recurrence

    hits = [_Hit("svc:LATER:u1", NOW + timedelta(days=1), 0.95)]
    _patched_search(monkeypatch, hits)
    conn = _Conn({"svc:LATER:u1": ("LATER", "Future incident")})

    class _Emb:
        min_similarity = 0.7
    out = recurrence.find_recurrences(conn, _Emb(), service="svc", incident_id="ME",
                                      query_text="errors", before=NOW)
    assert out == [], "an incident after this one is not a precedent"


def test_weak_matches_are_rejected_by_the_embedder_floor(monkeypatch):
    from freshet.autopilot import recurrence

    hits = [_Hit("svc:OTHER:u1", NOW - timedelta(days=5), 0.42)]
    _patched_search(monkeypatch, hits)
    conn = _Conn({"svc:OTHER:u1": ("OTHER", "Unrelated")})

    class _Emb:
        min_similarity = 0.7
    out = recurrence.find_recurrences(conn, _Emb(), service="svc", incident_id="ME",
                                      query_text="errors", before=NOW)
    assert out == [], "a weak match must not become a claimed recurrence"


def test_maintenance_notices_do_not_count_as_recurrence():
    """Measured on the live corpus: 5 of 6 sampled Cloudflare incidents reported
    'similar prior incidents' that were all routine datacenter maintenance."""
    from freshet.autopilot.recurrence import is_maintenance
    assert is_maintenance("SIN (Singapore) on 2026-08-19: We will be performing "
                          "scheduled maintenance")
    assert not is_maintenance("Increased Errors for Durable Objects in Hong Kong")


def test_a_maintenance_query_claims_nothing(monkeypatch):
    from freshet.autopilot import recurrence

    class _Emb:
        min_similarity = 0.7
    out = recurrence.find_recurrences(
        _Conn({}), _Emb(), service="cloudflare", incident_id="ME",
        query_text="ORD (Chicago): We will be performing scheduled maintenance",
        before=NOW)
    assert out == []
