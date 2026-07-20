from freshet.common.schemas import Event, EventSource, Severity
from freshet.pipeline.incidents import RESOLUTION_TYPES, correlate, incident_title, is_severe


def _ev(**kw) -> Event:
    base = {"service": "scheduler-api", "source": EventSource.ALERT, "type": "error_spike", "text": "x"}
    base.update(kw)
    return Event(**base)


class _FakeConn:
    """Routes correlate()'s SQL: claim/upsert/find/resolve, keyed on SQL text."""

    def __init__(self, *, inserted=False, resolves=True, open_incident=None,
                 claim_wins=True):
        self.inserted = inserted
        self.resolves = resolves
        self.open_incident = open_incident   # FIND_OPEN result (or None)
        self.claim_wins = claim_wins
        self.resolved = []
        self.claimed = []

    def execute(self, sql, params=None):
        row = None
        if "ON CONFLICT (primary_service)" in sql:      # atomic open claim
            if self.claim_wins:
                self.claimed.append(params["id"])
                row = (params["id"],)
        elif "INSERT INTO incidents" in sql:            # idempotent upsert
            row = (self.inserted,)
        elif "JOIN incident_services" in sql:            # FIND_OPEN
            row = (self.open_incident,) if self.open_incident else None
        elif "SET resolved_at" in sql and self.resolves:
            self.resolved.append(params["id"])
            row = (params["id"],)

        class _R:
            def __init__(self, r):
                self._r = r

            def fetchone(self):
                return self._r

        return _R(row)


def test_is_severe_by_type():
    assert is_severe(_ev(type="error_spike"))
    assert is_severe(_ev(type="latency_spike", source=EventSource.METRIC))
    assert is_severe(_ev(type="rollback", source=EventSource.DEPLOY))


def test_is_severe_by_severity():
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV1))
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV2))


def test_noise_is_not_severe():
    assert not is_severe(_ev(type="metric_sample", source=EventSource.METRIC))
    assert not is_severe(_ev(type="healthy", source=EventSource.ALERT))
    assert not is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV4))


def test_incident_title():
    assert incident_title(_ev(type="error_spike")) == "scheduler-api: error_spike"


def test_resolution_types_cover_synthetic_and_statuspage():
    # "healthy" is the generator's recovery event; "resolved"/"postmortem" are
    # Statuspage terminal statuses passed through by the status poller
    assert {"healthy", "resolved", "postmortem"} <= set(RESOLUTION_TYPES)
    assert "investigating" not in RESOLUTION_TYPES
    assert "monitoring" not in RESOLUTION_TYPES


def test_statuspage_resolved_event_resolves_incident():
    conn = _FakeConn()
    r = correlate(conn, _ev(type="resolved", incident_id="cloudflare:inc_1",
                            severity=Severity.SEV2))
    assert r.transition == "resolved"
    assert conn.resolved == ["cloudflare:inc_1"]


def test_statuspage_monitoring_event_does_not_resolve():
    conn = _FakeConn()
    r = correlate(conn, _ev(type="monitoring", incident_id="cloudflare:inc_1",
                            severity=Severity.SEV2))
    assert r.transition is None
    assert conn.resolved == []


def test_stray_severe_event_opens_via_atomic_claim():
    conn = _FakeConn(open_incident=None, claim_wins=True, inserted=False)
    r = correlate(conn, _ev(type="error_spike"))       # no incident_id
    assert r.transition == "opened"
    assert r.incident_id in conn.claimed               # claim created the row


def test_stray_severe_event_joins_existing_open_incident():
    conn = _FakeConn(open_incident="INC_open", inserted=False)
    r = correlate(conn, _ev(type="error_spike"))
    assert r.incident_id == "INC_open"
    assert r.transition is None                        # joined, not re-opened
    assert conn.claimed == []
