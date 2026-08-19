from datetime import UTC, datetime

from freshet.common.incidents import ENSURE_INCIDENT_SQL, ENSURE_SERVICE_SQL, ensure_incident


class _FakeConn:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


def test_ensure_incident_inserts_row_then_service_join():
    conn = _FakeConn()
    opened = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    ensure_incident(conn, "INC-1", "cloudflare", opened, "Elevated errors")
    assert len(conn.executed) == 2
    sql0, p0 = conn.executed[0]
    assert sql0 == ENSURE_INCIDENT_SQL
    assert p0 == ("INC-1", "Elevated errors", opened, "cloudflare")
    sql1, p1 = conn.executed[1]
    assert sql1 == ENSURE_SERVICE_SQL
    assert p1 == ("INC-1", "cloudflare")


def test_ensure_incident_skips_when_id_missing():
    conn = _FakeConn()
    ensure_incident(conn, None, "cloudflare", datetime.now(UTC), "t")
    assert conn.executed == []


class _RecordingConn:
    """Records SQL so the embedder's ensure call can be asserted without a DB."""
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

        class _R:
            def fetchone(self_inner):
                return None
        return _R()


def test_the_embedder_creates_the_incident_row_as_it_indexes():
    """Without this, Autopilot's claim matches no row and the brief never fires."""
    from datetime import UTC, datetime

    from freshet.common.schemas import Event, EventSource
    from freshet.pipeline.embedder import make_handler

    class _Emb:
        name = "stub"

        def encode(self, texts):
            return [[0.0] * 768 for _ in texts]

    ev = Event(event_id="cloudflare:inc9:u1", incident_id="inc9", service="cloudflare",
               source=EventSource.ALERT, type="status_update", ts=datetime.now(UTC),
               text="Elevated errors: we are investigating", title="Elevated errors")
    conn = _RecordingConn()
    make_handler(conn, _Emb(), producer=None)(ev.model_dump_json())

    ensures = [(s, p) for s, p in conn.executed if s == ENSURE_INCIDENT_SQL]
    assert len(ensures) == 1, "exactly one incidents upsert per indexed event"
    assert ensures[0][1][0] == "inc9"
    assert ensures[0][1][3] == "cloudflare"
    assert any(s == ENSURE_SERVICE_SQL for s, _ in conn.executed)
    # ordering matters: a claimable row must not exist before its evidence does
    assert conn.executed.index(ensures[0]) > 0


def test_an_event_without_an_incident_id_creates_no_row():
    conn = _FakeConn()
    ensure_incident(conn, None, "cloudflare", datetime(2026, 8, 19, tzinfo=UTC), "t")
    assert conn.executed == []
