"""Integration: correlate() reports open/resolve transitions exactly once."""
import uuid

import pytest

from freshet.common.schemas import Event, EventSource, Severity
from freshet.pipeline.incidents import correlate

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def _ev(iid, **kw):
    base = {"service": "scheduler-api", "source": EventSource.ALERT, "type": "error_spike",
            "text": "x", "incident_id": iid}
    base.update(kw)
    return Event(**base)


def test_statuspage_style_incident_resolves(conn):
    """Real status-feed events carry the raw Statuspage status as their type;
    a 'resolved' update must close the incident (not just 'healthy')."""
    iid = f"cloudflare:inc_{uuid.uuid4().hex[:8]}"
    r1 = correlate(conn, _ev(iid, service="cloudflare", type="investigating",
                             severity=Severity.SEV2))
    assert r1.transition == "opened"
    services = {r[0] for r in conn.execute(
        "SELECT service FROM incident_services WHERE incident_id = %s", (iid,)
    ).fetchall()}
    assert services == {"cloudflare"}
    r2 = correlate(conn, _ev(iid, service="cloudflare", type="resolved",
                             severity=Severity.SEV2))
    assert r2.transition == "resolved"
    # redelivered 'resolved' update is a no-op
    r3 = correlate(conn, _ev(iid, service="cloudflare", type="resolved",
                             severity=Severity.SEV2))
    assert r3.transition is None


def test_open_then_resolve_transitions_fire_once(conn):
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    # first severe event opens the incident
    r1 = correlate(conn, _ev(iid, severity=Severity.SEV1))
    assert r1.incident_id == iid and r1.transition == "opened"
    services = {r[0] for r in conn.execute(
        "SELECT service FROM incident_services WHERE incident_id = %s", (iid,)
    ).fetchall()}
    assert services == {"scheduler-api"}
    # a second severe event does NOT re-open
    r2 = correlate(conn, _ev(iid, severity=Severity.SEV1))
    assert r2.transition is None
    # a healthy event carrying the incident_id resolves it once
    r3 = correlate(conn, _ev(iid, type="healthy", severity=Severity.SEV4))
    assert r3.transition == "resolved"
    # resolving again is a no-op transition
    r4 = correlate(conn, _ev(iid, type="healthy", severity=Severity.SEV4))
    assert r4.transition is None
