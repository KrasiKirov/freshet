"""Durable follow-ups survive failures without another Kafka lifecycle event."""
from uuid import uuid4

import pytest

from freshet.autopilot import consumer
from freshet.autopilot.brief import Findings

pytestmark = pytest.mark.integration


@pytest.fixture
def pending(conn, request):
    kind = request.param
    iid = f"recovery:{uuid4().hex}"
    conn.execute(
        "INSERT INTO incidents (incident_id, opened_at, primary_service, brief_delivered_at,"
        " progress_needed, postmortem_needed, slack_ts) VALUES (%s, now(), 'test', now(),"
        " %s, %s, 'thread-1')", (iid, kind == "progress", kind == "postmortem"))
    yield iid, kind
    conn.execute("DELETE FROM incidents WHERE incident_id = %s", (iid,))


class Sink:
    def __init__(self, fail=False):
        self.fail = fail
        self.posts = []

    def deliver(self, findings, *, thread=None):
        if self.fail:
            raise RuntimeError("Slack unavailable")
        self.posts.append((findings.status, thread))


@pytest.fixture(autouse=True)
def summaries(monkeypatch):
    def finding(status):
        return Findings("test", status, None, None, None, None, None, "summary")
    monkeypatch.setattr(consumer, "gather_findings", lambda *a, **k: finding("in_progress"))
    monkeypatch.setattr(consumer, "gather_postmortem", lambda *a, **k: finding("resolved"))


@pytest.mark.parametrize("pending", ["progress", "postmortem"], indirect=True)
def test_failure_retries_without_a_due_opening_brief(conn, pending):
    iid, kind = pending
    with pytest.raises(RuntimeError, match="Slack unavailable"):
        consumer.drain_pending_followups(conn, sink=Sink(fail=True))
    row = conn.execute(
        f"SELECT {kind}_needed, {kind}_at, {kind}_delivered_at, brief_due_at"
        " FROM incidents WHERE incident_id = %s", (iid,)).fetchone()
    assert row == (True, None, None, None)
    sink = Sink()
    consumer.drain_due_briefs(conn, sink=sink)
    assert sink.posts == [("in_progress" if kind == "progress" else "resolved", "thread-1")]
    consumer.drain_due_briefs(conn, sink=sink)
    assert len(sink.posts) == 1


@pytest.mark.parametrize("pending", ["progress", "postmortem"], indirect=True)
def test_crashed_claim_waits_for_lease_expiry_then_recovers(conn, pending):
    iid, kind = pending
    claim = (consumer._CLAIM_DEFERRED_PROGRESS_SQL if kind == "progress"
             else consumer._CLAIM_DEFERRED_POSTMORTEM_SQL)
    assert conn.execute(claim, (iid,)).fetchone()
    assert conn.execute(claim, (iid,)).fetchone() is None
    sink = Sink()
    consumer.drain_pending_followups(conn, sink=sink)
    assert not sink.posts
    conn.execute(
        f"UPDATE incidents SET {kind}_at = now() - interval '16 minutes'"
        " WHERE incident_id = %s", (iid,))
    consumer.drain_pending_followups(conn, sink=sink)
    assert len(sink.posts) == 1


@pytest.mark.parametrize("pending", ["progress", "postmortem"], indirect=True)
def test_budget_exhaustion_preserves_pending_work(conn, pending, monkeypatch):
    from freshet.rag.budget import BudgetExhausted

    iid, kind = pending
    target = "gather_findings" if kind == "progress" else "gather_postmortem"
    def exhausted(*args, **kwargs):
        raise BudgetExhausted("hourly cap")
    monkeypatch.setattr(consumer, target, exhausted)
    assert consumer.drain_pending_followups(conn, sink=Sink()) == 0
    assert conn.execute(
        f"SELECT {kind}_needed, {kind}_at FROM incidents WHERE incident_id = %s",
        (iid,)).fetchone() == (True, None)
