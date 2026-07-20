"""Integration: an opened incident on the stream yields exactly one cited brief,
keyless. Exercises correlate→lifecycle→claim→gather→render end to end."""
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_open_incident_briefs_once(conn, emb, monkeypatch, capsys):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.autopilot.consumer import handle_lifecycle
    from freshet.autopilot.sinks.stdout import StdoutSink
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.lifecycle import LifecycleEvent

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    # simulate the incident being opened + present in the incidents table
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at)"
        " VALUES (%s, %s, now())",
        (iid, f"{truth.service}: open"),
    )
    raw = LifecycleEvent("opened", iid, truth.service, "2026-07-01T00:00:00+00:00").to_json()

    # first handle → briefs
    handle_lifecycle(conn, emb, raw, window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out1 = capsys.readouterr().out
    assert "INCIDENT BRIEF" in out1 and truth.cause_id in out1

    # second handle (redelivery) → claim lost → no second brief
    handle_lifecycle(conn, emb, raw, window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out2 = capsys.readouterr().out
    assert "already briefed" in out2.lower() and "INCIDENT BRIEF" not in out2


def test_resolve_posts_postmortem_once(conn, emb, monkeypatch, capsys):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.autopilot.consumer import handle_lifecycle
    from freshet.autopilot.sinks.stdout import StdoutSink
    from freshet.eval.run_eval import index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.lifecycle import LifecycleEvent

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    index_corpus(conn, emb, corpus)
    truth = truths[0]

    # briefed_at set: the incident went through the normal opened→brief flow
    # before resolving (the postmortem claim requires it).
    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at, briefed_at,"
        " resolved_at, resolution_summary) VALUES (%s, %s,"
        " now() - interval '30 minutes', now() - interval '29 minutes', now(), %s)",
        (iid, f"{truth.service}: resolved", "rolled back"),
    )
    raw = LifecycleEvent("resolved", iid, truth.service, "2026-07-01T00:00:00+00:00").to_json()

    handle_lifecycle(conn, emb, raw, window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out1 = capsys.readouterr().out
    assert "POSTMORTEM" in out1 and truth.service in out1

    # redelivery → postmortem already posted → no second postmortem
    handle_lifecycle(conn, emb, raw, window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out2 = capsys.readouterr().out
    assert "already" in out2.lower() and "POSTMORTEM" not in out2


def test_resolve_without_brief_skips_postmortem(conn, emb, monkeypatch, capsys):
    """A resolved incident that was never briefed (e.g. a historical incident
    replayed on the first status-feed poll) must not trigger a postmortem."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.autopilot.consumer import handle_lifecycle
    from freshet.autopilot.sinks.stdout import StdoutSink
    from freshet.pipeline.lifecycle import LifecycleEvent

    iid = f"INC_{uuid.uuid4().hex[:12]}"
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at, resolved_at,"
        " resolution_summary) VALUES (%s, %s,"
        " now() - interval '30 minutes', now(), %s)",
        (iid, "api: resolved", "rolled back"),
    )
    raw = LifecycleEvent("resolved", iid, "api", "2026-07-01T00:00:00+00:00").to_json()

    handle_lifecycle(conn, emb, raw, window_s=0, sink=StdoutSink(), sleep=lambda s: None)
    out = capsys.readouterr().out
    assert "never briefed" in out and "POSTMORTEM" not in out
