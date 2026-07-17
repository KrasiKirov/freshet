"""Integration: a commit event + a matching error spike on the same service, run
through retrieval + the timeline, yields a cause that cites the commit SHA. Keyless."""
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_brief_cause_cites_commit_sha(conn, emb, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from freshet.common.schemas import Event, EventSource, Severity
    from freshet.connectors.github import GitHubConnector
    from freshet.eval.run_eval import index_corpus
    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline

    svc = f"svc-{uuid.uuid4().hex[:8]}"
    push = json.loads((Path("freshet/connectors/fixtures/github/push.json")).read_text())
    push["repository"]["name"] = svc
    t0 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    push["head_commit"]["timestamp"] = t0.isoformat()

    commit_ev = GitHubConnector().parse("push", push)[0]
    spike = Event(ts=t0 + timedelta(minutes=5), service=svc, source=EventSource.ALERT,
                  type="error_spike", severity=Severity.SEV2,
                  text=f"5xx error rate on {svc} crossed 5% (now 20%)")

    index_corpus(conn, emb, [commit_ev, spike])
    res = hybrid_search(conn, emb,
                        f"what caused the {svc} incident?", k=12, service=svc)
    tl = build_timeline(res.hits)
    assert tl.cause is not None
    assert "a1b2c3d" in tl.cause.text and tl.cause.type == "commit"
