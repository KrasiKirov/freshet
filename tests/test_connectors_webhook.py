import json
from pathlib import Path

from fastapi.testclient import TestClient

import freshet.connectors.webhook as wh

FIX = Path("freshet/connectors/fixtures/github")


def _client(monkeypatch):
    produced = []
    monkeypatch.setattr(wh, "_get_producer", lambda: object())
    monkeypatch.setattr(wh, "produce_sync", lambda producer, topic, value, key=None: produced.append((topic, value)))
    return TestClient(wh.app), produced


def test_push_produces_a_commit_event(monkeypatch):
    monkeypatch.delenv("FRESHET_GITHUB_WEBHOOK_SECRET", raising=False)
    client, produced = _client(monkeypatch)
    body = (FIX / "push.json").read_text()
    r = client.post("/webhook/github", content=body, headers={"X-GitHub-Event": "push"})
    assert r.status_code == 200
    assert len(produced) == 1
    topic, value = produced[0]
    assert topic == "raw.events" and json.loads(value)["type"] == "commit"


def test_unknown_source_404(monkeypatch):
    client, _ = _client(monkeypatch)
    assert client.post("/webhook/gitlab", content="{}", headers={}).status_code == 404


def test_bad_signature_401(monkeypatch):
    monkeypatch.setenv("FRESHET_GITHUB_WEBHOOK_SECRET", "s3cr3t")
    client, produced = _client(monkeypatch)
    r = client.post("/webhook/github", content="{}",
                    headers={"X-GitHub-Event": "push", "X-Hub-Signature-256": "sha256=bad"})
    assert r.status_code == 401 and produced == []
