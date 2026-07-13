import hashlib
import hmac
import json
from pathlib import Path

from freshet.connectors.github import GitHubConnector

FIX = Path("freshet/connectors/fixtures/github")


def _load(name):
    return json.loads((FIX / name).read_text())


def test_event_type_from_header():
    gh = GitHubConnector()
    assert gh.event_type({"X-GitHub-Event": "push"}) == "push"


def test_parse_push_maps_to_commit_event():
    gh = GitHubConnector()
    events = gh.parse("push", _load("push.json"))
    assert len(events) == 1
    ev = events[0]
    assert ev.type == "commit"
    assert ev.service  # repo name
    # sha7 + author + message all present in the cited text
    assert "a1b2c3d" in ev.text and "alice" in ev.text.lower() and "pool" in ev.text.lower()


def test_parse_deployment_maps_to_deploy_started():
    gh = GitHubConnector()
    events = gh.parse("deployment", _load("deployment.json"))
    assert len(events) == 1 and events[0].type == "deploy_started"


def test_unmapped_event_type_is_empty():
    assert GitHubConnector().parse("star", {}) == []


def test_verify_skips_when_secret_unset(monkeypatch):
    monkeypatch.delenv("FRESHET_GITHUB_WEBHOOK_SECRET", raising=False)
    assert GitHubConnector().verify({}, b"{}") is True


def test_verify_accepts_good_and_rejects_tampered(monkeypatch):
    monkeypatch.setenv("FRESHET_GITHUB_WEBHOOK_SECRET", "s3cr3t")
    body = b'{"hello":"world"}'
    sig = "sha256=" + hmac.new(b"s3cr3t", body, hashlib.sha256).hexdigest()
    gh = GitHubConnector()
    assert gh.verify({"X-Hub-Signature-256": sig}, body) is True
    assert gh.verify({"X-Hub-Signature-256": sig}, b'{"hello":"tampered"}') is False
    assert gh.verify({}, body) is False  # missing signature with secret set
