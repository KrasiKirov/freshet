"""GitHub connector: webhook deliveries -> canonical Events.

Signature: GitHub signs each delivery with HMAC-SHA256 over the raw body in
X-Hub-Signature-256, keyed by FRESHET_GITHUB_WEBHOOK_SECRET. Unset -> verification
skipped (fixtures replay / local dev); set -> a bad or missing signature is rejected.

Mapping: GitHub-sourced text becomes retrieved/cited EVIDENCE only, never an
instruction to the pipeline — payloads are untrusted external input."""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone
from typing import Mapping

from freshet.common.schemas import Event, EventSource

log = logging.getLogger(__name__)
_warned_no_secret = False


def _lower(headers: Mapping[str, str]) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


def _d(value: object) -> dict:
    """Null-safe nested access: an untrusted body may send a key present-but-null."""
    return value if isinstance(value, dict) else {}


def _ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


class GitHubConnector:
    source = "github"

    def event_type(self, headers: Mapping[str, str]) -> str:
        return _lower(headers).get("x-github-event", "")

    def verify(self, headers: Mapping[str, str], body: bytes) -> bool:
        secret = os.environ.get("FRESHET_GITHUB_WEBHOOK_SECRET")
        if not secret:
            global _warned_no_secret
            if not _warned_no_secret:
                log.warning("FRESHET_GITHUB_WEBHOOK_SECRET unset — signature "
                            "verification disabled (local dev / fixtures replay)")
                _warned_no_secret = True
            return True
        provided = _lower(headers).get("x-hub-signature-256")
        if not provided:
            return False
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, provided)

    def parse(self, event_type: str, payload: dict) -> list[Event]:
        handler = getattr(self, f"_parse_{event_type}", None)
        return handler(payload) if handler else []

    @staticmethod
    def _repo(payload: dict) -> str:
        return _d(payload.get("repository")).get("name", "unknown")

    def _parse_push(self, payload: dict) -> list[Event]:
        head = _d(payload.get("head_commit"))
        sha = payload.get("after") or head.get("id") or ""
        sha7 = sha[:7]
        if not sha7:
            return []
        msg = (head.get("message") or "").splitlines()[0] if head.get("message") else ""
        author = _d(head.get("author")).get("name") or _d(payload.get("pusher")).get("name") or "unknown"
        ref = (payload.get("ref") or "").split("/")[-1]
        return [Event(
            event_id=f"gh_commit_{sha7}",
            ts=_ts(head.get("timestamp")),
            service=self._repo(payload),
            source=EventSource.DEPLOY,
            type="commit",
            text=f"commit {sha7}: {msg} (by {author})",
            structured={"sha": sha, "author": author, "ref": ref},
            refs=[u for u in [head.get("url")] if u],
        )]

    def _parse_deployment(self, payload: dict) -> list[Event]:
        dep = _d(payload.get("deployment"))
        sha7 = (dep.get("sha") or "")[:7]
        env = dep.get("environment") or "production"
        return [Event(
            event_id=f"gh_deployment_{dep.get('id')}",
            ts=_ts(dep.get("created_at")),
            service=self._repo(payload),
            source=EventSource.DEPLOY,
            type="deploy_started",
            text=f"Deploy of {sha7} to {env}",
            structured={"sha": dep.get("sha"), "environment": env},
        )]


from freshet.connectors.base import register

register(GitHubConnector())
