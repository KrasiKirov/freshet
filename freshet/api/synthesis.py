"""Keyless extractive root-cause timeline. Given retrieved hits for an incident,
select and order the load-bearing events and identify the cause (the deploy
preceding the first error spike) and the fix (the rollback) — a structured, cited
partial postmortem with no LLM. The narrative layer is M10b."""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from freshet.api.retrieval import RetrievedHit
from freshet.common.schemas import CHANGE_TYPES, REMEDIATION_TYPES

_ROLE_BY_TYPE = {
    "deploy_started": "deploy", "deploy_finished": "deploy",
    "error_spike": "spike", "latency_spike": "spike",
    "rollback": "rollback", "message": "chat", "rca": "postmortem",
    "healthy": "recovery", "runbook": "runbook",
    "config_changed": "deploy", "config_reverted": "rollback",
    "dependency_down": "spike", "dependency_failover": "rollback",
    "memory_leak_shipped": "deploy", "scaled_up": "rollback",
    "cert_expired": "spike", "cert_renewed": "rollback",
    "migration_applied": "deploy", "migration_reverted": "rollback",
    "commit": "commit",
}

# A code commit is a cause candidate too, but "commit" is connector-sourced (not an
# archetype change type), so keep it out of the shared CHANGE_TYPES and widen only
# the timeline's cause selection here.
_CAUSE_TYPES = CHANGE_TYPES | frozenset({"commit"})


def _role(hit: RetrievedHit) -> str:
    return _ROLE_BY_TYPE.get(hit.type, "other")


@dataclass
class TimelineEntry:
    role: str
    hit: RetrievedHit


@dataclass
class Timeline:
    service: Optional[str]
    entries: list[TimelineEntry] = field(default_factory=list)
    cause: Optional[RetrievedHit] = None
    fix: Optional[RetrievedHit] = None

    def render(self) -> str:
        if not self.entries:
            return "_Insufficient evidence to reconstruct the incident._"

        def cite(h: RetrievedHit) -> str:
            return f"`[{h.event_id} @ {h.ts:%Y-%m-%d %H:%M}]`"

        lines = [f"## Root cause — {self.service or 'incident'}", ""]
        lines.append(f"**Cause:** {self.cause.text} — {cite(self.cause)}"
                     if self.cause else "**Cause:** not identified from retrieved evidence")
        lines.append(f"**Resolution:** {self.fix.text} — {cite(self.fix)}"
                     if self.fix else "**Resolution:** not identified from retrieved evidence")
        lines += ["", "**Timeline:**"]
        for e in self.entries:
            lines.append(f"- {e.role}: {e.hit.text} — {cite(e.hit)}")
        return "\n".join(lines)


def _select_cause(candidates: list[RetrievedHit], hits: list[RetrievedHit],
                  first_spike_ts) -> Optional[RetrievedHit]:
    """Pick the cause among candidate changes at/before the spike. Blends retrieval
    RANK (position in `hits`, which reranking reorders — lower is better) with temporal
    proximity to the spike. Falls back to recency (latest) when rank is uninformative,
    so a single candidate — or synthetic hits with no meaningful order — reproduces the
    old `changes_before[-1]` behavior exactly."""
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    rank = {h.event_id: i for i, h in enumerate(hits)}
    n = max(len(hits), 1)

    def relevance(h: RetrievedHit) -> float:
        return 1.0 / (1 + rank.get(h.event_id, n))          # rank 0 -> 1.0

    def proximity(h: RetrievedHit) -> float:
        if first_spike_ts is None:
            return 1.0
        mins = max((first_spike_ts - h.ts).total_seconds(), 0.0) / 60.0
        return 1.0 / (1.0 + mins)                           # closer to spike -> higher

    # If every candidate shares the same rank (uninformative retrieval order), fall
    # back to pure recency — preserves prior behavior on non-ranked synthetic inputs.
    if len({rank.get(h.event_id, n) for h in candidates}) == 1:
        return candidates[-1]
    return max(candidates, key=lambda h: relevance(h) * proximity(h))


def build_timeline(hits: list[RetrievedHit]) -> Timeline:
    """Build the timeline for the dominant incident among the hits."""
    incident_hits = [h for h in hits if _role(h) != "runbook"]
    if not incident_hits:
        return Timeline(service=None)

    service = Counter(h.service for h in incident_hits).most_common(1)[0][0]
    focus = sorted((h for h in incident_hits if h.service == service), key=lambda h: h.ts)
    entries = [TimelineEntry(role=_role(h), hit=h) for h in focus]

    first_spike_ts = next((h.ts for h in focus if _role(h) == "spike"), None)
    changes_before = [h for h in focus if h.type in _CAUSE_TYPES
                      and (first_spike_ts is None or h.ts <= first_spike_ts)]
    cause = _select_cause(changes_before, hits, first_spike_ts)
    fix = next((h for h in focus if h.type in REMEDIATION_TYPES), None)
    return Timeline(service=service, entries=entries, cause=cause, fix=fix)


_NARRATIVE_SYSTEM = (
    "You explain software incidents to on-call engineers using ONLY the events "
    "provided. Write a concise causal narrative — what changed, what broke, and how "
    "it was resolved — in 2-4 sentences. Cite each claim with [event_id @ timestamp] "
    "exactly as given. Do not state anything not supported by the events. Respond "
    "with only the narrative — no preamble."
)


def _timeline_evidence(timeline: "Timeline") -> str:
    return "\n".join(
        f"[{e.hit.event_id} @ {e.hit.ts:%Y-%m-%d %H:%M:%S}] ({e.role}) {e.hit.text}"
        for e in timeline.entries
    )


def synthesize_narrative(timeline: "Timeline", client=None, model=None) -> str:
    """Optional, key-gated: write a grounded causal narrative over the timeline.
    An empty timeline abstains with no LLM call (keyless-safe). `client` is an
    Anthropic-style client (injected in tests); if None it is built lazily and
    requires the SDK + ANTHROPIC_API_KEY (fail loud)."""
    if not timeline.entries:
        return timeline.render()
    if client is None:
        import anthropic
        client = anthropic.Anthropic()
    model = model or os.environ.get("FRESHET_LLM_MODEL", "claude-sonnet-4-6")
    resp = client.messages.create(
        model=model,
        max_tokens=512,
        system=_NARRATIVE_SYSTEM,
        messages=[{
            "role": "user",
            "content": f"Incident events:\n{_timeline_evidence(timeline)}\n\n"
                       f"Write the causal narrative.",
        }],
    )
    return next((b.text for b in resp.content if b.type == "text"), "")
