"""Grounded-answer composition behind a pluggable interface.

TemplateComposer is the keyless default: a deterministic, extractive answer that
cites every event. AnthropicComposer (optional, `pip install -e ".[llm]"`,
requires ANTHROPIC_API_KEY) writes a fluent grounded answer. The retrieval layer
decides abstention; a composer is only called when there is evidence to ground
in, so neither composer needs to invent a refusal.
"""

from __future__ import annotations

import os
from typing import Protocol

from freshet.api.retrieval import RetrievedHit

NO_EVIDENCE = "I don't have enough indexed evidence to answer that."


def _citation(h: RetrievedHit) -> str:
    return f"[{h.event_id} @ {h.ts:%Y-%m-%d %H:%M:%S}]"


class Composer(Protocol):
    def compose(self, question: str, hits: list[RetrievedHit]) -> str: ...


class TemplateComposer:
    """Deterministic, dependency-free, no API key. The default."""

    def compose(self, question: str, hits: list[RetrievedHit]) -> str:
        if not hits:
            return NO_EVIDENCE
        lines = [f"Most relevant events for {question!r}:"]
        for h in hits:
            lines.append(f"- {_citation(h)} ({h.source}) {h.text}")
        return "\n".join(lines)


_SYSTEM = (
    "You answer on-call engineers' questions using ONLY the operational events "
    "provided. Cite every claim with [event_id @ timestamp] exactly as given. Be "
    "concise and factual. If the events do not address the question, say so "
    "plainly. Respond only with the final answer — no preamble, no meta-commentary "
    "about your reasoning."
)


class AnthropicComposer:
    """Fluent grounded answers via the Anthropic API. Lazy-imports the SDK so the
    keyless core never depends on it. Model is FRESHET_LLM_MODEL or sonnet-4-6."""

    def __init__(self, model: str | None = None):
        import anthropic  # lazy: only when an Anthropic composer is actually built

        self._client = anthropic.Anthropic()
        self._model = model or os.environ.get("FRESHET_LLM_MODEL", "claude-sonnet-4-6")

    def compose(self, question: str, hits: list[RetrievedHit]) -> str:
        if not hits:
            return NO_EVIDENCE
        context = "\n".join(f"{_citation(h)} ({h.source}) {h.text}" for h in hits)
        # thinking omitted: grounded summarization is simple and we want a fast,
        # cheap demo answer. The final-answer-only line in _SYSTEM prevents Opus
        # 4.8 from leaking reasoning into the response when thinking is off.
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"Question: {question}\n\nEvents:\n{context}",
            }],
        )
        return next((b.text for b in resp.content if b.type == "text"), "")


def make_composer(kind: str = "auto") -> Composer:
    """`template` | `anthropic` | `auto`. auto picks Anthropic only when a key is
    present and the SDK import + client construction succeed, else template."""
    if kind == "template":
        return TemplateComposer()
    if kind == "anthropic":
        return AnthropicComposer()
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            return AnthropicComposer()
        except Exception:
            return TemplateComposer()
    return TemplateComposer()
