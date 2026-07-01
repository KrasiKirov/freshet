"""Hand-rolled LLM-as-judge (no framework). Two reference-free metrics:
faithfulness (are the answer's claims supported by the evidence?) and
answer-relevance (does the answer address the question?). Key-gated; the client is
injectable so tests run without a key. Fails loud — never fabricates a score."""

from __future__ import annotations

import os
import re

from freshet.api.retrieval import RetrievedHit

_FAITH_SYSTEM = (
    "You are a strict grader. Given an ANSWER and the EVIDENCE events it is meant to "
    "be based on, estimate the fraction of the answer's factual claims that are "
    "directly supported by the evidence. If the answer makes no checkable claims, "
    "reply 1. Reply with ONLY a number between 0 and 1."
)
_REL_SYSTEM = (
    "You are a strict grader. Rate how directly the ANSWER addresses the QUESTION, "
    "ignoring correctness. Reply with ONLY a number between 0 and 1."
)


def _model() -> str:
    return os.environ.get("FRESHET_LLM_MODEL", "claude-sonnet-4-6")


def _judge_client():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY not set; the LLM judge is key-gated")
    import anthropic
    return anthropic.Anthropic()


def _text(resp) -> str:
    return next((b.text for b in resp.content if b.type == "text"), "")


def _parse_score(text: str) -> float:
    m = re.search(r"\d*\.?\d+", text)
    if not m:
        raise ValueError(f"no score in judge response: {text!r}")
    return max(0.0, min(1.0, float(m.group(0))))


def _ask(system: str, user: str, client) -> float:
    client = client or _judge_client()
    resp = client.messages.create(
        model=_model(), max_tokens=16, system=system,
        messages=[{"role": "user", "content": user}],
    )
    return _parse_score(_text(resp))


def judge_faithfulness(answer: str, evidence: list[RetrievedHit], client=None) -> float:
    ev = "\n".join(
        f"[{h.event_id} @ {h.ts:%Y-%m-%d %H:%M:%S}] ({h.source}) {h.text}" for h in evidence
    )
    return _ask(_FAITH_SYSTEM, f"EVIDENCE:\n{ev}\n\nANSWER:\n{answer}\n\nFraction supported (0-1):", client)


def judge_answer_relevance(answer: str, question: str, client=None) -> float:
    return _ask(_REL_SYSTEM, f"QUESTION:\n{question}\n\nANSWER:\n{answer}\n\nRelevance (0-1):", client)
