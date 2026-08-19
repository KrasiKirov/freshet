"""Pure rendering for the autopilot incident brief. Given normalized Findings,
produce a plain-text, cited brief. No I/O, no DB — trivially unit-testable.
Slack formatting lives in the Slack sink; the impact line is computed by the impact
heuristic (freshet/autopilot/impact.py) and rendered here when present."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class Findings:
    service: str
    status: str
    cause_text: str | None
    cause_cite: str | None
    fix_text: str | None
    fix_cite: str | None
    runbook: str | None
    narrative: str | None
    meta: str | None = None
    impact: str | None = None
    # Newest-first, pre-cited update lines. Status feeds carry no change events,
    # so cause/fix is often empty and this is the brief's actual content.
    updates: list[str] = field(default_factory=list)
    # "Has this happened before?" — the one input a key lookup cannot supply.
    recurrence: str | None = None


def cite_hit(hit) -> str:
    return f"[{hit.event_id} @ {hit.ts:%Y-%m-%d %H:%M:%S}]"



# Phrases where a provider actually NAMES a cause. Deliberately narrow: an
# update saying "we have identified the issue" announces progress, not a cause,
# and reporting it as one would be inventing content the provider never gave.
_CAUSE_MARKERS = (
    "caused by", "root cause", "due to", "as a result of",
    "identified the source of", "identified the cause of",
    "stemmed from", "triggered by", "resulted from",
)
# Sentences that mention a cause without giving one. Every entry below is a real
# string observed in the feeds, and each was surfaced as a "cause" before these
# filters existed:
#   - a promised future write-up ("a detailed root cause analysis will be shared")
#   - an announcement that the cause was FOUND, without naming it
#     ("we have identified the root cause and reverted the impacted change")
#   - a generic placeholder object ("identified the source of the issue")
_PROMISSORY = re.compile(
    r"will be (shared|provided|published|available)|will (follow|share|provide)"
    r"|as soon as it is available|analysis will|postmortem will"
    # "investigations are on-going into the root cause, and updates will
    # continue to be provided" — reports that work continues, names nothing
    r"|updates will|will continue", re.I)
_FOUND_BUT_UNNAMED = re.compile(
    # "identified the root cause and reverted..." — found it, never said what
    r"identified the (?:root cause|cause)(?!\s+of\s)"
    # "identified the source of the issue ..." — a placeholder, not a name
    r"|identified the (?:source|cause) of (?:the|this)\s",
    re.I)
_SENTENCE = re.compile(r"[^.!?]+[.!?]")

def _cause_sentence(text: str) -> str | None:
    """The single sentence in which a cause is stated, or None.

    Quoting one sentence keeps the brief honest and short: the surrounding
    apology and postmortem promise are not the cause.
    """
    flat = " ".join(text.split())
    for sentence in _SENTENCE.findall(flat):
        stripped = sentence.strip()
        lowered = stripped.lower()
        if not any(marker in lowered for marker in _CAUSE_MARKERS):
            continue
        if _PROMISSORY.search(stripped):
            continue          # a promise of an RCA is not an RCA
        if _FOUND_BUT_UNNAMED.search(stripped):
            continue          # "we found the cause" does not say what it was
        return stripped
    return None


def cause_from_updates(hits) -> tuple[str, str] | None:
    """Quote the provider's own words when an update states a cause.

    Returns (sentence, citation), or None when nothing states a cause — which is
    the common case on status feeds and must stay that way. The EARLIEST such
    statement wins: later updates tend to repeat it in summary form.
    """
    for hit in sorted(hits, key=lambda h: h.ts):
        sentence = _cause_sentence(hit.text)
        if sentence:
            return sentence, cite_hit(hit)
    return None


MAX_UPDATES = 4              # a Slack brief has to stay skimmable
_MAX_UPDATE_CHARS = 200


def findings_from_updates(service: str, status: str, hits,
                          runbook: str | None) -> Findings:
    """Brief the incident's own updates, newest first, each cited.

    Status feeds state what is happening in the provider's own words but contain
    no change events, so no cause can be derived from event types — and "no cause
    found" is not a useful brief on its own. This reports
    what the feed actually said, which is only possible because those updates
    were indexed seconds after being posted.
    """
    newest = sorted(hits, key=lambda h: h.ts, reverse=True)[:MAX_UPDATES]
    lines = []
    for hit in newest:
        text = " ".join(hit.text.split())
        if len(text) > _MAX_UPDATE_CHARS:
            text = text[:_MAX_UPDATE_CHARS].rstrip() + "..."
        lines.append(f"{hit.ts:%H:%M} — {text} {cite_hit(hit)}")
    return Findings(service=service, status=status, cause_text=None, cause_cite=None,
                    fix_text=None, fix_cite=None, runbook=runbook, narrative=None,
                    updates=lines)


def render_brief(f: Findings) -> str:
    title = "POSTMORTEM" if f.status == "resolved" else "INCIDENT BRIEF"
    lines = [f"=== {title} — {f.service} ({f.status}) ==="]
    # The summary is generated prose; the cause is a verbatim provider quote.
    # They are complementary, so both render — the narrative no longer replaces
    # the cause line the way it did when it was the only LLM output.
    if f.narrative:
        lines.append("")
        lines.append(f.narrative)
        lines.append("")
    if f.cause_text:
        lines.append(f"Cause: {f.cause_text} — {f.cause_cite}")
    elif not f.narrative:
        lines.append("Cause: not identified from retrieved evidence")
    if f.fix_text:
        lines.append(f"Resolution: {f.fix_text} — {f.fix_cite}")
    elif not f.narrative:
        lines.append("Resolution: not identified from retrieved evidence")
    if f.recurrence:
        lines.append(f.recurrence)
    if f.updates:
        lines.append("Updates:")
        lines.extend(f"  {line}" for line in f.updates)
    lines.append(f"Runbook: {f.runbook}" if f.runbook else "Runbook: none found")
    if f.meta:
        lines.append(f.meta)
    if f.impact:
        lines.append(f.impact)
    return "\n".join(lines)
