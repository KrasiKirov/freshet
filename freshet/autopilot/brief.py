"""Pure rendering for the autopilot incident brief. Given normalized Findings,
produce a plain-text, cited brief. No I/O, no DB — trivially unit-testable.
Slack formatting lives in the Slack sink; the impact line is computed by the impact
heuristic (freshet/autopilot/impact.py) and rendered here when present."""

from __future__ import annotations

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


def cite_hit(hit) -> str:
    return f"[{hit.event_id} @ {hit.ts:%Y-%m-%d %H:%M:%S}]"


def findings_from_timeline(tl, status: str, runbook: str | None) -> Findings:
    return Findings(
        service=tl.service or "incident",
        status=status,
        cause_text=tl.cause.text if tl.cause else None,
        cause_cite=cite_hit(tl.cause) if tl.cause else None,
        fix_text=tl.fix.text if tl.fix else None,
        fix_cite=cite_hit(tl.fix) if tl.fix else None,
        runbook=runbook,
        narrative=None,
    )


MAX_UPDATES = 4              # a Slack brief has to stay skimmable
_MAX_UPDATE_CHARS = 200


def findings_from_updates(service: str, status: str, hits,
                          runbook: str | None) -> Findings:
    """Brief the incident's own updates, newest first, each cited.

    Status feeds state what is happening in the provider's own words but contain
    no change events, so `findings_from_timeline` correctly declines to name a
    cause — and "no cause found" is not a useful brief on its own. This reports
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
    if f.narrative:
        lines.append("")
        lines.append(f.narrative)
    else:
        if f.cause_text:
            lines.append(f"Cause: {f.cause_text} — {f.cause_cite}")
        else:
            lines.append("Cause: not identified from retrieved evidence")
        if f.fix_text:
            lines.append(f"Resolution: {f.fix_text} — {f.fix_cite}")
        else:
            lines.append("Resolution: not identified from retrieved evidence")
    if f.updates:
        lines.append("Updates:")
        lines.extend(f"  {line}" for line in f.updates)
    lines.append(f"Runbook: {f.runbook}" if f.runbook else "Runbook: none found")
    if f.meta:
        lines.append(f.meta)
    if f.impact:
        lines.append(f.impact)
    return "\n".join(lines)
