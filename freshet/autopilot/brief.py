"""Pure rendering for the autopilot incident brief. Given normalized Findings,
produce a plain-text, cited brief. No I/O, no DB — trivially unit-testable.
Slack formatting lives in the Slack sink; the impact line is computed by the impact
heuristic (freshet/autopilot/impact.py) and rendered here when present."""

from __future__ import annotations

from dataclasses import dataclass


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


def findings_from_investigation(inv, service: str, status: str,
                                cause_hit, fix_hit, runbook: str | None) -> Findings:
    return Findings(
        service=service,
        status=status,
        cause_text=cause_hit.text if cause_hit else None,
        cause_cite=cite_hit(cause_hit) if cause_hit else None,
        fix_text=fix_hit.text if fix_hit else None,
        fix_cite=cite_hit(fix_hit) if fix_hit else None,
        runbook=runbook,
        narrative=inv.narrative,
    )


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
    lines.append(f"Runbook: {f.runbook}" if f.runbook else "Runbook: none found")
    if f.meta:
        lines.append(f.meta)
    if f.impact:
        lines.append(f.impact)
    return "\n".join(lines)
