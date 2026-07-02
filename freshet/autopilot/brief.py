"""Pure rendering for the autopilot incident brief. Given normalized Findings,
produce a plain-text, cited brief. No I/O, no DB — trivially unit-testable.
Slack formatting is sub-project ②; impact math is sub-project ④ (stub here)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

IMPACT_STUB = "Impact: estimation pending (sub-project ④)."


@dataclass
class Findings:
    service: str
    status: str
    cause_text: Optional[str]
    cause_cite: Optional[str]
    fix_text: Optional[str]
    fix_cite: Optional[str]
    runbook: Optional[str]
    narrative: Optional[str]


def cite_hit(hit) -> str:
    return f"[{hit.event_id} @ {hit.ts:%Y-%m-%d %H:%M:%S}]"


def findings_from_timeline(tl, status: str, runbook: Optional[str]) -> Findings:
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
                                cause_hit, fix_hit, runbook: Optional[str]) -> Findings:
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
    lines = [f"=== INCIDENT BRIEF — {f.service} ({f.status}) ==="]
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
    lines.append(IMPACT_STUB)
    return "\n".join(lines)
