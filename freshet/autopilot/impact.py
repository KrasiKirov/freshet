"""Derived incident-impact heuristic — pure, keyless, deterministic. Impact is a
*derived indicator* from observable proxies (breadth, duration, and quantified
figures stated in the retrieved text), NOT a measured user-impact number. Self-
contained (no imports from investigate) to avoid an import cycle."""

from __future__ import annotations

import re

_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def max_stated_pct(hit_texts: list[str]) -> float | None:
    vals = [float(m) for t in hit_texts for m in _PCT_RE.findall(t)]
    return max(vals) if vals else None


def _duration_min(opened_at, resolved_at) -> float | None:
    if not opened_at or not resolved_at:
        return None
    return (resolved_at - opened_at).total_seconds() / 60.0


def _duration_display(mins: float | None) -> str:
    if mins is None:
        return "ongoing"
    secs = int(mins * 60)
    if secs < 60:
        return f"{secs}s"
    m = secs // 60
    if m < 60:
        return f"{m}m"
    return f"{m // 60}h {m % 60}m"


def classify_impact(services: list[str], opened_at, resolved_at,
                    hit_texts: list[str]) -> str:
    pct = max_stated_pct(hit_texts)
    n = len(services)
    mins = _duration_min(opened_at, resolved_at)
    if (pct is not None and pct >= 25) or n >= 3 or (mins is not None and mins >= 60):
        return "High"
    # Low requires *positive* evidence of small impact (an explicitly stated low
    # percentage). No stated figure means unknown severity → Medium, not Low: an
    # on-call responder should not downgrade an unquantified incident to Low.
    if (pct is not None and pct < 5) and n == 1 and (mins is None or mins < 10):
        return "Low"
    return "Medium"


def estimate_impact(services: list[str], opened_at, resolved_at,
                    hit_texts: list[str]) -> str:
    label = classify_impact(services, opened_at, resolved_at, hit_texts)
    n = len(services)
    dur = _duration_display(_duration_min(opened_at, resolved_at))
    rationale = f"{n} service{'' if n == 1 else 's'}, {dur}"
    pct = max_stated_pct(hit_texts)
    stated = f"; source reports ~{pct:g}% errors" if pct is not None else ""
    return f"Impact: {label} — {rationale}{stated}"
