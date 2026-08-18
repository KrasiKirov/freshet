"""Statuspage v2 API -> IncidentUpdate. Pure mapping, no I/O, so it is fully
unit-testable against captured payloads."""
from __future__ import annotations

from datetime import UTC, datetime

from freshet.ingest.sources import IncidentUpdate


def _parse_ts(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except (ValueError, AttributeError):
        return None


def parse_statuspage(provider: str, payload: dict) -> list[IncidentUpdate]:
    """Flatten every update of every incident, oldest first.

    Malformed entries are skipped rather than raised: a single bad record from a
    third party must not stall ingestion of the other providers.
    """
    out: list[IncidentUpdate] = []
    for inc in payload.get("incidents") or []:
        incident_id = inc.get("id")
        if not incident_id:
            continue
        name = inc.get("name") or ""
        for upd in inc.get("incident_updates") or []:
            update_id = upd.get("id")
            ts = _parse_ts(upd.get("created_at", ""))
            if not update_id or ts is None:
                continue
            out.append(IncidentUpdate(
                provider=provider,
                incident_id=incident_id,
                update_id=update_id,
                created_at=ts,
                status=upd.get("status") or "unknown",
                text=upd.get("body") or "",
                incident_name=name,
            ))
    out.sort(key=lambda u: u.created_at)
    return out
