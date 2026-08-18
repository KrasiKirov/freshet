"""Statuspage Atom history feed -> IncidentUpdate. Pure parsing, no I/O, so it is
fully unit-testable against captured feeds.

Why Atom and not the JSON API: Statuspage's platform-wide robots.txt is
`Disallow: /api/`, so `/api/v2/incidents.json` is off-limits for scheduled
automated access. `/history.atom` is the endpoint the platform publishes for
subscription and is explicitly allowed.

The feed carries ONE ENTRY PER INCIDENT, revised in place as updates land, with
every update concatenated as escaped HTML inside <content>. So identity is
(incident, revision): `updated` is both the event time we measure staleness from
and the discriminator that makes a new update a new dedup key. That avoids
parsing timestamps out of the HTML body, which carries no year and would be
ambiguous across a year boundary.
"""
from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from freshet.ingest.sources import IncidentUpdate

_ATOM = "{http://www.w3.org/2005/Atom}"
# "tag:www.githubstatus.com,2005:Incident/31199495" -> "31199495"
_INCIDENT_ID = re.compile(r"Incident/(\w+)")
_TAG = re.compile(r"<[^>]+>")
_STATUS = re.compile(r"<strong>\s*([A-Za-z ]+?)\s*</strong>")


def _text(entry: ET.Element, tag: str) -> str:
    node = entry.find(f"{_ATOM}{tag}")
    return (node.text or "").strip() if node is not None else ""


def _parse_ts(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except (ValueError, AttributeError):
        return None


def _plain(markup: str) -> str:
    """Unescape the doubly-encoded HTML body and strip tags to readable text."""
    return re.sub(r"\s+", " ", _TAG.sub(" ", html.unescape(markup))).strip()


def parse_atom(provider: str, feed: str) -> list[IncidentUpdate]:
    """Flatten a status feed into updates, oldest first.

    Malformed entries are skipped rather than raised: one bad record from a third
    party must not stall ingestion of the other providers.
    """
    try:
        root = ET.fromstring(feed)
    except ET.ParseError:
        return []

    out: list[IncidentUpdate] = []
    for entry in root.findall(f"{_ATOM}entry"):
        raw_id = _text(entry, "id")
        match = _INCIDENT_ID.search(raw_id)
        revised = _parse_ts(_text(entry, "updated") or _text(entry, "published"))
        if match is None or revised is None:
            continue
        content = _text(entry, "content")
        status = _STATUS.search(html.unescape(content))
        out.append(IncidentUpdate(
            provider=provider,
            incident_id=match.group(1),
            # the revision instant IS the update's identity — see module docstring
            update_id=revised.isoformat(),
            created_at=revised,
            status=status.group(1).lower() if status else "unknown",
            text=_plain(content),
            incident_name=_text(entry, "title"),
        ))
    out.sort(key=lambda u: u.created_at)
    return out
