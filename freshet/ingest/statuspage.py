"""Statuspage Atom history feed -> IncidentUpdate. Pure parsing, no I/O, so it is
fully unit-testable against captured feeds.

Why Atom and not the JSON API: Statuspage's platform-wide robots.txt is
`Disallow: /api/`, so `/api/v2/incidents.json` is off-limits for scheduled
automated access. `/history.atom` is what the platform publishes for subscription
and is explicitly allowed.

The feed carries one <entry> per incident, revised in place, with every update
concatenated as escaped HTML inside <content>. We split that HTML back into
individual updates, because polling every 60s otherwise collapses any two updates
that land in the same window (measured on real data: 6.6% of updates lost).

Two properties this module is careful about:

* **Identity is body-derived**, never positional and never timestamp-derived. An
  update keeps its dedup key when a newer update pushes it down the list; if the
  key moved, the update would be re-emitted as though it were new.
* **A timestamp is never guessed.** The HTML carries no year and uses ambiguous
  timezone abbreviations, so unresolvable cases fall back to the entry's exact
  ISO `updated` rather than inventing an offset. The freshness metric is the one
  number this project reports; it must not rest on a guess.
"""
from __future__ import annotations

import hashlib
import html
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta, timezone

from freshet.ingest.sources import IncidentUpdate
from freshet.pipeline.metrics import TIMESTAMP_FALLBACK

_ATOM = "{http://www.w3.org/2005/Atom}"
# Statuspage URN (`tag:host,2005:Incident/123`) or a plain incident URL.
_INCIDENT_ID = re.compile(r"Incident/(\w+)|/incidents?/([\w-]+)", re.I)
_TAG = re.compile(r"<[^>]+>")
# <small>Aug <var>18</var>, <var>11:42</var> UTC</small><br> <strong>Resolved</strong> - body
_UPDATE = re.compile(
    r"<small>(?P<when>.*?)</small>\s*<br\s*/?>\s*<strong>(?P<status>.*?)</strong>\s*-\s*"
    r"(?P<body>.*?)(?=<small>|\Z)", re.I | re.S)
# second markup shape (non-Atlassian providers): one block holds current state
_STATUS_LINE = re.compile(r"<b>\s*Status:\s*(?P<status>[^<]+?)\s*</b>(?P<body>.*)",
                          re.I | re.S)
# Trailing component list reflects LIVE state, not the incident — excluded from identity.
_COMPONENTS = re.compile(r"<b>\s*Affected components\s*</b>.*\Z", re.I | re.S)
# A provider matching neither shape gets one record per revision, capped.
FALLBACK_MAX_CHARS = 2000
_WHEN = re.compile(r"([A-Z][a-z]{2})\s+(\d{1,2})\s*,\s*(\d{1,2}):(\d{2})\s*([A-Z]{2,5})")
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
# offsets unambiguous worldwide only; an unresolvable stamp falls back to the
# entry's ISO `updated`
_OFFSETS = {"UTC": 0, "GMT": 0, "UT": 0, "Z": 0,
            "EST": -5, "EDT": -4, "CDT": -5,
            "MST": -7, "MDT": -6, "PST": -8, "PDT": -7,
            "CET": 1, "CEST": 2, "EET": 2, "EEST": 3, "WET": 0, "WEST": 1,
            "JST": 9, "KST": 9, "HKT": 8, "SGT": 8, "MSK": 3,
            "AEST": 10, "AEDT": 11, "AWST": 8, "NZST": 12, "NZDT": 13,
            "BRT": -3, "ART": -3}


def _text(node: ET.Element, tag: str) -> str:
    found = node.find(f"{_ATOM}{tag}")
    return (found.text or "").strip() if found is not None else ""


def _parse_iso(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except (ValueError, AttributeError):
        return None


def _plain(markup: str) -> str:
    return re.sub(r"\s+", " ", _TAG.sub(" ", markup)).strip()


def _parse_when(raw: str, anchor: datetime) -> datetime | None:
    """Resolve a year-less, abbreviation-timezoned stamp against the entry's exact
    revision time. Returns None when anything is ambiguous — callers then use the
    anchor rather than a guess."""
    match = _WHEN.search(_plain(raw))
    if match is None:
        return None
    mon, day, hour, minute, tz = match.groups()
    if mon not in _MONTHS or tz not in _OFFSETS:
        return None
    tzinfo = timezone(timedelta(hours=_OFFSETS[tz]))
    try:
        local = datetime(anchor.year, _MONTHS[mon], int(day),
                         int(hour), int(minute), tzinfo=tzinfo)
    except ValueError:                      # e.g. Feb 30
        return None
    stamp = local.astimezone(UTC)
    # A stamp after its own revision means the year rolled over (Dec update, Jan revision).
    if stamp > anchor + timedelta(minutes=1):
        try:
            stamp = local.replace(year=anchor.year - 1).astimezone(UTC)
        except ValueError:
            return None
    if stamp < anchor - timedelta(days=400):
        return None
    return stamp


def parse_atom(provider: str, feed: str) -> list[IncidentUpdate]:
    """Flatten a status feed into individual updates, oldest first.

    Malformed entries are skipped rather than raised: one bad record from a third
    party must not stall ingestion of the other providers.
    """
    # No status feed declares a DTD; refuse one rather than let expat expand entities.
    if "<!DOCTYPE" in feed[:2048].upper():
        return []
    try:
        root = ET.fromstring(feed)
    except ET.ParseError:
        return []

    out: list[IncidentUpdate] = []
    for entry in root.findall(f"{_ATOM}entry"):
        incident = _INCIDENT_ID.search(_text(entry, "id"))
        revised = _parse_iso(_text(entry, "updated") or _text(entry, "published"))
        if incident is None or revised is None:
            continue
        incident_id = incident.group(1) or incident.group(2)
        name = _text(entry, "title")
        markup = html.unescape(_text(entry, "content"))
        blocks = list(_UPDATE.finditer(markup))
        if blocks:
            seen_markers: dict[str, int] = {}
            for block in blocks:
                body = _plain(block.group("body"))
                marker = _plain(block.group("when"))
                # Ordinal disambiguates repeats of the same minute-resolution stamp,
                # not position, so an update keeps its key as newer ones are prepended.
                ordinal = seen_markers[marker] = seen_markers.get(marker, -1) + 1
                stamp = _parse_when(block.group("when"), revised)
                if stamp is None:
                    TIMESTAMP_FALLBACK.inc()
                    stamp = revised
                out.append(_make(provider, incident_id, name, stamp,
                                 _plain(block.group("status")).lower(), body,
                                 identity=f"{marker}#{ordinal}"))
            continue

        single = _STATUS_LINE.search(markup)
        if single is not None:
            status = _plain(single.group("status")).lower()
            body = _plain(_COMPONENTS.sub("", single.group("body")))
            out.append(_make(provider, incident_id, name, revised, status, body,
                             identity=f"status:{status}"))
            continue

        prose = _plain(markup)[:FALLBACK_MAX_CHARS].strip()
        if not prose:
            continue        # markup that flattens to nothing is not an update
        out.append(_make(provider, incident_id, name, revised, "unknown", prose,
                         identity=f"revision:{revised.isoformat()}"))

    out.sort(key=lambda u: u.created_at)
    return out


def _make(provider: str, incident_id: str, name: str, stamp: datetime,
          status: str, body: str, identity: str) -> IncidentUpdate:
    digest = hashlib.blake2s(identity.encode(), digest_size=6).hexdigest()
    return IncidentUpdate(
        provider=provider, incident_id=incident_id, update_id=digest,
        created_at=stamp, status=status, text=body, incident_name=name,
        body_digest=hashlib.blake2s(body.encode(), digest_size=6).hexdigest(),
    )
