"""The curated list of status-feed endpoints, kept as data rather than code so
growing the fan-in is a config change and not a redesign.

Every entry targets `/history.atom`, NOT `/api/v2/incidents.json`. Statuspage's
platform-wide robots.txt is `Disallow: /api/`, which rules the JSON API out for
scheduled automated access; the Atom feed is the endpoint the platform publishes
for subscription and is explicitly allowed. Entries were verified as both
robots-allowed and serving entries before being committed.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

_DEFAULT = Path(__file__).with_name("pages.json")


@dataclass(frozen=True, slots=True)
class Page:
    provider: str
    url: str


def load_pages(path: Path | None = None) -> list[Page]:
    raw = json.loads((path or _DEFAULT).read_text())
    return [Page(provider=e["provider"], url=e["url"]) for e in raw]
