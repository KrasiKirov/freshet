"""Poll public Statuspage.io incident feeds and produce canonical Events to
raw.events. Real operational incidents from real companies — the live-data source
for the demo. stdlib-only fetch (urllib), like the rest of the app; produces to the
same pipeline the workers consume."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from freshet.common.kafka_io import make_producer, produce_sync
from freshet.common.schemas import Event, EventSource, Severity

log = logging.getLogger(__name__)

SOURCES: list[tuple[str, str]] = [
    ("cloudflare", "https://www.cloudflarestatus.com/api/v2/incidents.json"),
    ("github", "https://www.githubstatus.com/api/v2/incidents.json"),
    ("reddit", "https://www.redditstatus.com/api/v2/incidents.json"),
    ("discord", "https://discordstatus.com/api/v2/incidents.json"),
    ("openai", "https://status.openai.com/api/v2/incidents.json"),
]

_UA = "freshet-demo/1.0 (+https://github.com/KrasiKirov/freshet)"
_IMPACT_TO_SEV = {"critical": Severity.SEV1, "major": Severity.SEV2, "minor": Severity.SEV3}


def _d(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def _severity(impact: object) -> Optional[Severity]:
    return _IMPACT_TO_SEV.get(impact) if isinstance(impact, str) else None


def map_incident(source: str, incident: dict) -> list[Event]:
    inc = _d(incident)
    iid = inc.get("id")
    name = inc.get("name", "incident")
    sev = _severity(inc.get("impact"))
    out: list[Event] = []
    for upd in inc.get("incident_updates") or []:
        u = _d(upd)
        uid = u.get("id")
        if uid is None:
            continue
        digest = hashlib.sha256(f"{iid}:{uid}".encode()).hexdigest()[:16]
        out.append(Event(
            event_id=f"sp_{digest}",
            ts=_ts(u.get("created_at")),
            service=source,
            source=EventSource.ALERT,
            type=u.get("status", "update"),
            severity=sev,
            text=f"{name}: {u.get('body', '')}",
            incident_id=f"{source}:{iid}",
            structured={"impact": inc.get("impact"), "status": u.get("status")},
            refs=[r for r in [inc.get("shortlink")] if r],
        ))
    return out


def fetch(url: str, timeout: float = 10.0) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:  # network / JSON / anything — skip this source
        log.warning("fetch failed for %s: %s", url, e)
        return None


def poll_once(sources, producer, topic: str = "raw.events") -> int:
    n = 0
    for name, url in sources:
        data = fetch(url)
        if data is None:
            continue
        for incident in data.get("incidents") or []:
            for ev in map_incident(name, incident):
                produce_sync(producer, topic, key=ev.service, value=ev.model_dump_json())
                n += 1
    producer.flush()
    return n


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description="Freshet status-feed poller")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--topic", default="raw.events")
    p.add_argument("--once", action="store_true", help="poll once and exit")
    p.add_argument("--interval", type=float, default=300.0)
    a = p.parse_args()
    producer = make_producer(a.brokers)
    if a.once:
        print(f"produced {poll_once(SOURCES, producer, a.topic)} events")
        return
    while True:
        print(f"produced {poll_once(SOURCES, producer, a.topic)} events")
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
