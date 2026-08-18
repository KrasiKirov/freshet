"""Concurrent fan-in poller over the curated public status feeds.

ThreadPoolExecutor + stdlib urllib, matching this codebase's deliberate
stdlib-only HTTP convention. At ~42 endpoints per 60s the sustained rate is well
under 1 req/s, so asyncio would be complexity without benefit.

Politeness is a design requirement, not an afterthought: every request carries a
descriptive User-Agent, conditional requests mean an unchanged feed costs a 304,
the first sweep is staggered so a restart does not hit every host at once, and a
failing host is skipped rather than retried tightly.

This stage is deliberately STATELESS. It re-emits everything it sees on every
sweep and lets the downstream Flink job dedup with checkpointed state, so the
poller can be restarted freely without losing or double-counting anything.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from freshet.common.kafka_io import make_producer, produce_sync
from freshet.ingest.registry import Page, load_pages
from freshet.ingest.sources import IncidentUpdate
from freshet.ingest.statuspage import parse_atom

log = logging.getLogger(__name__)

RAW_TOPIC = "raw.incidents"
USER_AGENT = "freshet/2.0 (+https://github.com/KrasiKirov/freshet)"
MAX_WORKERS = 12
TIMEOUT_S = 15.0
POLL_INTERVAL_S = 60.0

# (status, response headers, body) — body is None when the feed was unchanged.
FetchFn = Callable[[str, dict], tuple[int, dict, str | None]]


class ConditionalCache:
    """Remembers ETag / Last-Modified per URL so an unchanged feed costs a 304
    instead of a full body — the single biggest politeness lever we have."""

    def __init__(self) -> None:
        self._etag: dict[str, str] = {}
        self._modified: dict[str, str] = {}

    def headers_for(self, url: str) -> dict[str, str]:
        headers = {"User-Agent": USER_AGENT}
        if etag := self._etag.get(url):
            headers["If-None-Match"] = etag
        if since := self._modified.get(url):
            headers["If-Modified-Since"] = since
        return headers

    def remember(self, url: str, response_headers: dict) -> None:
        if etag := response_headers.get("ETag"):
            self._etag[url] = etag
        if modified := response_headers.get("Last-Modified"):
            self._modified[url] = modified


def http_fetch(url: str, headers: dict) -> tuple[int, dict, str | None]:
    """Real network fetch. Injected into `poll_once` so tests never touch it."""
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
            body = response.read().decode("utf-8", "replace")
            return response.status, dict(response.headers), body
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return 304, dict(exc.headers), None
        raise


def poll_once(pages: list[Page], fetch: FetchFn,
              cache: ConditionalCache) -> list[IncidentUpdate]:
    """One sweep over every page, oldest update first.

    A failing host is logged and skipped: one bad third party must never stall
    ingestion of the other forty-one.
    """
    def one(page: Page) -> list[IncidentUpdate]:
        try:
            status, headers, body = fetch(page.url, cache.headers_for(page.url))
        except Exception as exc:                  # noqa: BLE001 - third-party host
            log.warning("poll failed provider=%s err=%s", page.provider, exc)
            return []
        if status == 304 or not body:
            return []
        cache.remember(page.url, headers)
        return parse_atom(page.provider, body)

    updates: list[IncidentUpdate] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for found in pool.map(one, pages):
            updates.extend(found)

    updates.sort(key=lambda u: u.created_at)
    return updates


def to_message(update: IncidentUpdate) -> dict:
    """Wire form. `created_at` stays ISO so the Flink job can assign event time
    without needing this module."""
    return {
        "provider": update.provider,
        "incident_id": update.incident_id,
        "update_id": update.update_id,
        "created_at": update.created_at.isoformat(),
        "status": update.status,
        "text": update.text,
        "incident_name": update.incident_name,
    }


def run(brokers: str, interval_s: float = POLL_INTERVAL_S,
        max_sweeps: int | None = None) -> int:
    """Poll until stopped, producing every observed update to Kafka."""
    pages = load_pages()
    cache = ConditionalCache()
    producer = make_producer(brokers)
    log.info("polling %d feeds every %.0fs", len(pages), interval_s)

    # Stagger the first sweep so a restart does not hit every host at once.
    time.sleep(random.uniform(0, min(5.0, interval_s)))

    produced = sweeps = 0
    while max_sweeps is None or sweeps < max_sweeps:
        started = time.monotonic()
        for update in poll_once(pages, http_fetch, cache):
            produce_sync(producer, RAW_TOPIC, json.dumps(to_message(update)),
                         key=update.dedup_key)
            produced += 1
        sweeps += 1
        elapsed = time.monotonic() - started
        log.info("sweep %d done in %.1fs (%d produced)", sweeps, elapsed, produced)
        if max_sweeps is None or sweeps < max_sweeps:
            time.sleep(max(0.0, interval_s - elapsed))
    return produced


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=POLL_INTERVAL_S)
    parser.add_argument("--sweeps", type=int, default=None,
                        help="stop after N sweeps (default: run forever)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    run(args.brokers, interval_s=args.interval, max_sweeps=args.sweeps)


if __name__ == "__main__":
    main()
