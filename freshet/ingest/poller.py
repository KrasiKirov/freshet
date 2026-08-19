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
import os
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from freshet.common.kafka_io import BufferedProducer
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

    def __init__(self, path: str | None = None) -> None:
        self._etag: dict[str, str] = {}
        self._modified: dict[str, str] = {}
        # Persisted so a restart resumes with 304s instead of re-downloading all
        # 42 feeds — the validators are the whole politeness lever, and losing
        # them on every restart threw it away.
        self._path = path or os.environ.get("FRESHET_POLL_CACHE") or ""
        self._backoff: dict = {}
        self._load()

    def _load(self) -> None:
        if not self._path or not os.path.exists(self._path):
            return
        try:
            with open(self._path) as fh:
                data = json.load(fh)
            self._etag = dict(data.get("etag") or {})
            self._modified = dict(data.get("modified") or {})
            self._backoff = dict(data.get("backoff") or {})
            log.info("poll cache: %d validators restored", len(self._etag))
        except Exception as exc:                  # noqa: BLE001 - cache is advisory
            log.warning("poll cache unreadable (%s); starting cold", exc)

    def restore_backoff(self, backoff: HostBackoff) -> None:
        """Reapply persisted per-host backoff after a restart."""
        backoff.restore(self._backoff)

    def save(self, backoff: HostBackoff | None = None) -> None:
        """Best-effort: a cache write must never interrupt polling."""
        if not self._path:
            return
        if backoff is not None:
            self._backoff = backoff.snapshot()
        try:
            tmp = f"{self._path}.tmp"
            with open(tmp, "w") as fh:
                json.dump({"etag": self._etag, "modified": self._modified,
                           "backoff": self._backoff}, fh)
            os.replace(tmp, self._path)           # atomic: no half-written cache
        except Exception as exc:                  # noqa: BLE001 - cache is advisory
            log.warning("could not persist poll cache: %s", exc)

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


MAX_BACKOFF_S = 300.0


class HostBackoff:
    """Skip a failing host for a growing interval instead of retrying every sweep.

    The README claimed per-host backoff; the code only logged and retried on the
    next sweep, so a host that was down stayed in every sweep's critical path.
    """

    def __init__(self, now=time.monotonic, wall=time.time) -> None:
        self._until: dict[str, float] = {}
        self._failures: dict[str, int] = {}
        self._now = now
        # monotonic time does not survive a process restart, so the deadline is
        # ALSO kept in wall-clock terms for persistence. A host that just started
        # a 300s backoff would otherwise be hammered again immediately on restart.
        self._wall = wall

    def snapshot(self) -> dict:
        remaining = {u: self._until[u] - self._now() for u in self._until}
        return {"failures": dict(self._failures),
                "deadlines": {u: self._wall() + r for u, r in remaining.items() if r > 0}}

    def restore(self, data: dict) -> None:
        self._failures = dict(data.get("failures") or {})
        now_wall, now_mono = self._wall(), self._now()
        for url, deadline in (data.get("deadlines") or {}).items():
            remaining = deadline - now_wall
            if remaining > 0:
                self._until[url] = now_mono + remaining

    def skip(self, url: str) -> bool:
        return self._now() < self._until.get(url, 0.0)

    def failed(self, url: str) -> float:
        n = self._failures[url] = self._failures.get(url, 0) + 1
        delay = min(MAX_BACKOFF_S, 2.0 ** n)
        self._until[url] = self._now() + delay
        return delay

    def succeeded(self, url: str) -> None:
        self._failures.pop(url, None)
        self._until.pop(url, None)


def poll_once(pages: list[Page], fetch: FetchFn,
              cache: ConditionalCache,
              backoff: HostBackoff | None = None) -> list[IncidentUpdate]:
    """One sweep over every page, oldest update first.

    A failing host is logged and skipped: one bad third party must never stall
    ingestion of the other forty-one.
    """
    def one(page: Page) -> list[IncidentUpdate]:
        if backoff is not None and backoff.skip(page.url):
            return []
        try:
            status, headers, body = fetch(page.url, cache.headers_for(page.url))
        except Exception as exc:                  # noqa: BLE001 - third-party host
            delay = backoff.failed(page.url) if backoff is not None else 0.0
            log.warning("poll failed provider=%s err=%s (backing off %.0fs)",
                        page.provider, exc, delay)
            return []
        if backoff is not None:
            backoff.succeeded(page.url)
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
        # RFC3339 with a literal Z, not "+00:00": Flink's ISO-8601 JSON parser
        # yields NULL for the offset form, and a NULL rowtime kills the job.
        "created_at": update.created_at.isoformat().replace("+00:00", "Z"),
        "status": update.status,
        "text": update.text,
        "incident_name": update.incident_name,
    }


def run(brokers: str, interval_s: float = POLL_INTERVAL_S,
        max_sweeps: int | None = None) -> int:
    """Poll until stopped, producing every observed update to Kafka."""
    pages = load_pages()
    cache = ConditionalCache()
    backoff = HostBackoff()
    cache.restore_backoff(backoff)   # a host mid-backoff stays skipped across restarts
    producer = BufferedProducer(brokers)
    log.info("polling %d feeds every %.0fs", len(pages), interval_s)

    # Stagger the first sweep so a restart does not hit every host at once.
    time.sleep(random.uniform(0, min(5.0, interval_s)))

    produced = sweeps = 0
    while max_sweeps is None or sweeps < max_sweeps:
        started = time.monotonic()
        # One buffered batch per sweep, flushed and checked at the end: a
        # synchronous produce per update paid a round trip 3,600 times a sweep.
        # flush_checked raises on any failed delivery, so a sweep cannot report
        # success while dropping updates.
        for update in poll_once(pages, http_fetch, cache, backoff):
            producer.produce(RAW_TOPIC, json.dumps(to_message(update)),
                             key=update.dedup_key)
            produced += 1
        producer.flush_checked()
        cache.save(backoff)
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
