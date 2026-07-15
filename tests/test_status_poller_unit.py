"""Unit test: poll_once maps a fetched feed and produces to the topic (keyless)."""
import json
from pathlib import Path

FIX = Path("freshet/ingest/fixtures/status/sample_incidents.json")


def test_poll_once_produces(monkeypatch):
    from freshet.ingest import status_poller as sp
    data = json.loads(FIX.read_text())
    monkeypatch.setattr(sp, "fetch", lambda url, timeout=10.0: data)

    produced = []
    monkeypatch.setattr(sp, "produce_sync",
                        lambda prod, topic, key=None, value=None: produced.append((topic, key)))

    class _Producer:
        def flush(self):
            pass

    n = sp.poll_once([("cloudflare", "http://x")], _Producer(), "raw.events")
    assert n == len(data["incidents"][0]["incident_updates"])
    assert n == len(produced)
    assert produced[0] == ("raw.events", "cloudflare")


def test_poll_once_dedupes_across_polls(monkeypatch):
    from freshet.ingest import status_poller as sp
    data = json.loads(FIX.read_text())
    monkeypatch.setattr(sp, "fetch", lambda url, timeout=10.0: data)

    produced = []
    monkeypatch.setattr(sp, "produce_sync",
                        lambda prod, topic, key=None, value=None: produced.append(key))

    class _Producer:
        def flush(self):
            pass

    seen: set = set()
    n1 = sp.poll_once([("cloudflare", "http://x")], _Producer(), "raw.events", seen=seen)
    n2 = sp.poll_once([("cloudflare", "http://x")], _Producer(), "raw.events", seen=seen)
    assert n1 > 0 and n2 == 0          # second poll re-produces nothing
    assert len(produced) == n1


def test_poll_once_skips_failed_source(monkeypatch):
    from freshet.ingest import status_poller as sp
    monkeypatch.setattr(sp, "fetch", lambda url, timeout=10.0: None)  # source down

    class _Producer:
        def flush(self):
            pass

    assert sp.poll_once([("x", "http://x")], _Producer()) == 0
