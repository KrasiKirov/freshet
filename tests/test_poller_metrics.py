"""The ingest stage determines freshness and talks to 42 third parties; without
metrics the only way to see a provider stop parsing is to query the index.

`freshet_poll_updates_parsed` is the counter that would have caught the openai
amplification in an hour: it went from ~1 update per incident to ~25 with nothing
else changing.
"""
from freshet.ingest import poller
from freshet.ingest.registry import Page
from freshet.pipeline import metrics

_FEED = ('<?xml version="1.0" encoding="UTF-8"?>'
         '<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
         "<id>tag:x,2005:Incident/1</id><updated>2026-08-18T11:42:59Z</updated>"
         "<title>Something broke</title><content type='html'>"
         "&lt;p&gt; &lt;small&gt;Aug &lt;var&gt;18&lt;/var&gt;, "
         "&lt;var&gt;11:42&lt;/var&gt; UTC&lt;/small&gt;&lt;br&gt; "
         "&lt;strong&gt;Resolved&lt;/strong&gt; - All clear.&lt;/p&gt;"
         "</content></entry></feed>")

PAGES = [Page("github", "https://x.test/f.atom")]


def _fetches(status: str) -> float:
    return metrics.POLL_FETCH.labels(provider="github", status=status)._value.get()


def _updates() -> float:
    return metrics.POLL_UPDATES.labels(provider="github")._value.get()


def test_an_unchanged_feed_is_counted_as_a_304():
    before = _fetches("304")
    poller.poll_once(PAGES, lambda u, h: (304, {}, None), poller.ConditionalCache(path=""))
    assert _fetches("304") == before + 1


def test_a_failed_fetch_is_counted_as_error_rather_than_going_unrecorded():
    def boom(url, headers):
        raise OSError("connection reset")

    before = _fetches("error")
    poller.poll_once(PAGES, boom, poller.ConditionalCache(path=""), poller.HostBackoff())
    assert _fetches("error") == before + 1


def test_a_backed_off_host_is_counted_as_skipped_not_silently_absent():
    """Without this label a host in backoff looks identical to a host nobody polls."""
    backoff = poller.HostBackoff()
    backoff.failed("https://x.test/f.atom")
    before = _fetches("skipped")
    poller.poll_once(PAGES, lambda u, h: (200, {}, _FEED),
                     poller.ConditionalCache(path=""), backoff)
    assert _fetches("skipped") == before + 1


def test_parsed_updates_are_counted_per_provider():
    before = _updates()
    poller.poll_once(PAGES, lambda u, h: (200, {}, _FEED),
                     poller.ConditionalCache(path=""))
    assert _updates() == before + 1


def test_the_backoff_gauge_reports_how_many_hosts_are_being_skipped():
    backoff = poller.HostBackoff(now=lambda: 0.0, wall=lambda: 0.0)
    assert backoff.active_count() == 0
    backoff.failed("https://a.test/f.atom")
    backoff.failed("https://b.test/f.atom")
    assert backoff.active_count() == 2
    backoff.succeeded("https://a.test/f.atom")
    assert backoff.active_count() == 1
