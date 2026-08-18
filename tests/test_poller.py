from freshet.ingest.poller import ConditionalCache, poll_once
from freshet.ingest.registry import Page

PAGES = [Page("a", "https://a.example/history.atom"),
         Page("b", "https://b.example/history.atom")]


def _feed(inc: str, updated: str, body: str = "All clear.") -> str:
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
            f"<id>tag:x,2005:Incident/{inc}</id><updated>{updated}</updated>"
            f"<title>Something broke</title><content type='html'>"
            f"&lt;p&gt; &lt;small&gt;Aug &lt;var&gt;18&lt;/var&gt;, "
            f"&lt;var&gt;11:42&lt;/var&gt; UTC&lt;/small&gt;&lt;br&gt; "
            f"&lt;strong&gt;Resolved&lt;/strong&gt; - {body}&lt;/p&gt;"
            "</content></entry></feed>")


def test_polls_every_page_and_flattens_results():
    calls = []

    def fetch(url, headers):
        calls.append(url)
        return 200, {}, _feed("i1", "2026-08-18T11:42:00Z")

    got = poll_once(PAGES, fetch, ConditionalCache())
    assert len(calls) == 2
    assert {u.provider for u in got} == {"a", "b"}


def test_304_not_modified_yields_no_updates():
    def fetch(url, headers):
        return 304, {}, None

    assert poll_once(PAGES, fetch, ConditionalCache()) == []


def test_cache_sends_etag_on_the_second_poll():
    cache = ConditionalCache()
    seen = []

    def fetch(url, headers):
        seen.append(dict(headers))
        return 200, {"ETag": 'W/"abc"'}, _feed("i1", "2026-08-18T11:42:00Z")

    poll_once(PAGES[:1], fetch, cache)
    poll_once(PAGES[:1], fetch, cache)
    assert "If-None-Match" not in seen[0]
    assert seen[1]["If-None-Match"] == 'W/"abc"'


def test_every_request_identifies_itself():
    """Polling third-party endpoints on a schedule without a descriptive
    User-Agent is impolite and makes us indistinguishable from a scraper."""
    seen = []

    def fetch(url, headers):
        seen.append(headers)
        return 304, {}, None

    poll_once(PAGES[:1], fetch, ConditionalCache())
    assert "freshet" in seen[0]["User-Agent"].lower()


def test_one_failing_page_does_not_stop_the_others():
    def fetch(url, headers):
        if "a.example" in url:
            raise TimeoutError("boom")
        return 200, {}, _feed("i1", "2026-08-18T11:42:00Z")

    got = poll_once(PAGES, fetch, ConditionalCache())
    assert [u.provider for u in got] == ["b"]


def test_results_are_sorted_by_event_time():
    def fetch(url, headers):
        if "a.example" in url:
            return 200, {}, _feed("i1", "2026-08-18T13:00:00Z", "later")
        return 200, {}, _feed("i2", "2026-08-18T12:00:00Z", "earlier")

    got = poll_once(PAGES, fetch, ConditionalCache())
    assert got[0].created_at <= got[1].created_at


def test_a_page_that_returns_garbage_is_skipped_not_fatal():
    def fetch(url, headers):
        return 200, {}, "<not-xml"

    assert poll_once(PAGES, fetch, ConditionalCache()) == []


def test_wire_timestamp_is_rfc3339_with_a_literal_z():
    """Flink's ISO-8601 JSON parser returns NULL for the "+00:00" offset form,
    and a NULL rowtime fails the whole streaming job. This is a wire contract."""
    from freshet.ingest.poller import to_message

    def fetch(url, headers):
        return 200, {}, _feed("i1", "2026-08-18T11:42:00Z")

    [update] = poll_once(PAGES[:1], fetch, ConditionalCache())
    stamp = to_message(update)["created_at"]
    assert stamp.endswith("Z"), stamp
    assert "+00:00" not in stamp
