"""A failing host must drop out of the sweep, not stay in its critical path.

The README claimed per-host backoff; the code only logged the failure and
retried the same host on the very next sweep.
"""
from freshet.ingest.poller import MAX_BACKOFF_S, ConditionalCache, HostBackoff, poll_once
from freshet.ingest.registry import Page


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_a_failure_skips_the_host_until_the_delay_elapses():
    clock = _Clock()
    b = HostBackoff(now=clock)
    b.failed("https://x/history.atom")
    assert b.skip("https://x/history.atom")
    clock.t = 1.9
    assert b.skip("https://x/history.atom"), "still inside the first backoff"
    clock.t = 2.1
    assert not b.skip("https://x/history.atom")


def test_repeated_failures_back_off_further_but_are_capped():
    clock = _Clock()
    b = HostBackoff(now=clock)
    delays = [b.failed("u") for _ in range(12)]
    assert delays[0] < delays[1] < delays[2], "must grow"
    assert max(delays) == MAX_BACKOFF_S, "and stop growing at the cap"


def test_a_success_clears_the_backoff():
    clock = _Clock()
    b = HostBackoff(now=clock)
    b.failed("u")
    b.succeeded("u")
    assert not b.skip("u")


def test_a_backed_off_host_is_not_fetched_again():
    clock = _Clock()
    b = HostBackoff(now=clock)
    b.failed("https://down/history.atom")
    fetched = []

    def fetch(url, headers):
        fetched.append(url)
        return 200, {}, "<feed></feed>"

    pages = [Page(provider="down", url="https://down/history.atom"),
             Page(provider="up", url="https://up/history.atom")]
    poll_once(pages, fetch, ConditionalCache(), b)
    assert fetched == ["https://up/history.atom"], "the failing host is skipped"


def test_the_cache_round_trips_validators_across_a_restart(tmp_path):
    path = str(tmp_path / "poll_cache.json")
    c = ConditionalCache(path)
    c.remember("https://x/history.atom", {"ETag": "abc", "Last-Modified": "yesterday"})
    c.save()

    restarted = ConditionalCache(path)
    headers = restarted.headers_for("https://x/history.atom")
    assert headers["If-None-Match"] == "abc"
    assert headers["If-Modified-Since"] == "yesterday"


def test_an_unreadable_cache_starts_cold_instead_of_crashing(tmp_path):
    """A corrupt cache is advisory data: it must not stop the poller."""
    path = tmp_path / "broken.json"
    path.write_text("{not json")
    headers = ConditionalCache(str(path)).headers_for("https://x/history.atom")
    assert "If-None-Match" not in headers, "no validators recovered, but no crash"
    assert headers["User-Agent"]
