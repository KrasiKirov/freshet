"""Politeness is a stated design requirement of this poller, not an afterthought.

The module docstring and the README both promise per-host backoff and cheap
unchanged fetches. These tests pin the levers that make those claims true.
"""
import gzip

import pytest

from freshet.ingest import poller


def test_a_429_backs_off_for_the_interval_the_provider_asked_for():
    """A 429 fell into the exponential guess and retried in 2s, which is the
    opposite of what the status code means."""
    backoff = poller.HostBackoff(now=lambda: 1000.0, wall=lambda: 0.0)
    assert backoff.failed("https://status.example.com/history.atom",
                          retry_after=120.0) == 120.0


def test_retry_after_is_capped_so_a_hostile_value_cannot_pin_a_feed_forever():
    backoff = poller.HostBackoff(now=lambda: 1000.0, wall=lambda: 0.0)
    assert backoff.failed("https://status.example.com/f.atom",
                          retry_after=99999.0) == poller.MAX_BACKOFF_S


def test_backoff_is_shared_by_every_feed_on_one_host():
    """The docstring and README both say per-HOST. Keyed by URL, two feeds on one
    host each hammered it independently while it was failing."""
    backoff = poller.HostBackoff(now=lambda: 1000.0, wall=lambda: 0.0)
    backoff.failed("https://status.atlassian.com/a.atom")
    assert backoff.skip("https://status.atlassian.com/b.atom")
    assert not backoff.skip("https://status.other.com/b.atom")


def test_a_success_on_one_feed_clears_the_whole_host():
    backoff = poller.HostBackoff(now=lambda: 1000.0, wall=lambda: 0.0)
    backoff.failed("https://status.atlassian.com/a.atom")
    backoff.succeeded("https://status.atlassian.com/b.atom")
    assert not backoff.skip("https://status.atlassian.com/a.atom")


def test_repeated_failures_cannot_overflow_the_exponent():
    """The failure count persists across restarts and only resets on success, so a
    permanently dead feed reaches n=1024 in about 85 days — and 2.0**1024 raises
    OverflowError, taking down the sweep rather than skipping one host."""
    backoff = poller.HostBackoff(now=lambda: 0.0, wall=lambda: 0.0)
    backoff._failures["status.example.com"] = 5000
    assert backoff.failed("https://status.example.com/f.atom") == poller.MAX_BACKOFF_S


def test_every_request_asks_for_a_compressed_response():
    """42 uncompressed feeds per cache-miss sweep. Status markup compresses hard;
    this is the second-biggest politeness lever after the validators."""
    cache = poller.ConditionalCache(path="")
    assert cache.headers_for("https://x.test/f.atom")["Accept-Encoding"] == "gzip"


def test_a_gzip_body_is_decompressed():
    class _Gzipped:
        headers = {"Content-Encoding": "gzip"}

        def read(self, n):
            return gzip.compress(b"<feed/>")

    assert poller._read_body(_Gzipped()) == "<feed/>"


def test_an_uncompressed_body_still_reads():
    class _Plain:
        headers: dict = {}

        def read(self, n):
            return b"<feed/>"

    assert poller._read_body(_Plain()) == "<feed/>"


def test_an_encoding_we_cannot_decode_raises_instead_of_returning_mojibake():
    """We advertise gzip only, because the stdlib has no brotli decoder — and a CDN
    offered `br` will use it (measured on status.openai.com). If one ever arrives
    anyway, failing loudly beats handing the parser bytes that decode to garbage,
    which would surface as an unparseable feed with the real reason lost."""
    class _Brotli:
        headers = {"Content-Encoding": "br"}

        def read(self, n):
            return b"\x1b\x0e\x00\xf8\x25"

    with pytest.raises(ValueError, match="unsupported Content-Encoding"):
        poller._read_body(_Brotli())


def test_an_explicit_identity_encoding_is_read_as_plain_text():
    class _Identity:
        headers = {"Content-Encoding": "identity"}

        def read(self, n):
            return b"<feed/>"

    assert poller._read_body(_Identity()) == "<feed/>"


def test_a_body_larger_than_the_cap_is_refused():
    """The bodies come from 42 third parties and were read unbounded into memory."""
    class _Huge:
        headers: dict = {}

        def read(self, n):
            return b"x" * n          # always returns the full requested length

    with pytest.raises(ValueError, match="exceeds"):
        poller._read_body(_Huge())


def test_retry_after_accepts_the_delta_seconds_form():
    assert poller._retry_after_seconds({"Retry-After": "45"}) == 45.0


def test_a_missing_or_unparseable_retry_after_falls_back_to_the_guess():
    """An advisory header must never be able to raise out of the fetch path."""
    assert poller._retry_after_seconds({}) is None
    assert poller._retry_after_seconds({"Retry-After": "soon"}) is None
