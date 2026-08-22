"""Unit-test guards that must apply before any test constructs a poller.

The poll cache is opt-OUT (see poller.DEFAULT_CACHE_PATH): a bare
`ConditionalCache()` now resolves to a real file under ~/.local/state. Unit tests
build one all over the place, so without this every run would read — and could
write — the developer's live validators, letting a real ETag leak into a test's
request headers. Pointing the env var at the empty string disables persistence for
the whole unit suite; the two tests that DO exercise persistence monkeypatch
DEFAULT_CACHE_PATH at a tmp_path and set the var themselves.
"""
import pytest


@pytest.fixture(autouse=True)
def _no_real_poll_cache(monkeypatch):
    monkeypatch.setenv("FRESHET_POLL_CACHE", "")
