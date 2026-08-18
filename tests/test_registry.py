import json
from urllib.parse import urlparse

from freshet.ingest.registry import load_pages


def test_registry_loads_and_is_well_formed():
    pages = load_pages()
    assert len(pages) >= 40, "need enough providers to produce real bursts"
    providers = [p.provider for p in pages]
    assert len(providers) == len(set(providers)), "provider names must be unique"


def test_every_url_is_https_and_a_statuspage_atom_feed():
    """Atom, not /api/v2/. Statuspage's platform robots.txt disallows /api/,
    while /history.atom is explicitly allowed — see the ingest module docstring."""
    for p in load_pages():
        assert urlparse(p.url).scheme == "https", p.url
        assert p.url.endswith("/history.atom"), p.url


def test_no_page_targets_the_robots_disallowed_api_path():
    for p in load_pages():
        assert "/api/" not in p.url, f"{p.provider} targets a disallowed path"


def test_registry_file_is_valid_json(tmp_path):
    custom = tmp_path / "pages.json"
    custom.write_text(json.dumps([{"provider": "acme",
                                   "url": "https://status.acme.com/history.atom"}]))
    pages = load_pages(custom)
    assert len(pages) == 1 and pages[0].provider == "acme"
