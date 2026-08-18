from datetime import UTC, datetime

from freshet.ingest.statuspage import parse_atom


def _feed(entries: str) -> str:
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<feed xmlns="http://www.w3.org/2005/Atom">' + entries + "</feed>")


def _entry(inc="31199495", updated="2026-08-18T11:42:59Z", title="Elevated errors",
           content="") -> str:
    return (f"<entry><id>tag:www.githubstatus.com,2005:Incident/{inc}</id>"
            f"<published>2026-08-18T09:00:00Z</published><updated>{updated}</updated>"
            f"<title>{title}</title><content type='html'>{content}</content></entry>")


# Statuspage wraps the day and time in <var> tags inside <small>.
def _block(mon, day, hhmm, tz, status, body):
    return (f"&lt;p&gt; &lt;small&gt;{mon} &lt;var data-var='date'&gt;{day}&lt;/var&gt;, "
            f"&lt;var data-var='time'&gt;{hhmm}&lt;/var&gt; {tz}&lt;/small&gt;&lt;br&gt; "
            f"&lt;strong&gt;{status}&lt;/strong&gt; - {body}&lt;/p&gt;")


TWO = _block("Aug", 18, "11:42", "UTC", "Resolved", "All clear now.") + \
      _block("Aug", 18, "11:24", "UTC", "Investigating", "Looking into it.")


def test_every_update_in_an_entry_becomes_its_own_record():
    got = parse_atom("github", _feed(_entry(content=TWO)))
    assert len(got) == 2, "one record per update, not per incident"
    assert [u.status for u in got] == ["investigating", "resolved"], "oldest first"
    assert "Looking into it." in got[0].text
    assert "All clear now." not in got[0].text, "updates must not bleed into each other"


def test_html_timestamps_are_used_for_created_at():
    got = parse_atom("github", _feed(_entry(content=TWO)))
    assert got[0].created_at == datetime(2026, 8, 18, 11, 24, tzinfo=UTC)
    assert got[1].created_at == datetime(2026, 8, 18, 11, 42, tzinfo=UTC)


def test_identity_is_body_derived_so_it_survives_a_newer_update_arriving():
    """If identity depended on position or on the entry's `updated`, an existing
    update would get a NEW dedup key the moment a newer update pushed it down the
    list — re-emitting it as if it were new."""
    before = parse_atom("github", _feed(_entry(content=TWO)))
    newer = _block("Aug", 18, "12:05", "UTC", "Monitoring", "Watching it.") + TWO
    after = parse_atom("github", _feed(_entry(updated="2026-08-18T12:05:00Z", content=newer)))
    assert len(after) == 3
    assert {u.dedup_key for u in before} <= {u.dedup_key for u in after}, \
        "existing updates must keep their keys"


def test_reparsing_the_same_feed_yields_identical_keys():
    a = parse_atom("github", _feed(_entry(content=TWO)))
    b = parse_atom("github", _feed(_entry(content=TWO)))
    assert [u.dedup_key for u in a] == [u.dedup_key for u in b]


def test_non_utc_timezone_abbreviations_are_converted():
    edt = _block("Aug", 6, "15:37", "EDT", "Resolved", "done")   # UTC-4
    got = parse_atom("datadog", _feed(_entry(updated="2026-08-06T19:37:00Z", content=edt)))
    assert got[0].created_at == datetime(2026, 8, 6, 19, 37, tzinfo=UTC)


def test_year_is_inferred_backwards_across_a_new_year_boundary():
    """HTML timestamps carry no year. An update dated Dec on an entry revised in
    Jan belongs to the PREVIOUS year, not a future one."""
    dec = _block("Dec", 30, "23:00", "UTC", "Resolved", "done")
    got = parse_atom("x", _feed(_entry(updated="2026-01-02T10:00:00Z", content=dec)))
    assert got[0].created_at == datetime(2025, 12, 30, 23, 0, tzinfo=UTC)


def test_unknown_timezone_falls_back_to_the_exact_entry_timestamp():
    weird = _block("Aug", 18, "11:42", "XYZ", "Resolved", "done")
    got = parse_atom("x", _feed(_entry(updated="2026-08-18T11:42:59Z", content=weird)))
    assert got[0].created_at == datetime(2026, 8, 18, 11, 42, 59, tzinfo=UTC), \
        "never guess an offset; use the timestamp we know exactly"


def test_unparseable_content_falls_back_to_one_record_per_revision():
    """Some providers (openai) do not use this markup at all. They must degrade to
    the previous behaviour, not vanish."""
    got = parse_atom("openai", _feed(_entry(content="&lt;p&gt;plain prose&lt;/p&gt;")))
    assert len(got) == 1
    assert got[0].created_at == datetime(2026, 8, 18, 11, 42, 59, tzinfo=UTC)


def test_empty_and_malformed_feeds_yield_nothing_rather_than_raising():
    assert parse_atom("x", "") == []
    assert parse_atom("x", "<not-xml") == []
    assert parse_atom("x", _feed("")) == []


def test_url_style_entry_ids_are_supported():
    """Not every provider uses Statuspage's `tag:...,2005:Incident/NNN` URN.
    OpenAI uses a plain incident URL; those entries must not be silently dropped."""
    feed = _feed(
        "<entry><id>https://status.openai.com//incidents/01M0B4WSV41BCFZ9VDWKSMQVSP</id>"
        "<updated>2026-08-18T19:26:38Z</updated><title>Elevated errors</title>"
        "<content type='html'>&lt;p&gt;plain prose&lt;/p&gt;</content></entry>")
    got = parse_atom("openai", feed)
    assert len(got) == 1
    assert got[0].incident_id == "01M0B4WSV41BCFZ9VDWKSMQVSP"


def test_identical_body_text_at_different_times_stays_two_distinct_updates():
    """Providers repeat boilerplate ("We are continuing to monitor..."). If identity
    were the body alone, those would collide and one would be lost."""
    repeated = (_block("Aug", 18, "12:00", "UTC", "Monitoring", "We continue to monitor.")
                + _block("Aug", 18, "11:00", "UTC", "Monitoring", "We continue to monitor."))
    got = parse_atom("github", _feed(_entry(content=repeated)))
    assert len(got) == 2
    assert got[0].dedup_key != got[1].dedup_key
