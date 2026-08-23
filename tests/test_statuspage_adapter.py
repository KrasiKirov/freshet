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


# The second markup shape, served by openai / hashicorp. One block per entry,
# holding the CURRENT state, followed by a live component-status list.
def _status_block(status: str, body: str, components: str = "Login (Operational)") -> str:
    return (f"&lt;b&gt;Status: {status}&lt;/b&gt;&lt;br/&gt;&lt;br/&gt;{body}"
            f"&lt;br/&gt;&lt;br/&gt;&lt;b&gt;Affected components&lt;/b&gt;"
            f"&lt;ul&gt;&lt;li&gt;{components}&lt;/li&gt;&lt;/ul&gt;")


def test_status_line_markup_yields_the_providers_real_status():
    """openai/hashicorp state the status in <b>Status: X</b>, not <strong>X</strong>.
    Falling through to 'unknown' excluded them from BOTH lifecycle predicates, so
    those providers could never be briefed."""
    feed = _feed(_entry(content=_status_block("Resolved", "The issue has been resolved.")))
    got = parse_atom("openai", feed)
    assert len(got) == 1
    assert got[0].status == "resolved"
    assert got[0].text == "The issue has been resolved."


def test_the_live_component_list_is_not_indexed():
    """'Login (Operational)' describes the component's state right now, not the
    incident. Indexing it floods both retrieval arms with boilerplate."""
    feed = _feed(_entry(content=_status_block("Investigating", "Users report errors.")))
    got = parse_atom("openai", feed)
    assert "Operational" not in got[0].text
    assert "Affected components" not in got[0].text


def test_a_component_flipping_does_not_mint_a_new_update():
    """The component list changes independently of the incident. When it was part
    of the identity digest, one incident accumulated 185 event_ids and 555 chunks."""
    first = parse_atom("openai", _feed(_entry(
        content=_status_block("Investigating", "Users report errors.",
                              components="Login (Operational)"))))
    later = parse_atom("openai", _feed(_entry(
        content=_status_block("Investigating", "Users report errors.",
                              components="Login (Degraded Performance) API (Operational)"))))
    assert first[0].dedup_key == later[0].dedup_key


def test_a_new_status_on_the_same_incident_is_a_new_update():
    investigating = parse_atom("openai", _feed(_entry(
        content=_status_block("Investigating", "Users report errors."))))
    resolved = parse_atom("openai", _feed(_entry(
        content=_status_block("Resolved", "The issue has been resolved."))))
    assert investigating[0].dedup_key != resolved[0].dedup_key


def test_the_statuspage_shape_still_wins_when_both_could_match():
    """github's markup must keep producing one record per update, not one per entry."""
    got = parse_atom("github", _feed(_entry(content=TWO)))
    assert len(got) == 2
    assert [u.status for u in got] == ["investigating", "resolved"]


def test_the_last_resort_record_is_bounded():
    """An unknown markup shape must not put an unbounded blob into the index; the
    chunker would silently split it into dozens of retrievable fragments."""
    long_prose = "word " * 2000
    got = parse_atom("x", _feed(_entry(content=f"&lt;p&gt;{long_prose}&lt;/p&gt;")))
    assert len(got) == 1
    assert len(got[0].text) <= 2000


def test_the_last_resort_identity_is_the_revision_not_the_body():
    """If the body were in the digest, any churn in an unknown provider's markup
    would mint a new update on every poll."""
    a = parse_atom("x", _feed(_entry(content="&lt;p&gt;first wording&lt;/p&gt;")))
    b = parse_atom("x", _feed(_entry(content="&lt;p&gt;second wording&lt;/p&gt;")))
    assert a[0].dedup_key == b[0].dedup_key


def test_asia_pacific_abbreviations_resolve_instead_of_falling_back():
    """A fallback stamps every update in the entry with the revision time, which
    collapses their order — the same quality loss the openai path had, just
    quieter. The table was US/EU-only."""
    jst = _block("Aug", 6, "15:37", "JST", "Resolved", "done")     # UTC+9
    got = parse_atom("x", _feed(_entry(updated="2026-08-06T07:00:00Z", content=jst)))
    assert got[0].created_at == datetime(2026, 8, 6, 6, 37, tzinfo=UTC)


def test_an_ambiguous_abbreviation_falls_back_rather_than_guessing():
    """CST is US-6 and China+8; IST is India+5:30 and Ireland+1; BST is Britain+1
    and Brazil-3. Picking one silently puts an update up to 14 hours from where it
    belongs, and freshness is the one number this project reports. The module's
    contract is that a timestamp is never guessed."""
    for abbrev in ("CST", "BST", "IST"):
        block = _block("Aug", 6, "15:37", abbrev, "Resolved", "done")
        got = parse_atom("x", _feed(_entry(updated="2026-08-06T19:37:00Z", content=block)))
        assert got[0].created_at == datetime(2026, 8, 6, 19, 37, tzinfo=UTC), abbrev


def test_a_timestamp_fallback_is_counted():
    """Counted so a provider whose format we cannot resolve is visible, rather
    than quietly degraded."""
    from freshet.pipeline.metrics import TIMESTAMP_FALLBACK

    before = TIMESTAMP_FALLBACK._value.get()
    weird = _block("Aug", 18, "11:42", "XYZ", "Resolved", "done")
    parse_atom("x", _feed(_entry(updated="2026-08-18T11:42:59Z", content=weird)))
    assert TIMESTAMP_FALLBACK._value.get() == before + 1


def test_a_resolved_timestamp_is_not_counted_as_a_fallback():
    from freshet.pipeline.metrics import TIMESTAMP_FALLBACK

    before = TIMESTAMP_FALLBACK._value.get()
    parse_atom("github", _feed(_entry(content=TWO)))
    assert TIMESTAMP_FALLBACK._value.get() == before


def test_a_feed_declaring_a_dtd_is_refused():
    """Internal entity definitions are how a small XML body becomes an unbounded
    one, and ElementTree's expat parser will expand them. No status feed needs a
    DTD, so refusing the construct is cheaper — and more honest about this
    deliberately stdlib-only ingest path — than adding a parser dependency."""
    hostile = ('<?xml version="1.0"?><!DOCTYPE feed [<!ENTITY a "aaaaaaaaaa">]>'
               '<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
               "<id>tag:x,2005:Incident/1</id><updated>2026-08-18T11:42:59Z</updated>"
               "<title>t</title><content type='html'>&a;</content></entry></feed>")
    assert parse_atom("x", hostile) == []


def test_a_normal_feed_is_not_mistaken_for_a_dtd():
    """The guard must not reject the 41 feeds that merely mention the word."""
    got = parse_atom("github", _feed(_entry(content=TWO)))
    assert len(got) == 2


def test_a_last_resort_record_with_no_prose_is_dropped():
    """Markup that flattens to nothing is not an update; indexing it adds an empty
    chunk that matches every query weakly."""
    assert parse_atom("x", _feed(_entry(content="&lt;ul&gt;&lt;/ul&gt;"))) == []
