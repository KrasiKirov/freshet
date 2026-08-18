from datetime import UTC, datetime

from freshet.ingest.statuspage import parse_atom

FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>GitHub Status - Incident History</title>
  <entry>
    <id>tag:www.githubstatus.com,2005:Incident/31199495</id>
    <published>2026-08-18T11:00:00Z</published>
    <updated>2026-08-18T11:42:59Z</updated>
    <title>Intermittent failures in runner groups</title>
    <content type="html">&lt;p&gt;&lt;small&gt;Aug 18, 11:42 UTC&lt;/small&gt;&lt;br&gt;&lt;strong&gt;Resolved&lt;/strong&gt; - This incident has been resolved.&lt;/p&gt;</content>
  </entry>
  <entry>
    <id>tag:www.githubstatus.com,2005:Incident/31100001</id>
    <published>2026-08-17T09:00:00Z</published>
    <updated>2026-08-17T09:30:00Z</updated>
    <title>Elevated API errors</title>
    <content type="html">&lt;p&gt;&lt;strong&gt;Investigating&lt;/strong&gt; - We are looking into elevated errors.&lt;/p&gt;</content>
  </entry>
</feed>
"""


def test_parses_every_entry_oldest_first():
    got = parse_atom("github", FEED)
    assert [u.incident_id for u in got] == ["31100001", "31199495"]
    assert got[0].provider == "github"
    assert got[0].incident_name == "Elevated API errors"


def test_created_at_is_the_revision_time_in_utc():
    got = parse_atom("github", FEED)
    assert got[1].created_at == datetime(2026, 8, 18, 11, 42, 59, tzinfo=UTC)
    assert got[1].created_at.utcoffset().total_seconds() == 0


def test_update_id_tracks_the_revision_so_a_new_update_is_a_new_key():
    """The feed carries one entry per incident, revised in place. Keying on the
    revision timestamp is what makes each new update a distinct dedup key."""
    first = parse_atom("github", FEED)[1]
    revised = parse_atom("github", FEED.replace("11:42:59Z", "12:10:00Z"))[1]
    assert first.incident_id == revised.incident_id
    assert first.dedup_key != revised.dedup_key


def test_status_and_text_are_extracted_as_plain_text():
    got = parse_atom("github", FEED)
    assert got[1].status == "resolved"
    assert "<" not in got[1].text and "&lt;" not in got[1].text
    assert "This incident has been resolved." in got[1].text


def test_empty_and_malformed_feeds_yield_nothing_rather_than_raising():
    assert parse_atom("x", "") == []
    assert parse_atom("x", "<not-xml") == []
    assert parse_atom("x", '<feed xmlns="http://www.w3.org/2005/Atom"></feed>') == []
