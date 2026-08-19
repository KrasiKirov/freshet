"""Citation verification: the LLM may only cite evidence it was given.

This exists because the LLM is now the DEFAULT author of the Slack brief. A
fabricated `[event_id @ timestamp]` would look authoritative to a responder and
be unverifiable — the worst failure mode this system has.
"""
from datetime import UTC, datetime
from types import SimpleNamespace

from freshet.rag.composer import AnthropicComposer, verify_citations


def _hit(eid, text="something happened"):
    return SimpleNamespace(event_id=eid, source="alert", text=text,
                           ts=datetime(2026, 8, 18, 12, 0, tzinfo=UTC))


def test_a_citation_for_evidence_that_was_provided_survives():
    hits = [_hit("github:abc:123")]
    answer = "Errors are elevated [github:abc:123 @ 2026-08-18 12:00:00]."
    assert verify_citations(answer, hits) == answer


def test_a_fabricated_citation_is_removed():
    hits = [_hit("github:abc:123")]
    answer = ("Errors are elevated [github:abc:123 @ 2026-08-18 12:00:00] "
              "caused by a bad deploy [github:INVENTED:999 @ 2026-08-18 11:00:00].")
    cleaned = verify_citations(answer, hits)
    assert "INVENTED" not in cleaned
    assert "github:abc:123" in cleaned, "real citations must be kept"


def test_prose_survives_when_a_citation_is_stripped():
    hits = [_hit("a:b:c")]
    cleaned = verify_citations("The service recovered [made:up:id @ 2026-01-01 00:00:00].", hits)
    assert "The service recovered" in cleaned


def test_composer_verifies_what_the_model_returns():
    """End to end through the composer with an injected client, so no API key is
    needed and the fabrication path is actually exercised."""
    class FakeClient:
        class messages:
            @staticmethod
            def create(**kw):
                return SimpleNamespace(content=[SimpleNamespace(
                    type="text",
                    text="Down [real:1:1 @ 2026-08-18 12:00:00] and [fake:9:9 @ 2026-08-18 12:00:00].")])

    composer = AnthropicComposer(client=FakeClient())
    out = composer.compose("what happened?", [_hit("real:1:1")])
    assert "real:1:1" in out
    assert "fake:9:9" not in out


def test_a_real_id_with_a_fabricated_timestamp_is_removed():
    """Checking only the event_id let the model attach any timestamp it liked to
    a real event — a subtler fabrication than inventing an id, and just as
    misleading to a responder reading a timeline."""
    hits = [_hit("github:abc:123")]
    answer = "Errors began [github:abc:123 @ 2020-01-01 00:00:00]."
    assert "2020-01-01" not in verify_citations(answer, hits)


def test_the_correct_timestamp_survives():
    hits = [_hit("github:abc:123")]
    answer = "Errors began [github:abc:123 @ 2026-08-18 12:00:00]."
    assert verify_citations(answer, hits) == answer


def test_whitespace_variation_in_a_citation_is_tolerated():
    """The model reproduces the citation it was shown; trivial spacing drift must
    not be treated as fabrication."""
    hits = [_hit("github:abc:123")]
    answer = "Errors began [github:abc:123  @  2026-08-18 12:00:00]."
    assert "github:abc:123" in verify_citations(answer, hits)
