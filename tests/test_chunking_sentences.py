"""A chunk is the retrieval unit, so it must not begin mid-sentence.

Measured on the live index, greedy word-packing left 8.9% of chunks (17.8% of
non-first chunks) starting mid-sentence — including cause statements cut in two,
which made them unretrievable in either half. The thing worth retrieving here is
usually one sentence stating a cause, so sentences are packed whole.
"""
from freshet.pipeline.chunking import DEFAULT_MAX_CHARS, chunk_text, split_sentences


def test_sentences_are_never_split():
    a = "Users report elevated errors on the API. " * 12          # ~480 chars
    chunks = chunk_text(a)
    assert len(chunks) > 1, "need multiple chunks for this to mean anything"
    for c in chunks:
        assert c[0].isupper(), f"chunk begins mid-sentence: {c[:40]!r}"
        assert c.rstrip().endswith("."), f"chunk ends mid-sentence: {c[-40:]!r}"


def test_a_cause_sentence_survives_intact():
    """The real failure: the cause began at the tail of one chunk and was cut."""
    filler = "We are investigating reports of degraded performance. " * 7
    cause = "The incident was caused by a configuration change that exhausted the connection pool."
    chunks = chunk_text(filler + cause)
    assert any(cause in c for c in chunks), "the cause statement must live in one chunk"


def test_chunks_respect_the_size_limit():
    text = "A short sentence here. " * 60
    assert all(len(c) <= DEFAULT_MAX_CHARS for c in chunk_text(text))


def test_a_single_oversized_sentence_falls_back_to_word_packing():
    """Better a split than an unbounded chunk the embedder silently truncates."""
    monster = "word " * 200                       # one 1000-char "sentence"
    chunks = chunk_text(monster)
    assert len(chunks) > 1
    assert all(len(c) <= DEFAULT_MAX_CHARS for c in chunks)


def test_short_text_is_a_single_chunk():
    assert chunk_text("Elevated errors in eu-west.") == ["Elevated errors in eu-west."]


def test_blank_text_yields_nothing():
    assert chunk_text("") == [] and chunk_text("   \n ") == []


def test_abbreviations_and_timestamps_do_not_split_sentences():
    """Status-feed prose is full of '23:39 UTC.' and 'v1.2' — a naive split on
    '.' would shatter it into fragments."""
    text = "Delays began at 23:39 UTC. We deployed v1.2 to mitigate. Impact is resolved."
    assert split_sentences(text) == [
        "Delays began at 23:39 UTC.",
        "We deployed v1.2 to mitigate.",
        "Impact is resolved.",
    ]


def test_whitespace_is_normalised():
    assert split_sentences("One.\n\n  Two.") == ["One.", "Two."]
