from freshet.pipeline.chunking import chunk_text


def test_short_text_is_one_chunk():
    assert chunk_text("error spike on scheduler-api") == ["error spike on scheduler-api"]


def test_long_text_packs_words_under_limit():
    text = " ".join(f"word{i}" for i in range(200))
    chunks = chunk_text(text, max_chars=100)
    assert len(chunks) > 1
    assert all(len(c) <= 100 for c in chunks)
    assert " ".join(chunks) == text


def test_blank_text_is_empty():
    assert chunk_text("") == []
    assert chunk_text("   ") == []


def test_corpus_shape_reports_the_distribution_that_matters():
    """The fixture corpus and the live index have different chunk-length
    distributions (fixture: 159 mean chars, 7.3% multi-chunk; live: 235 and
    40.7%). An embedding change validated only on the fixture is validated on
    the wrong shape — the title-prefix experiment looked free there and cost
    3.6 points of recall live."""
    from freshet.eval.chunk_sweep import corpus_shape

    per_event = [["one chunk"], ["first", "second"], ["a", "b", "c"]]
    s = corpus_shape(per_event)
    assert s["n_chunks"] == 6
    assert s["n_events"] == 3
    assert s["multi_chunk_events"] == 2
    assert abs(s["multi_chunk_frac"] - 2 / 3) < 1e-3
    assert abs(s["non_first_frac"] - 3 / 6) < 1e-9


def test_max_chars_default_is_late_bound():
    """The sweep rebinds the module default; a default bound at import time
    would silently ignore it and report four identical rows."""
    from freshet.pipeline import chunking

    text = "Alpha beta gamma. " * 40
    saved = chunking.DEFAULT_MAX_CHARS
    try:
        chunking.DEFAULT_MAX_CHARS = 100
        small = chunking.chunk_text(text)
        chunking.DEFAULT_MAX_CHARS = 800
        large = chunking.chunk_text(text)
    finally:
        chunking.DEFAULT_MAX_CHARS = saved
    assert len(small) > len(large)
    assert chunking.chunk_text(text, 100) == small        # explicit still wins
