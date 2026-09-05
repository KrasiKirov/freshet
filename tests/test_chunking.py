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


def test_max_chars_default_is_late_bound():
    """A caller may rebind the module default at runtime; a default bound at
    import time would silently ignore that and every call would keep using
    whatever value was in effect when the module first loaded."""
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
