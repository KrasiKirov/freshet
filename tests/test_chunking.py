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
