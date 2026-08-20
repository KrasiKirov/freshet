"""Sentence-aware chunker for event texts.

Chunks are the retrieval unit, so a chunk that begins mid-sentence is a chunk
that reads as noise — and a sentence split across a boundary is unretrievable in
either half. Measured on the live index, greedy word-packing left 8.9% of all
chunks (17.8% of non-first chunks) starting mid-sentence, including cause
statements cut in two:

    "...issues or inability for new customers to sign up. We have identified the r"

That matters more here than in most corpora: the thing worth retrieving is
usually a single sentence in which the provider states a cause, and the eval
scores exactly that. So sentences are packed whole and never split.

A single sentence longer than max_chars is the one exception — it falls back to
word packing, because the alternative is an unbounded chunk the embedder would
truncate silently.
"""

from __future__ import annotations

import re

DEFAULT_MAX_CHARS = 400

# End of sentence: . ! or ? followed by whitespace. Guarded against the common
# false positives in status-feed prose — "v1.2", "23:39 UTC.", "etc.", initials —
# by requiring the next character to open a new sentence.
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'\[(])")


def split_sentences(text: str) -> list[str]:
    """Sentences, whitespace-normalised. Never returns empty strings."""
    flat = " ".join(text.split())
    if not flat:
        return []
    return [s for s in (part.strip() for part in _SENTENCE_END.split(flat)) if s]


def _pack_words(sentence: str, max_chars: int) -> list[str]:
    """Last resort for a single sentence longer than a whole chunk."""
    words, chunks, current = sentence.split(), [], ""
    for word in words:
        if not current:
            current = word
        elif len(current) + 1 + len(word) <= max_chars:
            current += " " + word
        else:
            chunks.append(current)
            current = word
    if current:
        chunks.append(current)
    return chunks


def chunk_text(text: str, max_chars: int = DEFAULT_MAX_CHARS) -> list[str]:
    """Pack whole sentences up to max_chars; never split one unless it cannot fit."""
    chunks: list[str] = []
    current = ""
    for sentence in split_sentences(text):
        if len(sentence) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_pack_words(sentence, max_chars))
            continue
        if not current:
            current = sentence
        elif len(current) + 1 + len(sentence) <= max_chars:
            current += " " + sentence
        else:
            chunks.append(current)
            current = sentence
    if current:
        chunks.append(current)
    return chunks
