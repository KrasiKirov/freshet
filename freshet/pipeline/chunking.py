"""Greedy word-packing chunker for long event texts (e.g. postmortems).

Words are never split; each chunk stays under max_chars. 400 chars keeps a
chunk comfortably inside the embedding model's input window while leaving
retrieval granularity per-paragraph-ish.
"""

from __future__ import annotations

DEFAULT_MAX_CHARS = 400


def chunk_text(text: str, max_chars: int = DEFAULT_MAX_CHARS) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) <= max_chars:
            current += " " + word
        else:
            chunks.append(current)
            current = word
    chunks.append(current)
    return chunks
