"""An index built by a different embedder must be reported, not silently abstained.

This is a regression test for a real incident: the live index was left behind by
a debugging run with `--embedder stub`, and every query returned "I don't have
enough recent, relevant evidence" — indistinguishable from a genuinely empty
result. Cosine similarity cannot tell the two apart; provenance can.
"""
import pytest

from freshet.rag.retrieval import check_index_model


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        class _R:
            def __init__(self, r):
                self.r = r
            def fetchall(self):
                return self.r
        return _R(self.rows)


class _Emb:
    def __init__(self, name):
        self.name = name


def test_matching_model_is_silent():
    assert check_index_model(_Conn([("bge", 100)]), _Emb("bge")) is None


def test_empty_index_is_not_a_conflict():
    # A fresh install queries before indexing; that is not a misconfiguration.
    assert check_index_model(_Conn([]), _Emb("bge")) is None


def test_foreign_model_raises_with_both_names():
    with pytest.raises(RuntimeError) as e:
        check_index_model(_Conn([("stub", 6257)]), _Emb("bge"))
    msg = str(e.value)
    assert "stub" in msg and "bge" in msg
    assert "abstain" in msg          # names the symptom the operator actually saw


def test_partial_contamination_still_raises():
    # The real failure was MIXED: most rows re-indexed, a few stale ones left.
    with pytest.raises(RuntimeError, match="stub"):
        check_index_model(_Conn([("bge", 5916), ("stub", 341)]), _Emb("bge"))


def test_legacy_unlabelled_rows_warn_but_do_not_block():
    warning = check_index_model(_Conn([("unknown", 12)]), _Emb("bge"))
    assert warning is not None and "12" in warning


def test_embedder_without_a_name_is_skipped():
    assert check_index_model(_Conn([("bge", 1)]), object()) is None


def test_real_embedders_expose_a_name():
    """The guard is only as good as the provenance the embedders record."""
    from freshet.pipeline.embedding import StubEmbedder
    assert StubEmbedder().name == "stub"
    st = pytest.importorskip("sentence_transformers")  # noqa: F841
    from freshet.pipeline.embedding import make_embedder
    assert make_embedder("bge").name == "BAAI/bge-base-en-v1.5"


def test_production_indexing_cannot_select_the_stub():
    """StubEmbedder produces random unit vectors. It exists for tests and CI, and
    pointing the real indexer at it filled the index with noise that looked like
    data — every query abstained until the provenance column revealed why."""
    import argparse
    import contextlib
    import io

    from freshet.pipeline import embedder

    parser_choices = None
    real_parse = argparse.ArgumentParser.parse_args

    def _capture(self, *a, **k):
        nonlocal parser_choices
        for action in self._actions:
            if action.dest == "embedder":
                parser_choices = action.choices
        raise SystemExit(0)

    argparse.ArgumentParser.parse_args = _capture
    try:
        with contextlib.suppress(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            embedder.main()
    finally:
        argparse.ArgumentParser.parse_args = real_parse

    assert parser_choices == ["bge"], f"production must not offer a stub: {parser_choices}"


def test_the_guard_is_wired_into_the_running_agent():
    """It lost its only caller when the query API was deleted, becoming dead code."""
    import inspect

    from freshet.autopilot import __main__ as entry

    assert "check_index_model(conn, embedder)" in inspect.getsource(entry.main)
