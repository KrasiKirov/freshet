"""Paraphrased eval queries must be VERIFIED, not merely requested.

Questions generated from an incident title reuse its words, and every indexed
chunk carries that title as a prefix — so the keyword arm matches on wording
rather than meaning and its score is flattered. This measures the reuse.
"""
from freshet.eval.label_live import content_words, lexical_overlap


def test_content_words_drop_stopwords_and_short_tokens():
    assert content_words("The API is down for some of our users") == {
        "api", "down", "users"}


def test_a_verbatim_title_scores_total_overlap():
    title = "Elevated error rates in the IAD region"
    assert lexical_overlap(f"What caused {title}?", title) == 1.0


def test_a_true_paraphrase_scores_low():
    title = "Elevated error rates in the IAD region"
    q = "Why did Virginia traffic start failing so often?"
    assert lexical_overlap(q, title) == 0.0


def test_naming_the_service_is_not_counted_as_leakage():
    # An engineer knows which product they are asking about; that is realistic
    # context, not vocabulary leakage.
    title = "Figma service disruption"
    assert lexical_overlap("Why did Figma break?", title, service="figma") == 0.0


def test_partial_reuse_is_measured_not_rounded_away():
    title = "Database connection pool exhausted"
    # reuses 'database' only, of {database, connection, pool, exhausted}
    assert lexical_overlap("Why did the database stop responding?", title) == 0.25


def test_a_title_with_no_content_words_does_not_divide_by_zero():
    assert lexical_overlap("Why did it break?", "The", service="") == 0.0
