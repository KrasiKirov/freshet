"""The human-review tool must sample reproducibly and record verdicts honestly."""
import json

from freshet.eval.review_labels import apply_verdicts, sample


def _labels(n=40):
    services = ["github", "hubspot", "cloudflare", "asana", "grafana"]
    return [{"service": services[i % len(services)], "title": f"incident {i}",
             "query": f"why did {i} break?", "notes": [f"caused by {i}"],
             "cause_event_ids": [f"e{i}"]} for i in range(n)]


def test_the_sample_is_reproducible():
    labels = _labels()
    assert [i for i, _ in sample(labels)] == [i for i, _ in sample(labels)]


def test_the_sample_spreads_across_services():
    """64 labels are dominated by a few chatty providers; 20 rows of GitHub would
    say nothing about the other 24 services."""
    picked = sample(_labels(), size=10)
    assert len({e["service"] for _, e in picked}) == 5


def test_the_sample_never_exceeds_the_corpus():
    assert len(sample(_labels(3), size=20)) == 3


def test_applying_verdicts_drops_only_the_rejected_rows():
    doc = {"labeled": _labels(), "curated": "assistant-reviewed"}
    picked = sample(doc["labeled"], size=10)
    before = len(doc["labeled"])
    rejected_titles = {picked[0][1]["title"], picked[2][1]["title"]}

    out = apply_verdicts(doc, picked, {1, 3})
    assert len(out["labeled"]) == before - 2
    assert rejected_titles.isdisjoint({e["title"] for e in out["labeled"]})


def test_applying_verdicts_marks_the_file_human_reviewed():
    doc = {"labeled": _labels(), "curated": "assistant-reviewed"}
    out = apply_verdicts(doc, sample(doc["labeled"], size=10), set())
    assert out["curated"] == "reviewed"
    hr = out["_review"]["human_review"]
    assert hr["sampled"] == 10 and hr["rejected"] == []
    # the claim must stay scoped to what was actually read
    assert "not individually re-read" in hr["note"]


def test_rejecting_nothing_still_records_the_review():
    doc = {"labeled": _labels(), "curated": "assistant-reviewed"}
    out = apply_verdicts(doc, sample(doc["labeled"], size=10), set())
    assert len(out["labeled"]) == 40
    assert json.dumps(out)          # serialisable
