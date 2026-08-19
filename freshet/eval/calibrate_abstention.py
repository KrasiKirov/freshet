"""Propose an abstention floor from measured similarities — or decline to.

`pipeline/embedding.py` has long referenced `scripts/calibrate_abstention.py`,
which does not exist. This is that tool, built to the constraint that matters:
calibrate ONLY against paraphrased live labels. The frozen fixture's questions
are title-derived, so they share vocabulary with the documents and would justify
a floor that real on-call language never clears.

The live eval measured 33 of 64 paraphrased questions abstaining at the current
0.70 floor while the evidence was indexed. Either the floor is wrong or
retrieval is — and the two are distinguishable: if on-corpus and off-corpus
similarities OVERLAP, no threshold separates them and the floor is not the bug.

Prints a proposal. Never writes MIN_SIMILARITY_BGE: moving a floor is a
deliberate act, and a tool that retunes itself to its own benchmark is how a
metric stops meaning anything.

Run: make calibrate-abstention
"""

from __future__ import annotations

import json
import os
import pathlib

from freshet.eval.retrieval_eval import LIVE_LABELS, OFF_CORPUS

K = 5


def max_similarity(hits) -> float:
    return max((h.similarity for h in hits), default=0.0)


def propose_floor(on_corpus: list[float], off_corpus: list[float],
                  current: float) -> dict:
    """Midpoint between the arms, only when they actually separate."""
    if not on_corpus or not off_corpus:
        return {"proposal": None, "reason": "not enough samples"}
    lowest_on, highest_off = min(on_corpus), max(off_corpus)
    if lowest_on <= highest_off:
        return {
            "proposal": None,
            "current": current,
            "lowest_on_corpus": round(lowest_on, 3),
            "highest_off_corpus": round(highest_off, 3),
            "reason": (
                "on-corpus and off-corpus similarities OVERLAP: no threshold "
                "separates them, so the floor is not what is losing these "
                "questions — retrieval is. Keep the current floor and record that."),
        }
    proposed = round((lowest_on + highest_off) / 2, 3)
    return {
        "proposal": proposed,
        "current": current,
        "lowest_on_corpus": round(lowest_on, 3),
        "highest_off_corpus": round(highest_off, 3),
        "reason": ("a gap exists; the midpoint keeps every off-corpus question "
                   "abstaining while admitting every on-corpus one"),
    }


def main() -> None:
    from freshet.api.retrieval import hybrid_search
    from freshet.common.db import connect
    from freshet.pipeline.embedding import make_embedder

    labels = json.loads(pathlib.Path(LIVE_LABELS).read_text())
    conn = connect()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    current = float(getattr(embedder, "min_similarity", 0.3))

    # min_similarity=0 so nothing abstains: this measures the distribution the
    # floor is supposed to cut, not what survives the current cut.
    on_all, on_with_cause = [], []
    for entry in labels["labeled"]:
        r = hybrid_search(conn, embedder, entry["query"], k=K, min_similarity=0.0)
        sim = max_similarity(r.hits)
        on_all.append(sim)
        if {h.event_id for h in r.hits} & set(entry["cause_event_ids"]):
            on_with_cause.append(sim)          # only answerable questions bound the floor
    off = [max_similarity(hybrid_search(conn, embedder, q, k=K, min_similarity=0.0).hits)
           for q in OFF_CORPUS]

    report = {
        "on_corpus": {"n": len(on_all), "n_with_cause_retrieved": len(on_with_cause),
                      "min": round(min(on_all), 3), "max": round(max(on_all), 3)},
        "off_corpus": {"n": len(off), "min": round(min(off), 3),
                       "max": round(max(off), 3)},
        **propose_floor(on_with_cause, off, current),
    }
    out = pathlib.Path("results/abstention_calibration.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
