"""Present a sample of live labels for HUMAN review, and record the verdicts.

`labels_live.json` is `assistant-reviewed`: the same system that produced the
labels also judged them. That is not a signature, and RESULTS says so. This tool
draws a reproducible sample, prints what a reviewer actually has to judge, and
writes their verdicts back with an audit trail.

One question per row, now that queries are real provider text rather than
generated: does the CAUSE sentence name a cause, or only mention one?

    make review-labels              # print the sample
    make review-labels ARGS='--apply reject=3,7,11'
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random

LABELS = pathlib.Path("freshet/eval/fixtures/labels_live.json")
SAMPLE_SEED = 20260819          # fixed: the same 20 rows every run
SAMPLE_SIZE = 20


def load() -> dict:
    return json.loads(LABELS.read_text())


def sample(labels: list[dict], size: int = SAMPLE_SIZE, seed: int = SAMPLE_SEED
           ) -> list[tuple[int, dict]]:
    """A reproducible, service-stratified sample.

    Stratified because 64 labels are dominated by a few chatty providers, and 20
    rows of GitHub would say nothing about the other 24 services.
    """
    by_service: dict[str, list[tuple[int, dict]]] = {}
    for i, entry in enumerate(labels):
        by_service.setdefault(entry["service"], []).append((i, entry))
    rng = random.Random(seed)
    for rows in by_service.values():
        rng.shuffle(rows)
    picked: list[tuple[int, dict]] = []
    services = sorted(by_service)
    round_ = 0
    while len(picked) < min(size, len(labels)):
        for svc in services:                     # one per service per pass
            if round_ < len(by_service[svc]) and len(picked) < size:
                picked.append(by_service[svc][round_])
        round_ += 1
    return sorted(picked, key=lambda p: p[0])


def render(picked: list[tuple[int, dict]]) -> str:
    out = [f"{len(picked)} labels to review. Judge ONE thing per row:",
           "  does the CAUSE name a cause, or only mention one?",
           "  (Q is the incident's own first update, verbatim — nothing generated.)",
           ""]
    for n, (idx, e) in enumerate(picked, 1):
        out.append(f"[{n}] idx={idx}  {e['service']} — {e['title'][:66]}")
        out.append(f"     Q: {e['query'][:96]}")
        for note in e["notes"][:1]:
            out.append(f"     C: {note[:96]}")
        out.append("")
    out.append("Reject the bad ones:")
    out.append("  make review-labels ARGS='--apply reject=2,9'   (numbers in [brackets])")
    return "\n".join(out)


def apply_verdicts(doc: dict, picked: list[tuple[int, dict]],
                   rejected: set[int]) -> dict:
    """Drop rejected rows and mark the file human-reviewed."""
    reject_idx = {picked[n - 1][0] for n in rejected}
    removed = [{"service": doc["labeled"][i]["service"],
                "title": doc["labeled"][i]["title"]} for i in sorted(reject_idx)]
    doc["labeled"] = [e for i, e in enumerate(doc["labeled"]) if i not in reject_idx]
    doc["curated"] = "reviewed"
    doc.setdefault("_review", {})["human_review"] = {
        "sampled": len(picked),
        "rejected": removed,
        "note": ("A human read the sample and ruled on each row. Rows outside the "
                 "sample were not individually re-read."),
    }
    return doc


def main() -> None:
    p = argparse.ArgumentParser(description="Human review of live eval labels")
    p.add_argument("--apply", default=None,
                   help="record verdicts, e.g. reject=2,9 (or reject= for none)")
    args = p.parse_args()

    doc = load()
    picked = sample(doc["labeled"])
    if args.apply is None:
        print(render(picked))
        return

    _, _, values = args.apply.partition("reject=")
    rejected = {int(v) for v in values.split(",") if v.strip()}
    bad = [n for n in rejected if not 1 <= n <= len(picked)]
    if bad:
        raise SystemExit(f"no such row(s): {bad} (expected 1..{len(picked)})")
    doc = apply_verdicts(doc, picked, rejected)
    LABELS.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"recorded: {len(rejected)} rejected, {len(doc['labeled'])} labels remain, "
          f"curated = reviewed")


if __name__ == "__main__":
    main()
