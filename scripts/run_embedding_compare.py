#!/usr/bin/env python3
"""Embedding before/after on the 160-query benchmark: MiniLM (frozen baseline)
vs bge (run live), printing the recall@5 / nDCG@5 delta.

The pgvector column is a fixed dimension (768), so MiniLM (384-dim) and bge
(768-dim) cannot index into the same DB. The MiniLM "before" is therefore a
frozen snapshot of the committed MiniLM run (results/retrieval_metrics_minilm.json,
captured under the prior vector(384) schema); only the bge "after" is run live
here, refreshing results/retrieval_metrics.json. Both halves are deterministic and
reproducible. Needs the stack up on a fresh vector(768) DB."""
import json
import subprocess
import sys
from pathlib import Path

MODES = ["keyword", "vector", "hybrid"]
BASELINE = "results/retrieval_metrics_minilm.json"


def _run_bge() -> dict:
    subprocess.run(
        [sys.executable, "-m", "freshet.eval.run_eval", "--embedder", "bge", "--out", "results"],
        check=True,
    )
    return json.loads(Path("results/retrieval_metrics.json").read_text())["retrieval"]


def _table(label: str, metric: str, mini: dict, bge: dict) -> None:
    print(f"\n{'mode':10}{'minilm':>10}{'bge':>10}{'delta':>9}   ({label})")
    for m in MODES:
        a, b = mini[m][metric], bge[m][metric]
        print(f"{m:10}{a:10.3f}{b:10.3f}{b - a:+9.3f}")


def main() -> None:
    mini = json.loads(Path(BASELINE).read_text())["retrieval"]
    bge = _run_bge()  # canonical, refreshes results/retrieval_metrics.json
    _table("recall@5", "recall@5", mini, bge)
    _table("nDCG@5", "ndcg@5", mini, bge)


if __name__ == "__main__":
    main()
