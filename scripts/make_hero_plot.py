"""Generate the annotated README hero from the real streaming-vs-batch staleness
model (freshet.eval.run_eval.staleness_curves — pure, no stack needed). Writes
docs/hero-freshness.png. Kept separate from results/streaming_vs_batch.png (the
plain eval artifact that `make eval` regenerates) so the annotated hero is durable.

    .venv/bin/python scripts/make_hero_plot.py
"""
from __future__ import annotations

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from freshet.eval.run_eval import staleness_curves

STREAMING_FRESHNESS_S = 3.0     # matches run_eval default (measured p50, see RESULTS.md)
BATCH_INTERVAL_S = 3600.0       # hourly batch as a demo proxy for a nightly index
OUT = "docs/hero-freshness.png"

BLUE, ORANGE = "#1f77b4", "#ff7f0e"


def main() -> None:
    samples, streaming, batch = staleness_curves(STREAMING_FRESHNESS_S, BATCH_INTERVAL_S)
    s_clean = [s for s in streaming if s is not None]
    b_clean = [b for b in batch if b is not None]
    mean_s = sum(s_clean) / len(s_clean)
    mean_b = sum(b_clean) / len(b_clean)
    ratio = mean_b / mean_s
    peak_min = max(b_clean) / 60.0
    hours = [t / 3600.0 for t in samples]

    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.plot(hours, streaming, color=BLUE, linewidth=2.5)
    ax.plot(hours, batch, color=ORANGE, linewidth=2.5)
    ax.set_yscale("symlog")
    ax.set_ylim(top=2e4)   # headroom above the ~3.3k peaks for a clean label strip

    ax.set_title(f"Streaming keeps data ~{mean_s:.0f}s fresh — ~{ratio:.0f}× fresher "
                 f"than an hourly batch index", fontsize=15, fontweight="bold", pad=14)
    ax.set_xlabel("time (hours)", fontsize=12)
    ax.set_ylabel("data staleness (seconds, log) — lower is fresher", fontsize=12)
    ax.margins(x=0.01)

    # Direct line labels (no legend to decode).
    ax.text(2.0, 1.6, "streaming pipeline: always a few seconds old",
            color=BLUE, fontsize=12, fontweight="bold", va="top")
    ax.text(0.05, 8000, f"hourly batch: data rots up to ~{peak_min:.0f} min before each refresh",
            color=ORANGE, fontsize=12, fontweight="bold", va="center")

    # Headline callout in open space.
    ax.annotate(f"~{ratio:.0f}× fresher\non average",
                xy=(2.7, mean_b), xytext=(2.75, 40),
                fontsize=17, fontweight="bold", color="#111", ha="center",
                bbox=dict(boxstyle="round,pad=0.4", fc="#fff6e6", ec=ORANGE, lw=1.5),
                arrowprops=dict(arrowstyle="->", color=ORANGE, lw=1.6))

    ax.grid(True, which="major", axis="y", ls=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(OUT, dpi=140)
    plt.close(fig)
    print(f"wrote {OUT}  (streaming mean {mean_s:.1f}s, batch mean {mean_b:.0f}s, "
          f"ratio {ratio:.0f}x, peak {peak_min:.0f} min)")


if __name__ == "__main__":
    main()
