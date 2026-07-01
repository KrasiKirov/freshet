"""Matplotlib charts for the eval harness. Agg backend — writes PNGs, never
opens a window. Imported only by run_eval and the (eval-extra) plot tests."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

_METRICS = ["recall@5", "precision@5", "mrr", "ndcg@5"]


def plot_retrieval_quality(metrics_by_mode: dict[str, dict[str, float]], out_path: str) -> None:
    modes = list(metrics_by_mode)
    x = range(len(_METRICS))
    width = 0.8 / max(1, len(modes))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for i, mode in enumerate(modes):
        vals = [metrics_by_mode[mode].get(m, 0.0) for m in _METRICS]
        ax.bar([xi + i * width for xi in x], vals, width, label=mode)
    ax.set_xticks([xi + width * (len(modes) - 1) / 2 for xi in x])
    ax.set_xticklabels(_METRICS)
    ax.set_ylim(0, 1)
    ax.set_ylabel("score")
    ax.set_title("Retrieval quality by mode (higher is better)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_streaming_vs_batch(
    sample_times: list[float],
    streaming: list[float | None],
    batch: list[float | None],
    out_path: str,
    batch_interval_s: float,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(sample_times, streaming, label="streaming", linewidth=2)
    ax.plot(sample_times, batch, label=f"batch (every {int(batch_interval_s)}s)", linewidth=2)
    ax.set_yscale("symlog")
    ax.set_xlabel("time (s)")
    ax.set_ylabel("data staleness (s, symlog)")
    ax.set_title("Data staleness: streaming vs batch (lower is fresher)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_timeseries(
    times: list[float],
    values: list[float],
    out_path: str,
    title: str,
    ylabel: str,
    markers: list[tuple[float, str]] | None = None,
) -> None:
    """Single metric over time with optional labeled vertical markers (e.g.
    'worker killed'). Used by the failure drills."""
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(times, values, linewidth=2)
    for x, label in markers or []:
        ax.axvline(x, color="0.5", linestyle="--", linewidth=1)
        ax.text(x, ax.get_ylim()[1], label, rotation=90,
                va="top", ha="right", fontsize=8, color="0.4")
    ax.set_xlabel("time (s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
