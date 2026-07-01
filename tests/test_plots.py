import pytest

pytest.importorskip("matplotlib")  # plotting is the [eval] extra; CI ([test]) skips this file


def test_retrieval_quality_plot_writes_png(tmp_path):
    from freshet.eval.plots import plot_retrieval_quality

    out = tmp_path / "rq.png"
    metrics = {
        "vector": {"recall@5": 0.6, "precision@5": 0.3, "mrr": 0.5, "ndcg@5": 0.55},
        "keyword": {"recall@5": 0.5, "precision@5": 0.25, "mrr": 0.4, "ndcg@5": 0.45},
        "hybrid": {"recall@5": 0.8, "precision@5": 0.4, "mrr": 0.7, "ndcg@5": 0.75},
    }
    plot_retrieval_quality(metrics, str(out))
    assert out.exists() and out.stat().st_size > 0


def test_staleness_plot_writes_png(tmp_path):
    from freshet.eval.plots import plot_streaming_vs_batch

    out = tmp_path / "sb.png"
    times = [0.0, 5.0, 10.0, 15.0, 20.0]
    streaming = [3.0, 3.0, 3.0, 3.0, 3.0]
    batch = [None, 5.0, 10.0, 5.0, 10.0]
    plot_streaming_vs_batch(times, streaming, batch, str(out), batch_interval_s=10.0)
    assert out.exists() and out.stat().st_size > 0


def test_timeseries_plot_writes_png(tmp_path):
    from freshet.eval.plots import plot_timeseries

    out = tmp_path / "ts.png"
    plot_timeseries(
        times=[0.0, 2.0, 4.0, 6.0, 8.0],
        values=[0.0, 40.0, 80.0, 30.0, 0.0],
        out_path=str(out),
        title="consumer lag during worker kill/restart",
        ylabel="lag (messages)",
        markers=[(4.0, "embedder killed"), (5.0, "embedder restarted")],
    )
    assert out.exists() and out.stat().st_size > 0
