import json
from pathlib import Path

from freshet.eval import answer_eval


def test_aggregate_means_per_config():
    records = [
        {"config": "extractive", "faithfulness": 1.0, "answer_relevance": 0.6},
        {"config": "extractive", "faithfulness": 1.0, "answer_relevance": 0.4},
        {"config": "narrative", "faithfulness": 0.8, "answer_relevance": 0.9},
    ]
    out = answer_eval.aggregate(records)
    assert out["configs"]["extractive"] == {"faithfulness": 1.0, "answer_relevance": 0.5, "incidents": 2}
    assert out["configs"]["narrative"] == {"faithfulness": 0.8, "answer_relevance": 0.9, "incidents": 1}
    assert "note" in out


def test_main_skips_cleanly_without_key(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    out = tmp_path / "answer_eval.json"
    monkeypatch.setattr(answer_eval, "RESULTS", str(out))
    answer_eval.main()
    assert "skipped" in capsys.readouterr().out.lower()
    assert not out.exists()
