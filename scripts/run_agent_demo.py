#!/usr/bin/env python3
"""Agent demo: investigate one benchmark incident and save the transcript.

Requires ANTHROPIC_API_KEY (reads from .env.local if present). Assumes the
stack is up (make up). Indexes the benchmark corpus directly into the DB.

Usage:
    python3 scripts/run_agent_demo.py
    make agent-demo
"""
import json
import os
import sys
from pathlib import Path

# Load ANTHROPIC_API_KEY from .env.local if present
_env_file = Path(__file__).parent.parent / ".env.local"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

if not os.environ.get("ANTHROPIC_API_KEY"):
    print("ERROR: ANTHROPIC_API_KEY not set (and not found in .env.local)")
    sys.exit(1)

from freshet.api.agent import investigate
from freshet.common.db import connect
from freshet.eval.rootcause import _index_corpus
from freshet.generator.generator import build_benchmark
from freshet.pipeline.embedding import make_embedder

TRANSCRIPT = "results/agent_transcript.md"


def _render(service: str, incident_id: str, archetype: str,
            inv, truth_cause: str, truth_fix: str) -> str:
    lines = [f"# Agent investigation: {service}", ""]
    lines.append(f"**Incident:** {incident_id} | **Archetype:** {archetype}")
    lines.append("")

    for entry in inv.transcript:
        step = entry.get("step", "?")
        role = entry["role"]

        if role == "tool_call":
            lines.append(f"## Step {step}: `{entry['name']}`")
            lines.append("```json")
            lines.append(json.dumps(entry["input"], indent=2))
            lines.append("```")
            preview = entry.get("result_preview", "")[:200]
            lines.append(f"**Result preview:** `{preview}`")
            lines.append("")
        elif role == "assistant":
            lines.append(f"## Step {step}: Model reasoning")
            lines.append(entry.get("text", ""))
            lines.append("")
        elif role == "submit_findings":
            lines.append(f"## Step {step}: submit\\_findings")
            lines.append(f"- **cause\\_id:** `{entry.get('cause_id')}`")
            lines.append(f"- **fix\\_id:** `{entry.get('fix_id')}`")
            lines.append(f"- **narrative:** {entry.get('narrative', '')}")
            lines.append("")

    lines.append("---")
    lines.append(f"**Steps used:** {inv.steps}")
    lines.append(
        f"**Cause hit:** {inv.cause_id == truth_cause} "
        f"(expected `{truth_cause}`, got `{inv.cause_id}`)"
    )
    lines.append(
        f"**Fix hit:** {inv.fix_id == truth_fix} "
        f"(expected `{truth_fix}`, got `{inv.fix_id}`)"
    )
    return "\n".join(lines)


def main() -> None:
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()

    corpus, truths = build_benchmark(seed=1, n_incidents=40)
    _index_corpus(conn, embedder, corpus)

    truth = truths[0]  # first incident — deploy_regression archetype
    print(f"Investigating: {truth.incident_id} ({truth.archetype}) — service={truth.service}")
    print(f"  Ground truth: cause={truth.cause_id}, fix={truth.fix_id}")
    print()

    inv = investigate(conn, embedder, truth.service)

    text = _render(
        truth.service, truth.incident_id, truth.archetype,
        inv, truth.cause_id, truth.fix_id,
    )
    print(text)

    os.makedirs("results", exist_ok=True)
    Path(TRANSCRIPT).write_text(text)
    print(f"\nTranscript saved to {TRANSCRIPT}")
    conn.close()


if __name__ == "__main__":
    main()
