"""Snapshot the status-poller's five public Statuspage feeds into committed
fixtures for the real-data validation set.

The eval must be deterministic, so it runs against these snapshots — not the
live feeds. Re-running this script refreshes the snapshots (and invalidates
the hand-curated labels in labels.json, which reference incident ids: re-curate
after refreshing).

Run:
    python scripts/fetch_real_incidents.py
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from freshet.ingest.status_poller import SOURCES, fetch  # noqa: E402

OUT = pathlib.Path("freshet/eval/fixtures/real")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, url in SOURCES:
        data = fetch(url, timeout=20.0)
        if data is None:
            print(f"{name}: FETCH FAILED — keeping existing snapshot if any")
            continue
        incidents = data.get("incidents", [])
        (OUT / f"{name}.json").write_text(json.dumps(data, indent=1))
        n_upd = sum(len(i.get("incident_updates", [])) for i in incidents)
        resolved = sum(1 for i in incidents
                       if i.get("status") in ("resolved", "postmortem"))
        print(f"{name}: {len(incidents)} incidents ({resolved} resolved), "
              f"{n_upd} updates")


if __name__ == "__main__":
    main()
