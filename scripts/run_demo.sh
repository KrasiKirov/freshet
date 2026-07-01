#!/usr/bin/env bash
# One-command demo: stream the scripted incident end to end, then ask about it.
# Assumes `make up`. EMBEDDER=stub skips the model download.
set -euo pipefail
cd "$(dirname "$0")/.."

EMBEDDER="${EMBEDDER:-minilm}"
echo "==> Streaming the scripted incident through the live pipeline..."
EMBEDDER="$EMBEDDER" COUNT=60 SPACING=0.05 bash scripts/run_slice.sh

echo
echo "==> Asking the system about the incident it just ingested:"
EMBEDDER="$EMBEDDER" python3 - <<'EOF'
import os
from freshet.api.composer import make_composer
from freshet.api.retrieval import hybrid_search
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder

emb = make_embedder(os.environ["EMBEDDER"])
conn = connect()
question = "what happened with scheduler-api and how was it resolved?"
result = hybrid_search(conn, emb, question, k=5)
print(f"\nQ: {question}\n")
if result.abstained:
    print("(abstained — not enough evidence)")
else:
    print(make_composer("auto").compose(question, result.hits))
    print("\nciting:")
    for h in result.hits:
        print(f"  [{h.event_id} @ {h.ts:%H:%M:%S}] ({h.source}) {h.text[:70]}")
conn.close()
EOF
