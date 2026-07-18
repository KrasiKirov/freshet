"""Dead-letter support: unprocessable messages are recorded, never dropped.

The envelope keeps the original payload byte-for-byte so a fixed consumer (or
a human) can replay it later, plus enough context to know what failed where.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

DEADLETTER_TOPIC = "deadletter.events"


def build_deadletter(error: str, payload: str, source_topic: str) -> str:
    return json.dumps(
        {
            "error": error,
            "source_topic": source_topic,
            "payload": payload,
            "dead_lettered_at": datetime.now(UTC).isoformat(),
        }
    )
