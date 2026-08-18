"""Pure stream logic, deliberately free of any Flink import.

Keeping these as plain functions means the behaviour is unit-tested in
milliseconds rather than by booting a cluster, and if the Flink job is ever
replaced by a plain consumer the logic survives untouched. `dedup_job.py` is thin
glue over this module.
"""
from __future__ import annotations

BURST_THRESHOLD = 3     # distinct providers within one window
WINDOW_SECONDS = 300    # 5-minute event-time tumbling window

# Statuspage's own vocabulary. Incidents move investigating -> identified ->
# monitoring -> resolved; scheduled maintenance moves scheduled -> in_progress ->
# verifying -> completed. Only the edges are lifecycle transitions: the middle
# states are progress reports, and firing on them would brief the same incident
# repeatedly.
_OPENS = frozenset({"investigating", "identified"})
_CLOSES = frozenset({"resolved", "completed"})


def dedup_key(record: dict) -> str:
    """Identity of one update. Must match `IncidentUpdate.dedup_key`, because the
    poller keys its Kafka messages with it."""
    return f"{record['provider']}:{record['incident_id']}:{record['update_id']}"


def is_burst(providers: list[str], threshold: int = BURST_THRESHOLD) -> bool:
    """True when enough DISTINCT providers reported inside one window.

    Distinctness is the whole point: one provider posting ten updates about its
    own outage is routine, whereas three unrelated providers degrading together
    suggests a shared upstream cause.
    """
    return len(set(providers)) >= threshold


def lifecycle_for(status: str) -> str | None:
    """Map a feed status to an incident lifecycle transition, or None.

    v1 inferred lifecycle by correlating event types; the status feeds state it
    outright, so this is a lookup rather than a heuristic. Returns "opened",
    "resolved", or None for intermediate and unrecognised states.
    """
    normalized = status.strip().lower()
    if normalized in _OPENS:
        return "opened"
    if normalized in _CLOSES:
        return "resolved"
    return None
