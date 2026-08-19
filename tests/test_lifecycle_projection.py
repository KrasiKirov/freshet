"""The lifecycle topic means FIRST open and FIRST resolve, once per incident.

Partitioning by (provider, incident_id, update_id) emitted 'opened' for every
investigating/identified update, so a long incident re-fired the lifecycle and
the Autopilot re-claimed it each time — only the delivery guard stopped a
duplicate brief. The SQL is Flink's, so these constants document the intent and
fail if the projection drifts from it.
"""
import re
from pathlib import Path

SQL = (Path(__file__).resolve().parents[1] / "freshet/stream/dedup_job.sql").read_text()

OPEN_STATUSES = {"investigating", "identified", "monitoring"}
RESOLVE_STATUSES = {"resolved", "completed"}


def _projection(kind: str) -> str:
    m = re.search(rf"INSERT INTO incident_lifecycle\s*\nSELECT[^;]*?'{kind}' AS `type`[^;]*?;",
                  SQL, re.S)
    assert m, f"no {kind} projection in dedup_job.sql"
    return m.group(0)


def _statuses(block: str) -> set[str]:
    m = re.search(r"LOWER\(status\) IN \(([^)]*)\)", block)
    assert m, "projection must filter on status"
    return set(re.findall(r"'([a-z]+)'", m.group(1)))


def test_open_and_resolve_are_separate_projections():
    assert _statuses(_projection("opened")) == OPEN_STATUSES
    assert _statuses(_projection("resolved")) == RESOLVE_STATUSES


def test_neither_projection_can_fire_twice_for_one_incident():
    for kind in ("opened", "resolved"):
        block = _projection(kind)
        assert "PARTITION BY provider, incident_id\n" in block, (
            f"{kind} must partition by incident, not by update — partitioning by "
            f"update_id fires once per update")
        assert "WHERE seq = 1" in block, f"{kind} must keep only the first row"


def test_the_first_row_is_chosen_by_a_single_time_attribute():
    """Flink only recognises ROW_NUMBER as deduplication (append-only) when the
    ORDER BY is one time attribute. Two sort keys make it a general Rank, whose
    changelog carries updates, and the Kafka sink then refuses the job:
    "doesn't support consuming update and delete changes"."""
    for kind in ("opened", "resolved"):
        block = _projection(kind)
        assert "ORDER BY proc_time ASC)" in block
        assert "created_at ASC," not in block, "a second sort key breaks the sink"


def test_open_and_resolve_statuses_do_not_overlap():
    assert not (OPEN_STATUSES & RESOLVE_STATUSES), (
        "an overlapping status would emit both an open and a resolve")


def test_the_watermark_covers_reemitted_history():
    """A cache-miss sweep re-emits months of Atom history after the watermark
    has moved to 'now'. 90s of allowed lateness dropped those first-seen rows.
    Dedup still orders by proc_time; the watermark only gates late event-time."""
    m = re.search(r"WATERMARK FOR created_at AS created_at - INTERVAL '(\d+)' DAY", SQL)
    assert m and int(m.group(1)) >= 7


def test_parse_errors_are_still_tolerated():
    # A single poison record must not kill the job; those rows are silent drops
    # at the JSON decoder. Rows that parse but have a NULL created_at are
    # routed to a dead-letter sink instead of vanishing.
    assert "'json.ignore-parse-errors' = 'true'" in SQL


def test_checkpointing_is_enabled():
    """Without a checkpoint interval, keyed keep-first state dies on a TM
    restart and the job header's 'checkpointed' claim is false."""
    assert "execution.checkpointing.interval" in SQL


def test_null_created_at_rows_are_dead_lettered():
    assert "deadletter.raw" in SQL
    assert "created_at IS NULL" in SQL


def test_idle_partitions_do_not_hold_the_watermark():
    assert "idle-timeout" in SQL
