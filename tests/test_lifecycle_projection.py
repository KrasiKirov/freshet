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
# Statement-splitting must ignore comment text: this file is heavily commented and a
# prose semicolon inside a `--` line truncated a statement mid-match.
CODE = re.sub(r"--[^\n]*", "", SQL)

OPEN_STATUSES = {"investigating", "identified", "monitoring"}
# 'complete' (no 'd') is what hashicorp posts; without it those incidents
# resolve silently and never get a postmortem
RESOLVE_STATUSES = {"resolved", "completed", "complete"}


def _view() -> str:
    """The single source both transitions are derived from. Open and resolve were
    two byte-identical projections until the recency guard was found on only one
    of them; they are now one view plus one INSERT."""
    m = re.search(r"CREATE TEMPORARY VIEW recent_transitions AS.*?;", CODE, re.S)
    assert m, "no recent_transitions view in dedup_job.sql"
    return m.group(0)


def _projection() -> str:
    m = re.search(r"INSERT INTO incident_lifecycle\s*\nSELECT[^;]*?;", CODE, re.S)
    assert m, "no lifecycle projection in dedup_job.sql"
    return m.group(0)


def _statuses(block: str) -> set[str]:
    """The WHERE filter specifically — the view also names statuses inside the CASE
    that classifies the transition, and matching that one instead would silently
    compare the resolve set against itself."""
    m = re.search(r"AND LOWER\(status\) IN \(([^)]*)\)", block, re.S)
    assert m, "the view must filter on status"
    return set(re.findall(r"'([a-z]+)'", m.group(1)))


def test_the_view_admits_exactly_the_open_and_resolve_statuses():
    assert _statuses(_view()) == OPEN_STATUSES | RESOLVE_STATUSES


def test_a_status_is_classified_as_resolved_only_if_it_is_a_resolve_status():
    """The CASE decides the transition; anything not named as a resolve falls to
    'opened'. An open status leaking into the resolve list would post a postmortem
    for an incident that is still burning."""
    case = re.search(r"CASE WHEN LOWER\(status\) IN \(([^)]*)\)\s*\n?\s*THEN 'resolved'",
                     _view())
    assert case, "the view must classify the transition with a CASE"
    assert set(re.findall(r"'([a-z]+)'", case.group(1))) == RESOLVE_STATUSES


def test_neither_transition_can_fire_twice_for_one_incident():
    block = _projection()
    assert "PARTITION BY provider, incident_id, transition\n" in block, (
        "must partition by (incident, transition), not by update — partitioning by "
        "update_id fires once per update, and omitting transition collapses the two")
    assert "WHERE seq = 1" in block, "must keep only the first row"


def test_the_first_row_is_chosen_by_a_single_time_attribute():
    """Flink only recognises ROW_NUMBER as deduplication (append-only) when the
    ORDER BY is one time attribute. Two sort keys make it a general Rank, whose
    changelog carries updates, and the Kafka sink then refuses the job:
    "doesn't support consuming update and delete changes"."""
    block = _projection()
    assert "ORDER BY proc_time ASC)" in block
    assert "created_at ASC," not in block, "a second sort key breaks the sink"


def test_open_and_resolve_statuses_do_not_overlap():
    assert not (OPEN_STATUSES & RESOLVE_STATUSES), (
        "an overlapping status would emit both an open and a resolve")


def test_reemitted_history_can_never_be_dropped_as_late_data():
    """A cache-miss sweep re-emits months of Atom history. Under the old watermark
    those first-seen rows were dropped as late data and never indexed; it was
    widened to 7 days to compensate. With no event-time operator left in the job
    there is no lateness to gate on at all, which settles the problem rather than
    tuning it — so no watermark may come back without a reason to consume it."""
    assert "WATERMARK FOR" not in SQL.upper()
    assert "ORDER BY proc_time ASC)" in _projection(), \
        "dedup must stay on processing time, which no watermark can gate"


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


def test_the_idle_timeout_is_gone_with_the_watermark_it_served():
    """`table.exec.source.idle-timeout` existed to stop a quiet partition pinning the
    watermark. With no watermark left in the job it advances nothing, so keeping it
    set would be one more option that looks load-bearing and is not."""
    assert "SET 'table.exec.source.idle-timeout'" not in SQL
