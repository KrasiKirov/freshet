"""The claim is a LEASE, not a tombstone.

Kafka already guarantees redelivery (autopilot consumes with auto_commit=False),
so a crashed brief WILL come back. A permanent claim silently downgrades that
at-least-once guarantee to at-most-once: the redelivered event finds the slot
taken and is skipped forever.

This logic lives entirely in a SQL predicate, so it is tested against real
Postgres — a fake connection would assert nothing about the thing that matters.
"""
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def incident(conn):
    conn.execute("DELETE FROM incidents WHERE incident_id = 'INC-LEASE'")
    conn.execute(
        "INSERT INTO incidents (incident_id, title, opened_at) "
        "VALUES ('INC-LEASE', 'lease test', now())")
    yield "INC-LEASE"
    conn.execute("DELETE FROM incidents WHERE incident_id = 'INC-LEASE'")


def _age_the_claim(conn, incident_id, minutes):
    conn.execute(
        "UPDATE incidents SET briefed_at = now() - (%s * interval '1 minute')"
        " WHERE incident_id = %s", (minutes, incident_id))


def test_a_fresh_claim_blocks_a_second_worker(conn, incident):
    from freshet.autopilot.consumer import claim_incident

    assert claim_incident(conn, incident) is True
    assert claim_incident(conn, incident) is False, (
        "a live lease must stop a concurrent worker double-posting")


def test_an_expired_lease_is_reclaimable_so_a_hard_kill_self_heals(conn, incident):
    """SIGKILL between claim and delivery leaves the claim set and the release
    never runs. The lease is what lets the redelivered event retry."""
    from freshet.autopilot.consumer import LEASE_MINUTES, claim_incident

    assert claim_incident(conn, incident) is True
    _age_the_claim(conn, incident, LEASE_MINUTES + 1)
    assert claim_incident(conn, incident) is True, (
        "an expired lease must be reclaimable, or the brief is lost forever")


def test_a_delivered_brief_is_never_reposted_even_after_the_lease_expires(conn, incident):
    """The lease must not resurrect work that actually succeeded."""
    from freshet.autopilot.consumer import (
        LEASE_MINUTES,
        claim_incident,
        mark_brief_delivered,
    )

    assert claim_incident(conn, incident) is True
    mark_brief_delivered(conn, incident)
    _age_the_claim(conn, incident, LEASE_MINUTES + 1)
    assert claim_incident(conn, incident) is False, (
        "delivery is final; an expired lease must not re-post it")


def test_the_same_rules_hold_for_the_postmortem_slot(conn, incident):
    from freshet.autopilot.consumer import (
        LEASE_MINUTES,
        claim_incident,
        claim_postmortem,
        mark_brief_delivered,
        mark_postmortem_delivered,
    )

    claim_incident(conn, incident)
    mark_brief_delivered(conn, incident)          # postmortems require a brief first

    assert claim_postmortem(conn, incident) is True
    assert claim_postmortem(conn, incident) is False
    conn.execute(
        "UPDATE incidents SET postmortem_at = now() - (%s * interval '1 minute')"
        " WHERE incident_id = %s", (LEASE_MINUTES + 1, incident))
    assert claim_postmortem(conn, incident) is True, "expired lease is retryable"
    mark_postmortem_delivered(conn, incident)
    conn.execute(
        "UPDATE incidents SET postmortem_at = now() - (%s * interval '1 minute')"
        " WHERE incident_id = %s", (LEASE_MINUTES + 1, incident))
    assert claim_postmortem(conn, incident) is False, "delivery is final"
