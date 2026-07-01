from freshet.common.schemas import EventType, CHANGE_TYPES, REMEDIATION_TYPES


def test_archetype_event_types_exist():
    for name in ("CONFIG_CHANGED", "CONFIG_REVERTED", "DEPENDENCY_DOWN",
                 "DEPENDENCY_FAILOVER", "MEMORY_LEAK_SHIPPED", "SCALED_UP",
                 "CERT_EXPIRED", "CERT_RENEWED", "MIGRATION_APPLIED", "MIGRATION_REVERTED"):
        assert hasattr(EventType, name)


def test_change_and_remediation_sets_are_disjoint_and_typed():
    assert CHANGE_TYPES.isdisjoint(REMEDIATION_TYPES)
    assert "deploy_started" in CHANGE_TYPES and "rollback" in REMEDIATION_TYPES
    assert "config_changed" in CHANGE_TYPES and "config_reverted" in REMEDIATION_TYPES
    assert len(CHANGE_TYPES) == 6 and len(REMEDIATION_TYPES) == 6


from freshet.generator.scenarios import Archetype, Step, ARCHETYPES


def test_six_archetypes_each_well_formed():
    assert len(ARCHETYPES) == 6
    causes, fixes = set(), set()
    for arc in ARCHETYPES:
        change = [s for s in arc.steps if s.role == "change"]
        remediation = [s for s in arc.steps if s.role == "remediation"]
        spike = [s for s in arc.steps if s.role == "spike"]
        assert len(change) == 1, arc.name
        assert len(remediation) == 1, arc.name
        assert len(spike) == 1, arc.name
        assert spike[0].severity is not None
        assert arc.queries, arc.name
        causes.add(change[0].type)
        fixes.add(remediation[0].type)
    assert len(causes) == 6 and len(fixes) == 6


def test_archetype_types_match_shared_sets():
    from freshet.common.schemas import CHANGE_TYPES, REMEDIATION_TYPES
    causes = {s.type for arc in ARCHETYPES for s in arc.steps if s.role == "change"}
    fixes = {s.type for arc in ARCHETYPES for s in arc.steps if s.role == "remediation"}
    assert causes == set(CHANGE_TYPES)
    assert fixes == set(REMEDIATION_TYPES)
