"""Authored evaluation queries with ground-truth defined by relevance predicate.

Because the incident is scripted (see generator/scenarios.py), we know which
event *types* answer each question. Ground truth is resolved against a concrete
corpus at eval time — no hardcoded event ids (those are minted per seed)."""

from __future__ import annotations

from dataclasses import dataclass

from freshet.common.schemas import Event


@dataclass(frozen=True)
class LabeledQuery:
    text: str
    relevant_types: frozenset[str]
    incident_id: str


def relevant_event_ids(query: LabeledQuery, corpus: list[Event]) -> set[str]:
    """The ground-truth relevant set: incident events whose type the query targets."""
    return {
        e.event_id
        for e in corpus
        if e.incident_id == query.incident_id and e.type in query.relevant_types
    }


def build_labeled_queries(corpus, truths) -> list[LabeledQuery]:
    """Mechanically derive labeled queries from the benchmark's ground truth: for
    each incident, instantiate its archetype's query templates (text via the
    incident's service; relevant_types straight from the template). Ground truth
    resolves via relevant_event_ids (incident_id + relevant_types)."""
    from freshet.generator.scenarios import ARCHETYPES

    by_name = {a.name: a for a in ARCHETYPES}
    out: list[LabeledQuery] = []
    for t in truths:
        arc = by_name[t.archetype]
        for template, relevant_types in arc.queries:
            out.append(LabeledQuery(
                text=template.format(service=t.service),
                relevant_types=relevant_types,
                incident_id=t.incident_id,
            ))
    return out
