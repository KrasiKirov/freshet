"""Curate labels from the LIVE index, so retrieval is scored on current incidents.

The committed fixture corpus is frozen at 225 incidents from 5 providers. The
live index holds ~1,200 incidents from 42, but scoring needs ground truth: which
update actually STATES a cause. Keyword matching alone is far too noisy to be
ground truth — measured on this index, "The root cause has been fixed" (names
nothing) and "many rules triggered by date time triggers" (matched on
`triggered by`) both pass the marker filter.

So candidates are shortlisted by marker, then judged one by one. Two properties
keep this honest:

  * The judge sees ONLY the update text and answers a single question: does this
    sentence name a cause? It never sees the query, the ranking, or the
    retrieval system, so it cannot leak an answer into the metric it produces.
  * The QUESTION for each incident is generated from the incident TITLE alone —
    never from the cause update — so the query cannot encode which update is the
    right answer.

Output is marked `curated: "draft"`. The eval prints a warning until a human
reviews it and sets "reviewed", exactly as v1 required.

Run: make label-live
"""

from __future__ import annotations

import json
import os
import pathlib
import re

from freshet.autopilot.brief import _CAUSE_MARKERS, _cause_sentence

OUT = pathlib.Path("freshet/eval/fixtures/labels_live.json")

_JUDGE = (
    "You label incident-status updates for a retrieval benchmark. Given one "
    "update, answer whether it NAMES a technical cause — a specific fault, "
    "change, or dependency that produced the incident.\n\n"
    "YES: 'due to an upstream provider outage', 'caused by a bad config push', "
    "'a database migration exhausted connections'.\n"
    "NO: 'the root cause has been fixed', 'we identified the issue', 'a root "
    "cause analysis will follow', or any sentence that refers to a cause "
    "without saying what it was.\n\n"
    "Reply with exactly one word: YES or NO."
)
_QUESTION = (
    "Write the question an on-call engineer asks when they want to know WHY an "
    "incident happened. One sentence, natural, no preamble. Use only the incident "
    "title given — never invent specifics beyond it."
)

# Questions generated from a title reuse the title's words, and every indexed
# chunk carries that title as a prefix — so the lexical arm matches on wording
# rather than meaning, and its score is flattered. A paraphrase describes the same
# symptom in DIFFERENT words: the service may be named (an engineer knows which
# product they are asking about) but the title's distinctive terms may not be.
_PARAPHRASE = (
    "Write the question an on-call engineer asks when they want to know WHY an "
    "incident happened.\n\n"
    "CONSTRAINT: describe the symptom in YOUR OWN WORDS. You may name the service. "
    "Do NOT reuse the distinctive nouns, verbs or adjectives from the title — use "
    "synonyms or a plain-language description of the same problem.\n\n"
    "One sentence, natural, no preamble, no invented specifics."
)

_STOPWORDS = frozenset(["a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can", "could", "did", "do", "does", "for", "from", "had", "has", "have", "how", "in", "into", "is", "it", "its", "may", "might", "of", "on", "or", "our", "so", "than", "that", "the", "their", "there", "these", "they", "this", "to", "was", "were", "what", "when", "where", "which", "who", "why", "will", "with", "would", "you", "your", "some", "not", "no"])


def content_words(text: str) -> set[str]:
    """Lowercased content words: the tokens a lexical arm can actually match on."""
    return {w for w in re.findall(r"[a-z0-9]+", text.lower())
            if w not in _STOPWORDS and len(w) > 2}


def lexical_overlap(query: str, title: str, service: str = "") -> float:
    """Fraction of the title's content words the query reuses.

    The service name is excluded: naming the product you are asking about is
    realistic, not leakage. Everything else is the wording we want paraphrased away.
    """
    title_words = content_words(title) - content_words(service)
    if not title_words:
        return 0.0
    return round(len(title_words & content_words(query)) / len(title_words), 3)


def paraphrase_query(client, model: str, title: str, service: str,
                     max_overlap: float = 0.25, attempts: int = 3) -> tuple[str, float]:
    """Generate a question that avoids the title's vocabulary, and VERIFY it did.

    The model is asked to paraphrase, then the result is measured; a prompt alone
    is not evidence of compliance. The best of `attempts` is returned even if none
    clears the bar, so a stubborn title degrades one query rather than the run.
    """
    best, best_overlap = "", 1.0
    for _ in range(attempts):
        r = client.messages.create(model=model, max_tokens=90, system=_PARAPHRASE,
                                   messages=[{"role": "user", "content":
                                              f"Service: {service}\nTitle: {title}"}])
        q = next((b.text for b in r.content if b.type == "text"), "").strip()
        overlap = lexical_overlap(q, title, service)
        if overlap < best_overlap:
            best, best_overlap = q, overlap
        if overlap <= max_overlap:
            break
    return best, best_overlap


def candidates(conn, limit: int | None = None) -> list[dict]:
    """Updates whose text contains a cause marker and survives the sentence filter."""
    like = " OR ".join(f"lower(text) LIKE '%%{m}%%'" for m in _CAUSE_MARKERS)
    rows = conn.execute(
        f"SELECT DISTINCT ON (event_id) event_id, incident_id, service, title, text, ts"
        f" FROM vector_records WHERE ({like}) AND title IS NOT NULL"
        f"  AND incident_id IS NOT NULL AND text IS NOT NULL"
        f" ORDER BY event_id, ts DESC").fetchall()
    out = []
    for event_id, incident_id, service, title, text, _ts in rows:
        prefix = f"{title}: "
        body = text[len(prefix):] if text.startswith(prefix) else text
        sentence = _cause_sentence(body)
        if sentence:
            out.append({"event_id": event_id, "incident_id": incident_id,
                        "service": service, "title": title, "sentence": sentence})
    return out[:limit] if limit else out


def _ask(client, model: str, system: str, content: str, max_tokens: int = 120) -> str:
    resp = client.messages.create(model=model, max_tokens=max_tokens, system=system,
                                  messages=[{"role": "user", "content": content}])
    return next((b.text for b in resp.content if b.type == "text"), "").strip()


def main() -> None:
    from anthropic import Anthropic

    from freshet.common.db import connect

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY is required to curate labels")
    model = os.environ.get("FRESHET_LLM_MODEL", "claude-sonnet-4-6")
    client, conn = Anthropic(), connect()

    cands = candidates(conn, limit=int(os.environ.get("LABEL_LIMIT", "0")) or None)
    print(f"[label-live] {len(cands)} marker candidates to judge")

    by_incident: dict[str, dict] = {}
    kept = 0
    for i, c in enumerate(cands, 1):
        if _ask(client, model, _JUDGE, c["sentence"], max_tokens=5).upper().startswith("NO"):
            continue
        kept += 1
        entry = by_incident.setdefault(c["incident_id"], {
            "incident_id": c["incident_id"], "service": c["service"],
            "title": c["title"], "query": "", "cause_event_ids": [], "notes": []})
        entry["cause_event_ids"].append(c["event_id"])
        entry["notes"].append(c["sentence"][:200])
        if i % 25 == 0:
            print(f"   judged {i}/{len(cands)} — {kept} name a cause")

    for entry in by_incident.values():
        # From the TITLE only: the query must not encode which update is the answer.
        entry["query"] = _ask(client, model, _QUESTION, entry["title"])

    out = {
        "curated": "draft",
        "_source": "live index (42 providers) — judged by LLM, pending human review",
        "_method": "marker shortlist -> per-update LLM judge (names a cause?) -> "
                   "query generated from the incident TITLE only",
        "labeled": sorted(by_incident.values(), key=lambda e: e["incident_id"]),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2) + "\n")
    print(f"[label-live] {len(out['labeled'])} incidents labeled -> {OUT}")


if __name__ == "__main__":
    main()
