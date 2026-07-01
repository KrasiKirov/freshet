"""Standard ranking metrics for retrieval evaluation, over event-id lists with
binary relevance. Pure functions — the eval's scoreable core."""

from __future__ import annotations

import math


def recall_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    if not relevant:
        return 0.0
    hit = sum(1 for cid in ranked[:k] if cid in relevant)
    return hit / len(relevant)


def precision_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    if k <= 0:
        return 0.0
    hit = sum(1 for cid in ranked[:k] if cid in relevant)
    return hit / k


def reciprocal_rank(ranked: list[str], relevant: set[str]) -> float:
    for i, cid in enumerate(ranked):
        if cid in relevant:
            return 1.0 / (i + 1)
    return 0.0


def ndcg_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    """Binary-relevance nDCG@k. DCG uses 1/log2(rank+1) (rank 1-based)."""
    dcg = sum(
        1.0 / math.log2(i + 2)  # i is 0-based -> rank i+1 -> log2((i+1)+1)
        for i, cid in enumerate(ranked[:k])
        if cid in relevant
    )
    ideal_hits = min(len(relevant), k)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(ideal_hits))
    return dcg / idcg if idcg > 0 else 0.0
