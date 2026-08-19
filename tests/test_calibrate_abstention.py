"""A floor may only be proposed when the two distributions actually separate."""
from freshet.eval.calibrate_abstention import max_similarity, propose_floor


class _H:
    def __init__(self, s):
        self.similarity = s


def test_a_clean_gap_proposes_the_midpoint():
    r = propose_floor(on_corpus=[0.80, 0.90], off_corpus=[0.40, 0.50], current=0.70)
    assert r["proposal"] == 0.65
    assert 0.50 < r["proposal"] < 0.80


def test_overlapping_distributions_propose_nothing():
    """If an off-corpus question scores above an answerable on-corpus one, no
    threshold separates them — the floor is not the bug."""
    r = propose_floor(on_corpus=[0.45, 0.80], off_corpus=[0.50], current=0.70)
    assert r["proposal"] is None
    assert "OVERLAP" in r["reason"]
    assert r["current"] == 0.70


def test_no_samples_proposes_nothing():
    assert propose_floor([], [0.4], current=0.7)["proposal"] is None
    assert propose_floor([0.9], [], current=0.7)["proposal"] is None


def test_max_similarity_of_no_hits_is_zero_not_an_error():
    assert max_similarity([]) == 0.0
    assert max_similarity([_H(0.2), _H(0.7)]) == 0.7
