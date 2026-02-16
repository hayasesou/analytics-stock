from __future__ import annotations

from src.research.ratings import compute_fundamental_rating


def test_compute_fundamental_rating_returns_a_for_high_quality() -> None:
    rating = compute_fundamental_rating(
        combined_score=80.0,
        confidence="High",
        missing_ratio=0.1,
        has_major_contradiction=False,
        primary_source_count=3,
    )
    assert rating == "A"


def test_compute_fundamental_rating_returns_c_on_contradiction() -> None:
    rating = compute_fundamental_rating(
        combined_score=90.0,
        confidence="High",
        missing_ratio=0.05,
        has_major_contradiction=True,
        primary_source_count=4,
    )
    assert rating == "C"


def test_compute_fundamental_rating_returns_b_for_mid_case() -> None:
    rating = compute_fundamental_rating(
        combined_score=62.0,
        confidence="Medium",
        missing_ratio=0.2,
        has_major_contradiction=False,
        primary_source_count=1,
    )
    assert rating == "B"
