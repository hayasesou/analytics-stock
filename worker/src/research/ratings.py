from __future__ import annotations


def compute_fundamental_rating(
    *,
    combined_score: float,
    confidence: str,
    missing_ratio: float,
    has_major_contradiction: bool = False,
    primary_source_count: int = 0,
) -> str:
    score = float(combined_score)
    confidence_norm = (confidence or "").strip().lower()
    missing = float(missing_ratio)
    sources = int(primary_source_count)

    if has_major_contradiction:
        return "C"
    if confidence_norm == "high" and score >= 75 and missing <= 0.15 and sources >= 2:
        return "A"
    if score >= 55 and missing <= 0.30:
        return "B"
    return "C"
