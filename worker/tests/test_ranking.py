import pandas as pd

from src.analytics.ranking import build_top50


def test_build_top50_market_minimums() -> None:
    rows = []
    for i in range(1, 21):
        rows.append(
            {
                "security_id": f"JP:{1000 + i}",
                "market": "JP",
                "combined_score": 100 - i,
                "exclusion_flag": False,
                "confidence": "High",
            }
        )
    for i in range(1, 21):
        rows.append(
            {
                "security_id": f"US:{i}",
                "market": "US",
                "combined_score": 95 - i,
                "exclusion_flag": False,
                "confidence": "High",
            }
        )

    scored = pd.DataFrame(rows)
    top = build_top50(scored, top_n=30, jp_min=10, us_min=10)

    assert len(top) == 30
    assert (top["market"] == "JP").sum() >= 10
    assert (top["market"] == "US").sum() >= 10
    assert top["mixed_rank"].min() == 1


def test_build_top50_excludes_flagged() -> None:
    scored = pd.DataFrame(
        [
            {"security_id": "JP:1111", "market": "JP", "combined_score": 99, "exclusion_flag": True},
            {"security_id": "JP:2222", "market": "JP", "combined_score": 80, "exclusion_flag": False},
            {"security_id": "US:1", "market": "US", "combined_score": 90, "exclusion_flag": False},
        ]
    )

    top = build_top50(scored, top_n=2, jp_min=1, us_min=1)
    assert "JP:1111" not in set(top["security_id"])
