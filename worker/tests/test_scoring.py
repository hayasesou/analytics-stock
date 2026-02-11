from __future__ import annotations

from datetime import date

import pandas as pd

from src.analytics.scoring import score_securities


def _config() -> dict:
    return {
        "scoring": {
            "weights": {
                "quality": 0.25,
                "growth": 0.20,
                "value": 0.20,
                "catalyst": 0.25,
                "momentum": 0.10,
            },
            "penalties": {
                "missing_ratio_threshold": 0.2,
                "missing_ratio_penalty": 8.0,
                "low_liquidity_penalty": 100.0,
            },
        },
        "confidence": {
            "high": {
                "min_primary_sources": 2,
                "requires_key_numbers_with_citations": True,
                "requires_no_major_contradiction": True,
                "max_missing_ratio": 0.15,
            }
        },
    }


def test_score_securities_adds_edge_score_without_changing_combined_formula() -> None:
    features = pd.DataFrame(
        [
            {
                "security_id": "JP:1111",
                "market": "JP",
                "as_of_date": date(2026, 2, 1),
                "vol_20d": 0.15,
                "missing_ratio": 0.05,
                "ret_20d": 0.1,
                "ret_5d": 0.03,
                "dollar_volume_20d": 250_000_000,
                "avg_volume_20d": 500_000,
                "jump_flag": 1,
            },
            {
                "security_id": "JP:2222",
                "market": "JP",
                "as_of_date": date(2026, 2, 1),
                "vol_20d": 0.35,
                "missing_ratio": 0.10,
                "ret_20d": -0.02,
                "ret_5d": -0.01,
                "dollar_volume_20d": 180_000_000,
                "avg_volume_20d": 250_000,
                "jump_flag": 0,
            },
            {
                "security_id": "US:119",
                "market": "US",
                "as_of_date": date(2026, 2, 1),
                "vol_20d": 0.22,
                "missing_ratio": 0.04,
                "ret_20d": 0.08,
                "ret_5d": 0.02,
                "dollar_volume_20d": 900_000_000,
                "avg_volume_20d": 2_200_000,
                "jump_flag": 1,
            },
            {
                "security_id": "US:7",
                "market": "US",
                "as_of_date": date(2026, 2, 1),
                "vol_20d": 0.45,
                "missing_ratio": 0.08,
                "ret_20d": -0.04,
                "ret_5d": -0.02,
                "dollar_volume_20d": 350_000_000,
                "avg_volume_20d": 1_300_000,
                "jump_flag": 0,
            },
        ]
    )

    scored = score_securities(features, _config())

    assert "edge_score" in scored.columns
    assert scored["edge_score"].between(0, 100).all()

    recomputed = (
        scored["quality"] * 0.25
        + scored["growth"] * 0.20
        + scored["value"] * 0.20
        + scored["catalyst"] * 0.25
        + scored["momentum"] * 0.10
    )
    assert (scored["combined_score"] - recomputed).abs().max() < 1e-8
