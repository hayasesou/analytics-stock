from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _percentile_within_market(df: pd.DataFrame, column: str, ascending: bool = True) -> pd.Series:
    ranked = df.groupby("market")[column].rank(pct=True, ascending=ascending)
    return (ranked * 100.0).fillna(0.0)


def score_securities(
    features: pd.DataFrame,
    config: dict[str, Any],
    evidence_stats: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame()

    df = features.copy()
    weights = config["scoring"]["weights"]
    penalties = config["scoring"]["penalties"]

    # Proxies for MVP baseline
    df["quality_raw"] = (1.0 - df["vol_20d"].fillna(df["vol_20d"].median())) - df["missing_ratio"].fillna(0)
    df["growth_raw"] = df["ret_20d"].fillna(0)
    df["value_raw"] = (1.0 / (1.0 + df["ret_20d"].abs().fillna(0))) + np.log1p(df["dollar_volume_20d"].fillna(0)) / 20.0
    df["momentum_raw"] = (df["ret_20d"].fillna(0) * 0.7) + (df["ret_5d"].fillna(0) * 0.3)
    df["catalyst_raw"] = 0.4 + (df["jump_flag"].fillna(0) * 0.3)

    if evidence_stats is not None and not evidence_stats.empty:
        cols = [
            "security_id",
            "primary_source_count",
            "has_key_numbers",
            "has_major_contradiction",
            "catalyst_bonus",
        ]
        merged = evidence_stats.reindex(columns=cols).copy()
        merged["has_key_numbers"] = merged["has_key_numbers"].fillna(False)
        merged["has_major_contradiction"] = merged["has_major_contradiction"].fillna(False)
        df = df.merge(merged, on="security_id", how="left")
    else:
        df["primary_source_count"] = 0
        df["has_key_numbers"] = False
        df["has_major_contradiction"] = False
        df["catalyst_bonus"] = 0.0

    df["catalyst_raw"] += df["catalyst_bonus"].fillna(0.0)

    df["quality"] = _percentile_within_market(df, "quality_raw", ascending=True)
    df["growth"] = _percentile_within_market(df, "growth_raw", ascending=True)
    df["value"] = _percentile_within_market(df, "value_raw", ascending=True)
    df["momentum"] = _percentile_within_market(df, "momentum_raw", ascending=True)
    df["catalyst"] = _percentile_within_market(df, "catalyst_raw", ascending=True)

    df["combined_score"] = (
        df["quality"] * float(weights["quality"])
        + df["growth"] * float(weights["growth"])
        + df["value"] * float(weights["value"])
        + df["catalyst"] * float(weights["catalyst"])
        + df["momentum"] * float(weights["momentum"])
    )

    df["liquidity_flag"] = (
        (df["avg_volume_20d"].fillna(0) < 120_000)
        | (df["dollar_volume_20d"].fillna(0) < 80_000_000)
    )

    missing_thr = float(penalties["missing_ratio_threshold"])
    penalty_missing = float(penalties["missing_ratio_penalty"])
    df.loc[df["missing_ratio"] > missing_thr, "combined_score"] -= penalty_missing

    # Gate out low liquidity names from buy candidates.
    df["exclusion_flag"] = df["liquidity_flag"]
    df.loc[df["exclusion_flag"], "combined_score"] -= float(penalties["low_liquidity_penalty"])

    high_cfg = config["confidence"]["high"]
    high_mask = (
        (df["primary_source_count"].fillna(0) >= int(high_cfg["min_primary_sources"]))
        & (df["has_key_numbers"].fillna(False) == bool(high_cfg["requires_key_numbers_with_citations"]))
        & (df["has_major_contradiction"].fillna(False) == (not bool(high_cfg["requires_no_major_contradiction"])))
        & (df["missing_ratio"].fillna(1.0) <= float(high_cfg["max_missing_ratio"]))
    )

    medium_mask = (
        (df["missing_ratio"].fillna(1.0) <= 0.3)
        & (~df["exclusion_flag"])
    )

    df["confidence"] = np.where(high_mask, "High", np.where(medium_mask, "Medium", "Low"))

    df["market_rank"] = df.groupby("market")["combined_score"].rank(method="dense", ascending=False).astype(int)

    columns = [
        "security_id",
        "market",
        "as_of_date",
        "quality",
        "growth",
        "value",
        "momentum",
        "catalyst",
        "combined_score",
        "missing_ratio",
        "liquidity_flag",
        "exclusion_flag",
        "confidence",
        "market_rank",
    ]

    return df[columns].sort_values(["market", "market_rank"]).reset_index(drop=True)
