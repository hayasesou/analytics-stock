from __future__ import annotations

import pandas as pd


def build_top50(
    scored: pd.DataFrame,
    top_n: int,
    jp_min: int,
    us_min: int,
) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame()

    eligible = scored.loc[~scored["exclusion_flag"]].copy()
    if eligible.empty:
        return pd.DataFrame()

    jp = eligible[eligible["market"] == "JP"].sort_values("combined_score", ascending=False)
    us = eligible[eligible["market"] == "US"].sort_values("combined_score", ascending=False)

    selected = pd.concat([jp.head(jp_min), us.head(us_min)], axis=0)
    selected = selected.drop_duplicates(subset=["security_id"])  # safety

    remainder = (
        eligible.loc[~eligible["security_id"].isin(selected["security_id"])]
        .sort_values("combined_score", ascending=False)
    )

    remaining_slots = max(top_n - len(selected), 0)
    selected = pd.concat([selected, remainder.head(remaining_slots)], axis=0)
    selected = selected.sort_values("combined_score", ascending=False).head(top_n).copy()

    selected["mixed_rank"] = range(1, len(selected) + 1)
    selected["selection_reason"] = "score_rank"

    mandatory_ids = set(jp.head(jp_min)["security_id"]).union(set(us.head(us_min)["security_id"]))
    selected.loc[selected["security_id"].isin(mandatory_ids), "selection_reason"] = "market_minimum"

    cols = list(scored.columns) + ["mixed_rank", "selection_reason"]
    cols = [c for c in cols if c in selected.columns]
    return selected[cols].reset_index(drop=True)
