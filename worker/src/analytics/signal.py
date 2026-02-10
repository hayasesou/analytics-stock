from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


def _next_weekly_cutoff(as_of: date) -> date:
    # 次の土曜を期限とする
    days_to_sat = (5 - as_of.weekday()) % 7
    if days_to_sat == 0:
        days_to_sat = 7
    return as_of + timedelta(days=days_to_sat)


def generate_b_mode_signals(
    top50: pd.DataFrame,
    as_of_date: date,
    risk_alert_mode: bool = False,
) -> pd.DataFrame:
    if top50.empty:
        return pd.DataFrame()

    df = top50.copy()
    valid_until = _next_weekly_cutoff(as_of_date)

    df["is_signal"] = (df["confidence"] == "High") & (df["mixed_rank"] <= 10)
    df["reason"] = ""
    df.loc[df["is_signal"], "reason"] = "confidence_high_and_top10"
    df.loc[df["confidence"] != "High", "reason"] = "confidence_not_high"
    df.loc[df["mixed_rank"] > 10, "reason"] = "rank_outside_top10"

    cap = 1 if risk_alert_mode else 3
    signal_candidates = df[df["is_signal"]].sort_values("combined_score", ascending=False)
    allowed_ids = set(signal_candidates.head(cap)["security_id"])
    df["entry_allowed"] = df["security_id"].isin(allowed_ids)

    if risk_alert_mode:
        df.loc[df["entry_allowed"], "reason"] = "risk_alert_mode_entry_cap"

    df["as_of_date"] = as_of_date
    df["valid_until"] = valid_until

    cols = [
        "security_id",
        "market",
        "as_of_date",
        "is_signal",
        "entry_allowed",
        "reason",
        "mixed_rank",
        "confidence",
        "valid_until",
    ]
    return df[cols].reset_index(drop=True)
