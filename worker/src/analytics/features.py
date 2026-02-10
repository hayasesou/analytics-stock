from __future__ import annotations

import numpy as np
import pandas as pd


def compute_layer0_features(prices: pd.DataFrame) -> pd.DataFrame:
    required = {
        "security_id",
        "market",
        "trade_date",
        "open_raw",
        "high_raw",
        "low_raw",
        "close_raw",
        "volume",
    }
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"prices is missing required columns: {sorted(missing)}")

    df = prices.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["security_id", "trade_date"]).reset_index(drop=True)

    grp = df.groupby("security_id", group_keys=False)
    df["ret_1d"] = grp["close_raw"].pct_change()
    df["ret_5d"] = grp["close_raw"].pct_change(5)
    df["ret_20d"] = grp["close_raw"].pct_change(20)
    df["vol_20d"] = grp["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    df["avg_volume_20d"] = grp["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["dollar_volume"] = df["close_raw"] * df["volume"]
    df["dollar_volume_20d"] = grp["dollar_volume"].rolling(20).mean().reset_index(level=0, drop=True)

    prev_close = grp["close_raw"].shift(1)
    tr = pd.concat(
        [
            df["high_raw"] - df["low_raw"],
            (df["high_raw"] - prev_close).abs(),
            (df["low_raw"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.groupby(df["security_id"]).transform(lambda s: s.rolling(14).mean())

    df["jump_flag"] = (df["ret_1d"].abs() > 0.08).astype(int)

    cols_for_missing = ["ret_5d", "ret_20d", "vol_20d", "avg_volume_20d", "atr14"]
    df["missing_ratio"] = df[cols_for_missing].isna().mean(axis=1)

    latest = df.loc[df.groupby("security_id")["trade_date"].idxmax()].copy()
    latest["as_of_date"] = latest["trade_date"].dt.date

    feature_cols = [
        "security_id",
        "market",
        "as_of_date",
        "ret_5d",
        "ret_20d",
        "vol_20d",
        "avg_volume_20d",
        "dollar_volume_20d",
        "atr14",
        "jump_flag",
        "missing_ratio",
    ]

    for c in feature_cols:
        if c not in latest.columns:
            latest[c] = np.nan

    return latest[feature_cols].reset_index(drop=True)
