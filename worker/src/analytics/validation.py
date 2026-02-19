from __future__ import annotations

from datetime import date, timedelta
from statistics import mean, median
from typing import Any

import pandas as pd

from src.analytics.backtest import run_backtest

DEFAULT_VALIDATION_POLICY: dict[str, Any] = {
    "enabled": True,
    "lookback_days": 900,
    "train_days": 252,
    "test_days": 63,
    "step_days": 63,
    "warmup_days": 40,
    "execution_buffer_days": 70,
    "momentum_quantile": 0.65,
    "max_volatility_20d": 0.80,
    "min_signal_gap_days": 5,
    "min_signals_per_fold": 2,
    "primary_cost_profile": "strict",
    "strict_cost_profile": "strict",
    "gates": {
        "min_fold_count": 3,
        "min_total_trades": 8,
        "min_median_sharpe": 0.30,
        "min_mean_sharpe": 0.20,
        "min_worst_max_dd": -0.25,
        "min_strict_median_sharpe": 0.10,
        "require_positive_mean_cagr": False,
    },
}


def resolve_validation_policy(cfg: dict[str, Any]) -> dict[str, Any]:
    strategy_factory_cfg = cfg.get("strategy_factory", {})
    raw = strategy_factory_cfg.get("validation", {})
    if not isinstance(raw, dict):
        raw = {}

    effective = dict(DEFAULT_VALIDATION_POLICY)
    effective.update({k: v for k, v in raw.items() if k in effective and k != "gates"})

    gates_raw = raw.get("gates", {})
    gates = dict(DEFAULT_VALIDATION_POLICY["gates"])
    if isinstance(gates_raw, dict):
        gates.update(gates_raw)
    effective["gates"] = gates

    int_keys = [
        "lookback_days",
        "train_days",
        "test_days",
        "step_days",
        "warmup_days",
        "execution_buffer_days",
        "min_signal_gap_days",
        "min_signals_per_fold",
    ]
    for key in int_keys:
        try:
            effective[key] = max(1, int(effective.get(key, DEFAULT_VALIDATION_POLICY[key])))
        except (TypeError, ValueError):
            effective[key] = DEFAULT_VALIDATION_POLICY[key]

    try:
        effective["momentum_quantile"] = max(0.05, min(0.95, float(effective.get("momentum_quantile", 0.65))))
    except (TypeError, ValueError):
        effective["momentum_quantile"] = DEFAULT_VALIDATION_POLICY["momentum_quantile"]

    try:
        effective["max_volatility_20d"] = max(0.05, float(effective.get("max_volatility_20d", 0.8)))
    except (TypeError, ValueError):
        effective["max_volatility_20d"] = DEFAULT_VALIDATION_POLICY["max_volatility_20d"]

    for key in [
        "min_fold_count",
        "min_total_trades",
    ]:
        try:
            gates[key] = max(1, int(gates.get(key, DEFAULT_VALIDATION_POLICY["gates"][key])))
        except (TypeError, ValueError):
            gates[key] = DEFAULT_VALIDATION_POLICY["gates"][key]

    for key in [
        "min_median_sharpe",
        "min_mean_sharpe",
        "min_worst_max_dd",
        "min_strict_median_sharpe",
    ]:
        try:
            gates[key] = float(gates.get(key, DEFAULT_VALIDATION_POLICY["gates"][key]))
        except (TypeError, ValueError):
            gates[key] = DEFAULT_VALIDATION_POLICY["gates"][key]

    gates["require_positive_mean_cagr"] = bool(gates.get("require_positive_mean_cagr", False))

    effective["enabled"] = bool(effective.get("enabled", True))
    effective["primary_cost_profile"] = str(effective.get("primary_cost_profile", "strict"))
    effective["strict_cost_profile"] = str(effective.get("strict_cost_profile", "strict"))
    return effective


def _prepare_prices(prices: pd.DataFrame, security_id: str, market: str) -> pd.DataFrame:
    required_cols = {"trade_date", "open_raw", "high_raw", "low_raw", "close_raw"}
    if prices.empty or not required_cols.issubset(prices.columns):
        return pd.DataFrame(columns=["security_id", "market", "trade_date", "open_raw", "high_raw", "low_raw", "close_raw"])

    df = prices.copy()
    if "security_id" not in df.columns:
        df["security_id"] = security_id
    if "market" not in df.columns:
        df["market"] = market

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    for col in ["open_raw", "high_raw", "low_raw", "close_raw"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["trade_date", "open_raw", "high_raw", "low_raw", "close_raw"])
    df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"]).reset_index(drop=True)

    # 価格が不正な行は除外
    df = df[(df["open_raw"] > 0) & (df["high_raw"] > 0) & (df["low_raw"] > 0) & (df["close_raw"] > 0)]
    return df


def _add_features(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices.copy().sort_values("trade_date").reset_index(drop=True)
    df["ret_1d"] = df["close_raw"].pct_change()
    df["ret_5d"] = df["close_raw"].pct_change(5)
    df["ret_20d"] = df["close_raw"].pct_change(20)
    df["vol_20d"] = df["ret_1d"].rolling(20).std(ddof=0) * (252 ** 0.5)
    return df


def _build_windows(trade_dates: pd.Series, policy: dict[str, Any]) -> list[dict[str, Any]]:
    train_days = int(policy["train_days"])
    test_days = int(policy["test_days"])
    step_days = int(policy["step_days"])

    dates = pd.Index(sorted(pd.unique(trade_dates.dropna())))
    windows: list[dict[str, Any]] = []
    cursor = train_days
    fold_idx = 0

    while cursor + test_days <= len(dates):
        train_start = dates[cursor - train_days]
        train_end = dates[cursor - 1]
        test_start = dates[cursor]
        test_end = dates[cursor + test_days - 1]

        windows.append(
            {
                "fold": fold_idx,
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            }
        )
        cursor += step_days
        fold_idx += 1

    return windows


def _build_signals_for_window(
    features: pd.DataFrame,
    window: dict[str, Any],
    security_id: str,
    market: str,
    policy: dict[str, Any],
) -> tuple[pd.DataFrame, float | None]:
    train_mask = (features["trade_date"] >= window["train_start"]) & (features["trade_date"] <= window["train_end"])
    test_mask = (features["trade_date"] >= window["test_start"]) & (features["trade_date"] <= window["test_end"])

    train_df = features.loc[train_mask]
    train_ret20 = train_df["ret_20d"].dropna()
    if train_ret20.empty:
        return pd.DataFrame(), None

    threshold = float(train_ret20.quantile(float(policy["momentum_quantile"])))
    max_vol = float(policy["max_volatility_20d"])

    candidates = features.loc[test_mask].copy()
    candidates = candidates[
        (candidates["ret_20d"] >= threshold)
        & (candidates["ret_5d"] > 0)
        & (candidates["vol_20d"] <= max_vol)
    ]

    selected_dates: list[date] = []
    min_gap = int(policy["min_signal_gap_days"])
    last_date: date | None = None
    for trade_date in candidates["trade_date"].tolist():
        if last_date is None or (trade_date - last_date).days >= min_gap:
            selected_dates.append(trade_date)
            last_date = trade_date

    signals = pd.DataFrame(
        [
            {
                "security_id": security_id,
                "market": market,
                "as_of_date": d,
                "is_signal": True,
                "entry_allowed": True,
            }
            for d in selected_dates
        ]
    )
    return signals, threshold


def _summarize_profile_metrics(profile_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    if not profile_metrics:
        return {
            "fold_count": 0,
            "total_trades": 0,
            "mean_sharpe": None,
            "median_sharpe": None,
            "mean_cagr": None,
            "worst_max_dd": None,
        }

    sharpe_values = [float(m["sharpe"]) for m in profile_metrics if m.get("sharpe") is not None]
    cagr_values = [float(m["cagr"]) for m in profile_metrics if m.get("cagr") is not None]
    max_dd_values = [float(m["max_dd"]) for m in profile_metrics if m.get("max_dd") is not None]
    total_trades = sum(int(m.get("trade_count") or 0) for m in profile_metrics)

    return {
        "fold_count": len(profile_metrics),
        "total_trades": total_trades,
        "mean_sharpe": float(mean(sharpe_values)) if sharpe_values else None,
        "median_sharpe": float(median(sharpe_values)) if sharpe_values else None,
        "mean_cagr": float(mean(cagr_values)) if cagr_values else None,
        "worst_max_dd": float(min(max_dd_values)) if max_dd_values else None,
    }


def _evaluate_gate(summary_by_profile: dict[str, dict[str, Any]], policy: dict[str, Any]) -> dict[str, Any]:
    gates = dict(policy.get("gates", {}))
    primary_profile = str(policy.get("primary_cost_profile", "strict"))
    strict_profile = str(policy.get("strict_cost_profile", "strict"))

    primary = summary_by_profile.get(primary_profile, {})
    strict = summary_by_profile.get(strict_profile, {})

    reasons: list[str] = []

    fold_count = int(primary.get("fold_count") or 0)
    if fold_count < int(gates.get("min_fold_count", 3)):
        reasons.append(f"fold_count<{int(gates.get('min_fold_count', 3))}")

    total_trades = int(primary.get("total_trades") or 0)
    if total_trades < int(gates.get("min_total_trades", 8)):
        reasons.append(f"total_trades<{int(gates.get('min_total_trades', 8))}")

    median_sharpe = primary.get("median_sharpe")
    if median_sharpe is None or float(median_sharpe) < float(gates.get("min_median_sharpe", 0.3)):
        reasons.append(f"median_sharpe<{float(gates.get('min_median_sharpe', 0.3))}")

    mean_sharpe = primary.get("mean_sharpe")
    if mean_sharpe is None or float(mean_sharpe) < float(gates.get("min_mean_sharpe", 0.2)):
        reasons.append(f"mean_sharpe<{float(gates.get('min_mean_sharpe', 0.2))}")

    worst_max_dd = primary.get("worst_max_dd")
    if worst_max_dd is None or float(worst_max_dd) < float(gates.get("min_worst_max_dd", -0.25)):
        reasons.append(f"worst_max_dd<{float(gates.get('min_worst_max_dd', -0.25))}")

    strict_median_sharpe = strict.get("median_sharpe")
    if strict_median_sharpe is None or float(strict_median_sharpe) < float(gates.get("min_strict_median_sharpe", 0.1)):
        reasons.append(f"strict_median_sharpe<{float(gates.get('min_strict_median_sharpe', 0.1))}")

    if bool(gates.get("require_positive_mean_cagr", False)):
        mean_cagr = primary.get("mean_cagr")
        if mean_cagr is None or float(mean_cagr) <= 0:
            reasons.append("mean_cagr<=0")

    return {
        "passed": len(reasons) == 0,
        "reasons": reasons,
        "primary_cost_profile": primary_profile,
        "strict_cost_profile": strict_profile,
    }


def run_walk_forward_validation(
    prices: pd.DataFrame,
    security_id: str,
    market: str,
    config: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    prepared = _prepare_prices(prices, security_id=security_id, market=market)
    if prepared.empty:
        return {
            "policy": policy,
            "folds": [],
            "summary": {},
            "gate": {"passed": False, "reasons": ["no_price_data"]},
        }

    features = _add_features(prepared)
    windows = _build_windows(features["trade_date"], policy)
    if not windows:
        return {
            "policy": policy,
            "folds": [],
            "summary": {},
            "gate": {"passed": False, "reasons": ["insufficient_history"]},
        }

    folds: list[dict[str, Any]] = []
    profile_metrics_store: dict[str, list[dict[str, Any]]] = {}

    for window in windows:
        signals, threshold = _build_signals_for_window(
            features=features,
            window=window,
            security_id=security_id,
            market=market,
            policy=policy,
        )

        signal_count = int(len(signals))
        fold_payload: dict[str, Any] = {
            "fold": int(window["fold"]),
            "train_start": window["train_start"].isoformat(),
            "train_end": window["train_end"].isoformat(),
            "test_start": window["test_start"].isoformat(),
            "test_end": window["test_end"].isoformat(),
            "signal_count": signal_count,
            "momentum_threshold": threshold,
            "profiles": {},
            "skipped": False,
            "skip_reason": None,
        }

        if signal_count < int(policy["min_signals_per_fold"]):
            fold_payload["skipped"] = True
            fold_payload["skip_reason"] = "insufficient_signals"
            folds.append(fold_payload)
            continue

        start_with_warmup = window["test_start"] - timedelta(days=int(policy["warmup_days"]))
        end_with_buffer = window["test_end"] + timedelta(days=int(policy["execution_buffer_days"]))
        eval_prices = prepared[(prepared["trade_date"] >= start_with_warmup) & (prepared["trade_date"] <= end_with_buffer)].copy()
        if eval_prices.empty:
            fold_payload["skipped"] = True
            fold_payload["skip_reason"] = "no_eval_prices"
            folds.append(fold_payload)
            continue

        backtest_results = run_backtest(eval_prices, signals, config)
        if not backtest_results:
            fold_payload["skipped"] = True
            fold_payload["skip_reason"] = "empty_backtest"
            folds.append(fold_payload)
            continue

        for result in backtest_results:
            profile = result.cost_profile
            profile_metrics = {
                "sharpe": float(result.metrics.get("sharpe", 0.0)),
                "cagr": float(result.metrics.get("cagr", 0.0)),
                "max_dd": float(result.metrics.get("max_dd", 0.0)),
                "trade_count": len(result.trades),
            }
            fold_payload["profiles"][profile] = profile_metrics
            profile_metrics_store.setdefault(profile, []).append(profile_metrics)

        folds.append(fold_payload)

    summary = {
        profile: _summarize_profile_metrics(metrics)
        for profile, metrics in profile_metrics_store.items()
    }
    gate = _evaluate_gate(summary, policy)

    return {
        "policy": policy,
        "folds": folds,
        "summary": summary,
        "gate": gate,
    }
