from __future__ import annotations

from dataclasses import asdict
from datetime import date
from math import sqrt
from typing import Any

import numpy as np
import pandas as pd

from src.types import BacktestResult, BacktestTrade


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close_raw"].shift(1)
    tr = pd.concat(
        [
            df["high_raw"] - df["low_raw"],
            (df["high_raw"] - prev_close).abs(),
            (df["low_raw"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def _simulate_single_trade(
    sec_prices: pd.DataFrame,
    signal_date: date,
    stop_multiple: float,
    trail_multiple: float,
    take_profit_ratio: float,
) -> dict[str, Any] | None:
    sec_prices = sec_prices.copy().sort_values("trade_date").reset_index(drop=True)
    sec_prices["atr14"] = _atr(sec_prices, 14)

    entry_candidates = sec_prices[sec_prices["trade_date"] > signal_date]
    if entry_candidates.empty:
        return None

    entry_row = entry_candidates.iloc[0]
    entry_idx = int(entry_row.name)
    entry_date = entry_row["trade_date"]
    entry_price = float(entry_row["open_raw"])

    atr0 = float(entry_row["atr14"]) if pd.notna(entry_row["atr14"]) else entry_price * 0.02
    stop = entry_price - stop_multiple * atr0
    high_since = entry_price

    realized_return = 0.0
    remaining_qty = 1.0
    took_partial = False

    exit_date = entry_date
    exit_price = float(entry_row["close_raw"])
    exit_reason = "time_exit"

    max_hold = min(entry_idx + 63, len(sec_prices) - 1)

    for i in range(entry_idx + 1, max_hold + 1):
        row = sec_prices.iloc[i]
        trade_date = row["trade_date"]
        atr_i = float(row["atr14"]) if pd.notna(row["atr14"]) else atr0

        high_since = max(high_since, float(row["high_raw"]))
        trailing_stop = max(stop, high_since - trail_multiple * atr_i)

        if (not took_partial) and float(row["high_raw"]) >= entry_price * (1.0 + take_profit_ratio):
            realized_return += 0.5 * take_profit_ratio
            remaining_qty = 0.5
            took_partial = True

        if float(row["low_raw"]) <= trailing_stop:
            exit_date = trade_date
            exit_price = trailing_stop
            exit_reason = "atr_trailing_stop"
            break

        exit_date = trade_date
        exit_price = float(row["close_raw"])

    gross_return = realized_return + remaining_qty * ((exit_price / entry_price) - 1.0)

    return {
        "entry_date": entry_date,
        "entry_price": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "gross_return": gross_return,
        "exit_reason": exit_reason,
    }


def _build_equity_curve(
    trades: list[BacktestTrade],
    benchmark_curve: pd.DataFrame,
) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(columns=["trade_date", "equity", "benchmark_equity"])

    start = min(t.entry_date for t in trades)
    end = max(t.exit_date for t in trades)
    dates = pd.bdate_range(start=start, end=end)
    df = pd.DataFrame({"trade_date": dates.date})
    df["equity"] = 1.0

    pnl_on_exit: dict[date, float] = {}
    for t in trades:
        pnl_on_exit[t.exit_date] = pnl_on_exit.get(t.exit_date, 0.0) + t.net_pnl

    eq = 1.0
    vals: list[float] = []
    for d in df["trade_date"]:
        if d in pnl_on_exit:
            eq *= max(1.0 + pnl_on_exit[d], 0.01)
        vals.append(eq)
    df["equity"] = vals

    if benchmark_curve.empty:
        df["benchmark_equity"] = np.nan
        return df

    bench = benchmark_curve.copy()
    bench["trade_date"] = pd.to_datetime(bench["trade_date"]).dt.date
    bench = bench.set_index("trade_date").reindex(df["trade_date"]).ffill().reset_index()
    bench.rename(columns={"index": "trade_date"}, inplace=True)
    df["benchmark_equity"] = bench["benchmark_equity"].values

    return df


def _metrics_from_curve(curve: pd.DataFrame, trades: list[BacktestTrade]) -> dict[str, float]:
    if curve.empty:
        return {
            "cagr": 0.0,
            "max_dd": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "volatility": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "alpha_simple": 0.0,
            "information_ratio_simple": 0.0,
        }

    curve = curve.copy()
    curve["ret"] = curve["equity"].pct_change().fillna(0.0)

    days = max(len(curve), 1)
    years = max(days / 252.0, 1 / 252.0)
    final_equity = float(curve["equity"].iloc[-1])
    cagr = (final_equity ** (1.0 / years)) - 1.0

    roll_max = curve["equity"].cummax()
    drawdown = curve["equity"] / roll_max - 1.0
    max_dd = float(drawdown.min())

    vol = float(curve["ret"].std(ddof=0) * sqrt(252)) if len(curve) > 1 else 0.0
    mean_ret = float(curve["ret"].mean())
    std_ret = float(curve["ret"].std(ddof=0))
    sharpe = (mean_ret / std_ret * sqrt(252)) if std_ret > 0 else 0.0

    downside = curve.loc[curve["ret"] < 0, "ret"]
    downside_std = float(downside.std(ddof=0)) if len(downside) else 0.0
    sortino = (mean_ret / downside_std * sqrt(252)) if downside_std > 0 else 0.0

    wins = [t.net_pnl for t in trades if t.net_pnl > 0]
    losses = [t.net_pnl for t in trades if t.net_pnl <= 0]
    win_rate = (len(wins) / len(trades)) if trades else 0.0
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0

    alpha = 0.0
    ir = 0.0
    if "benchmark_equity" in curve.columns and curve["benchmark_equity"].notna().any():
        bench = curve["benchmark_equity"].ffill().bfill().replace(0, np.nan)
        bench_ret = bench.pct_change().fillna(0.0)
        bench_final = float(bench.iloc[-1])
        bench_cagr = (bench_final ** (1.0 / years)) - 1.0 if bench_final > 0 else 0.0
        alpha = cagr - bench_cagr

        excess = curve["ret"] - bench_ret
        ex_std = float(excess.std(ddof=0))
        ir = float(excess.mean() / ex_std * sqrt(252)) if ex_std > 0 else 0.0

    return {
        "cagr": cagr,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "sortino": sortino,
        "volatility": vol,
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "alpha_simple": alpha,
        "information_ratio_simple": ir,
    }


def _build_benchmark_curve(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["trade_date", "benchmark_equity"])

    daily = (
        prices.groupby("trade_date", as_index=False)["close_raw"]
        .mean()
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    daily["ret"] = daily["close_raw"].pct_change().fillna(0.0)
    daily["benchmark_equity"] = (1.0 + daily["ret"]).cumprod()
    return daily[["trade_date", "benchmark_equity"]]


def run_backtest(
    prices: pd.DataFrame,
    signals: pd.DataFrame,
    config: dict[str, Any],
) -> list[BacktestResult]:
    if prices.empty or signals.empty:
        return []

    prices = prices.copy()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.date
    prices = prices.sort_values(["security_id", "trade_date"])

    signals = signals.copy()
    signals = signals[(signals["is_signal"]) & (signals["entry_allowed"])].reset_index(drop=True)
    if signals.empty:
        return []

    atr_cfg = config["risk_management"]["atr"]
    stop_multiple = float(atr_cfg["initial_stop_multiple"])
    trail_multiple = float(atr_cfg["trailing_stop_multiple"])
    take_profit_ratio = float(atr_cfg["partial_take_profit"]["threshold"])

    costs = config["backtest"]["costs"]
    benchmark_curve = _build_benchmark_curve(prices)

    precomputed: list[dict[str, Any]] = []
    for _, sig in signals.iterrows():
        sec_id = sig["security_id"]
        sec_prices = prices[prices["security_id"] == sec_id]
        result = _simulate_single_trade(
            sec_prices,
            sig["as_of_date"],
            stop_multiple,
            trail_multiple,
            take_profit_ratio,
        )
        if result is None:
            continue
        result["security_id"] = sec_id
        result["market"] = sig["market"]
        precomputed.append(result)

    all_results: list[BacktestResult] = []

    for profile_name, c in costs.items():
        trades: list[BacktestTrade] = []
        for t in precomputed:
            market = t["market"]
            one_way = float(c["jp_round_trip_one_way"] if market == "JP" else c["us_round_trip_one_way"])
            cost = one_way * 2
            net_return = t["gross_return"] - cost
            entry_price = float(t["entry_price"])
            qty = 1.0
            gross_pnl = entry_price * t["gross_return"]
            net_pnl = entry_price * net_return

            trades.append(
                BacktestTrade(
                    security_id=t["security_id"],
                    market=market,
                    entry_date=t["entry_date"],
                    entry_price=entry_price,
                    exit_date=t["exit_date"],
                    exit_price=float(t["exit_price"]),
                    quantity=qty,
                    gross_pnl=float(gross_pnl),
                    net_pnl=float(net_return),
                    cost=float(cost),
                    exit_reason=t["exit_reason"],
                )
            )

        curve = _build_equity_curve(trades, benchmark_curve)
        metrics = _metrics_from_curve(curve, trades)

        equity_points = [
            {
                "trade_date": row["trade_date"],
                "equity": float(row["equity"]),
                "benchmark_equity": float(row["benchmark_equity"]) if pd.notna(row["benchmark_equity"]) else None,
            }
            for _, row in curve.iterrows()
        ]

        all_results.append(
            BacktestResult(
                cost_profile=profile_name,
                metrics=metrics,
                equity_curve=equity_points,
                trades=trades,
            )
        )

    return all_results


def serialize_backtest_results(results: list[BacktestResult]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for r in results:
        payload[r.cost_profile] = {
            "metrics": r.metrics,
            "equity_curve": r.equity_curve,
            "trades": [asdict(t) for t in r.trades],
        }
    return payload
