from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.analytics.validation import resolve_validation_policy, run_walk_forward_validation


def _build_price_history(days: int = 800) -> pd.DataFrame:
    start = date(2022, 1, 3)
    rows = []
    price = 100.0
    for i in range(days):
        d = start + timedelta(days=i)
        if d.weekday() >= 5:
            continue
        # 緩やかな上昇トレンド
        price *= 1.0012
        rows.append(
            {
                "security_id": "JP:1111",
                "market": "JP",
                "trade_date": d,
                "open_raw": price,
                "high_raw": price * 1.01,
                "low_raw": price * 0.99,
                "close_raw": price,
            }
        )
    return pd.DataFrame(rows)


def _config() -> dict:
    return {
        "risk_management": {
            "atr": {
                "initial_stop_multiple": 2.5,
                "trailing_stop_multiple": 3.0,
                "partial_take_profit": {"threshold": 0.2},
            }
        },
        "backtest": {
            "costs": {
                "zero": {"jp_round_trip_one_way": 0.0, "us_round_trip_one_way": 0.0},
                "strict": {"jp_round_trip_one_way": 0.0012, "us_round_trip_one_way": 0.0012},
            }
        },
        "strategy_factory": {
            "validation": {
                "enabled": True,
                "train_days": 120,
                "test_days": 40,
                "step_days": 40,
                "min_signals_per_fold": 1,
                "gates": {
                    "min_fold_count": 2,
                    "min_total_trades": 2,
                    "min_median_sharpe": -1.0,
                    "min_mean_sharpe": -1.0,
                    "min_worst_max_dd": -1.0,
                    "min_strict_median_sharpe": -1.0,
                },
            }
        },
    }


def test_resolve_validation_policy_clamps_invalid_values() -> None:
    policy = resolve_validation_policy(
        {
            "strategy_factory": {
                "validation": {
                    "train_days": "-10",
                    "test_days": "x",
                    "momentum_quantile": 9.9,
                    "max_volatility_20d": -1,
                    "gates": {"min_fold_count": "0"},
                }
            }
        }
    )
    assert policy["train_days"] >= 1
    assert policy["test_days"] >= 1
    assert 0.05 <= policy["momentum_quantile"] <= 0.95
    assert policy["max_volatility_20d"] > 0
    assert policy["gates"]["min_fold_count"] >= 1


def test_run_walk_forward_validation_returns_structured_result() -> None:
    prices = _build_price_history()
    cfg = _config()
    policy = resolve_validation_policy(cfg)

    result = run_walk_forward_validation(
        prices=prices,
        security_id="JP:1111",
        market="JP",
        config=cfg,
        policy=policy,
    )

    assert "policy" in result
    assert "folds" in result
    assert "summary" in result
    assert "gate" in result
    assert isinstance(result["folds"], list)
    assert len(result["folds"]) > 0
    assert isinstance(result["gate"]["reasons"], list)


def test_run_walk_forward_validation_handles_empty_prices() -> None:
    result = run_walk_forward_validation(
        prices=pd.DataFrame(),
        security_id="JP:1111",
        market="JP",
        config=_config(),
        policy=resolve_validation_policy(_config()),
    )
    assert result["gate"]["passed"] is False
    assert "no_price_data" in result["gate"]["reasons"]
