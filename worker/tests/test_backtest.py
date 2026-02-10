from datetime import date, timedelta

import pandas as pd

from src.analytics.backtest import run_backtest


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
                "standard": {"jp_round_trip_one_way": 0.001, "us_round_trip_one_way": 0.0008},
                "strict": {"jp_round_trip_one_way": 0.0015, "us_round_trip_one_way": 0.0012},
            }
        },
    }


def test_backtest_returns_profiles() -> None:
    start = date(2025, 1, 1)
    rows = []
    price = 100.0
    for i in range(80):
        d = start + timedelta(days=i)
        if d.weekday() >= 5:
            continue
        price *= 1.003
        rows.append(
            {
                "security_id": "JP:1111",
                "trade_date": d,
                "open_raw": price,
                "high_raw": price * 1.01,
                "low_raw": price * 0.99,
                "close_raw": price,
            }
        )
    prices = pd.DataFrame(rows)

    signals = pd.DataFrame(
        [
            {
                "security_id": "JP:1111",
                "market": "JP",
                "as_of_date": start,
                "is_signal": True,
                "entry_allowed": True,
            }
        ]
    )

    results = run_backtest(prices, signals, _config())
    assert len(results) == 3

    metric_by_profile = {r.cost_profile: r.metrics for r in results}
    assert metric_by_profile["strict"]["cagr"] <= metric_by_profile["zero"]["cagr"]
