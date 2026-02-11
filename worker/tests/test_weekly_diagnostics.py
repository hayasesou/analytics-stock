from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.jobs.weekly import _compute_signal_diagnostics


def _prices() -> pd.DataFrame:
    start = date(2025, 1, 1)
    rows: list[dict[str, object]] = []
    px = 100.0
    for i in range(140):
        d = start + timedelta(days=i)
        if d.weekday() >= 5:
            continue
        px *= 1.01
        rows.append({"security_id": "JP:1111", "trade_date": d, "close_raw": px})
    return pd.DataFrame(rows)


def test_compute_signal_diagnostics_returns_5d_20d_60d_rows() -> None:
    prices = _prices()
    signal_history = pd.DataFrame(
        [
            {"security_id": "JP:1111", "as_of_date": date(2025, 1, 10)},
            {"security_id": "JP:1111", "as_of_date": date(2025, 1, 17)},
        ]
    )

    rows = _compute_signal_diagnostics(prices, signal_history, horizons=(5, 20, 60))

    assert [r["horizon_days"] for r in rows] == [5, 20, 60]
    assert rows[0]["sample_size"] > 0
    assert rows[1]["sample_size"] > 0
    assert rows[2]["sample_size"] > 0
    assert rows[0]["hit_rate"] == 1.0
    assert rows[1]["hit_rate"] == 1.0
    assert rows[2]["hit_rate"] == 1.0


def test_compute_signal_diagnostics_returns_zero_sample_when_no_history() -> None:
    rows = _compute_signal_diagnostics(
        _prices(),
        pd.DataFrame(columns=["security_id", "as_of_date"]),
        horizons=(5, 20, 60),
    )

    assert [r["horizon_days"] for r in rows] == [5, 20, 60]
    assert rows[0]["sample_size"] == 0
    assert rows[1]["sample_size"] == 0
    assert rows[2]["sample_size"] == 0
    assert rows[0]["hit_rate"] == 0.0
    assert rows[1]["median_return"] is None
    assert rows[2]["median_return"] is None
