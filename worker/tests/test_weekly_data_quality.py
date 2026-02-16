from __future__ import annotations

from datetime import date

import pandas as pd

from src.jobs.weekly import (
    _compute_market_price_coverage,
    _enforce_weekly_data_quality,
    _resolve_weekly_data_quality_policy,
)
from src.types import Security


def test_compute_market_price_coverage_counts_recent_rows() -> None:
    securities = [
        Security(security_id="JP:1111", market="JP", ticker="1111", name="A"),
        Security(security_id="JP:2222", market="JP", ticker="2222", name="B"),
        Security(security_id="US:AAPL", market="US", ticker="AAPL", name="C"),
        Security(security_id="US:MSFT", market="US", ticker="MSFT", name="D"),
    ]
    prices = pd.DataFrame(
        [
            {"security_id": "JP:1111", "market": "JP", "trade_date": date(2026, 2, 14)},
            {"security_id": "US:AAPL", "market": "US", "trade_date": date(2026, 2, 14)},
            # 古い行（lookback外）
            {"security_id": "JP:2222", "market": "JP", "trade_date": date(2025, 12, 1)},
        ]
    )
    coverage = _compute_market_price_coverage(
        securities=securities,
        prices=prices,
        as_of_date=date(2026, 2, 16),
        lookback_days=14,
    )

    assert coverage["JP"]["total"] == 2
    assert coverage["JP"]["covered"] == 1
    assert coverage["JP"]["coverage_ratio"] == 0.5
    assert coverage["US"]["total"] == 2
    assert coverage["US"]["covered"] == 1
    assert coverage["US"]["coverage_ratio"] == 0.5


def test_enforce_weekly_data_quality_raises_on_breach() -> None:
    policy = _resolve_weekly_data_quality_policy(
        {
            "data_quality": {
                "weekly": {
                    "enabled": True,
                    "lookback_days": 14,
                    "min_coverage_ratio": {"JP": 0.8, "US": 0.8},
                }
            }
        }
    )
    coverage = {
        "JP": {"total": 10, "covered": 7, "coverage_ratio": 0.7, "latest_trade_date": date(2026, 2, 16)},
        "US": {"total": 10, "covered": 9, "coverage_ratio": 0.9, "latest_trade_date": date(2026, 2, 16)},
    }

    try:
        _enforce_weekly_data_quality(policy, coverage)
    except RuntimeError as exc:
        assert "JP" in str(exc)
    else:
        raise AssertionError("expected RuntimeError when coverage is below threshold")
