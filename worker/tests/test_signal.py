from datetime import date

import pandas as pd

from src.analytics.signal import generate_b_mode_signals


def test_signal_condition_and_cap() -> None:
    rows = []
    for i in range(1, 16):
        rows.append(
            {
                "security_id": f"JP:{1000 + i}",
                "market": "JP",
                "mixed_rank": i,
                "confidence": "High" if i <= 12 else "Medium",
                "combined_score": 100 - i,
            }
        )
    top50 = pd.DataFrame(rows)
    signals = generate_b_mode_signals(top50, as_of_date=date(2026, 2, 7), risk_alert_mode=False)

    assert signals.loc[signals["mixed_rank"] <= 10, "is_signal"].all()
    assert signals["entry_allowed"].sum() == 3


def test_signal_cap_in_risk_alert_mode() -> None:
    top50 = pd.DataFrame(
        [
            {"security_id": "JP:1111", "market": "JP", "mixed_rank": 1, "confidence": "High", "combined_score": 99},
            {"security_id": "US:1", "market": "US", "mixed_rank": 2, "confidence": "High", "combined_score": 98},
        ]
    )

    signals = generate_b_mode_signals(top50, as_of_date=date(2026, 2, 7), risk_alert_mode=True)
    assert signals["entry_allowed"].sum() == 1
