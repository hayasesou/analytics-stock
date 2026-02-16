from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class RiskThresholds:
    max_drawdown_breach: float = -0.03
    min_sharpe_20d: float = 0.0


def evaluate_risk_state(
    drawdown: float,
    sharpe_20d: float | None,
    thresholds: RiskThresholds = RiskThresholds(),
) -> tuple[str, dict[str, float | bool]]:
    triggers: dict[str, float | bool] = {
        "drawdown_breach": drawdown <= thresholds.max_drawdown_breach,
        "sharpe_breach": bool(sharpe_20d is not None and sharpe_20d < thresholds.min_sharpe_20d),
        "drawdown": drawdown,
        "max_drawdown_breach": thresholds.max_drawdown_breach,
    }
    if sharpe_20d is not None:
        triggers["sharpe_20d"] = sharpe_20d
        triggers["min_sharpe_20d"] = thresholds.min_sharpe_20d

    if bool(triggers["drawdown_breach"]):
        return "halted", triggers
    if bool(triggers["sharpe_breach"]):
        return "risk_alert", triggers
    return "normal", triggers


def rolling_sharpe_annualized(
    returns: Iterable[float],
    window: int = 20,
    annualization: int = 252,
) -> float | None:
    window_size = max(2, int(window))
    sample = pd.Series(list(returns), dtype=float).dropna()
    if len(sample) < window_size:
        return None

    recent = sample.iloc[-window_size:]
    std = float(recent.std(ddof=1))
    if std == 0:
        return None
    return float((recent.mean() / std) * sqrt(float(annualization)))
