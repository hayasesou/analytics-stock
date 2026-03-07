from __future__ import annotations

from datetime import datetime
from math import sqrt
from typing import Any, Iterable, Protocol

from src.types import EdgeState


class EdgeStateStrategy(Protocol):
    """Unified strategy contract for producing one normalized EdgeState."""

    def compute_edge_state(
        self,
        *,
        observed_at: datetime,
        snapshot: dict[str, Any],
        params: dict[str, Any],
    ) -> EdgeState:
        ...


def compute_ewma_zscore(values: Iterable[float], alpha: float = 0.2) -> float | None:
    series = [float(v) for v in values if v is not None]
    if len(series) < 2:
        return None

    alpha_value = max(0.01, min(0.99, float(alpha)))
    mean = float(series[0])
    var = 0.0
    for x in series[1:]:
        delta = x - mean
        mean = (alpha_value * x) + ((1.0 - alpha_value) * mean)
        var = (alpha_value * (delta * delta)) + ((1.0 - alpha_value) * var)

    std = sqrt(max(var, 0.0))
    if std <= 1e-12:
        return None
    return (series[-1] - mean) / std


def compute_expected_net_edge_bps(
    spread_bps: float,
    funding_bps: float,
    basis_bps: float,
    fee_bps: float,
    slippage_bps: float,
    borrow_bps: float,
) -> float:
    return float(spread_bps + funding_bps + basis_bps - fee_bps - slippage_bps - borrow_bps)


def check_dollar_neutrality(
    net_notional_usd: float | None,
    epsilon_notional_usd: float,
) -> tuple[bool, str | None]:
    if net_notional_usd is None:
        return False, "missing_net_notional"
    epsilon = max(0.0, float(epsilon_notional_usd))
    if abs(float(net_notional_usd)) > epsilon:
        return False, "delta_neutrality_breach"
    return True, None


def compute_edge_score(expected_net_edge_bps: float | None, score_per_bps: float = 3.0) -> float:
    if expected_net_edge_bps is None:
        return 0.0
    return max(0.0, min(100.0, 50.0 + (float(expected_net_edge_bps) * float(score_per_bps))))


def compute_distance_to_entry_bps(expected_net_edge_bps: float | None, min_entry_bps: float) -> float | None:
    if expected_net_edge_bps is None:
        return None
    return max(0.0, float(min_entry_bps) - float(expected_net_edge_bps))
