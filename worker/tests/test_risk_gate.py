from __future__ import annotations

from src.execution.risk import RiskThresholds, evaluate_risk_state, rolling_sharpe_annualized


def test_evaluate_risk_state_halts_on_drawdown_breach() -> None:
    state, triggers = evaluate_risk_state(
        drawdown=-0.031,
        sharpe_20d=0.4,
        thresholds=RiskThresholds(max_drawdown_breach=-0.03, min_sharpe_20d=0.1),
    )

    assert state == "halted"
    assert triggers["drawdown_breach"] is True
    assert triggers["sharpe_breach"] is False


def test_evaluate_risk_state_flags_sharpe_breach() -> None:
    state, triggers = evaluate_risk_state(
        drawdown=-0.01,
        sharpe_20d=-0.2,
        thresholds=RiskThresholds(max_drawdown_breach=-0.03, min_sharpe_20d=0.0),
    )

    assert state == "risk_alert"
    assert triggers["drawdown_breach"] is False
    assert triggers["sharpe_breach"] is True


def test_rolling_sharpe_returns_none_for_short_or_flat_series() -> None:
    assert rolling_sharpe_annualized([0.01, 0.02], window=20) is None
    assert rolling_sharpe_annualized([0.0] * 30, window=20) is None


def test_rolling_sharpe_returns_value_for_valid_window() -> None:
    value = rolling_sharpe_annualized(
        [0.01, -0.005, 0.006, 0.004, -0.003, 0.007, 0.002, -0.001] * 3,
        window=20,
    )
    assert value is not None
    assert value > 0
