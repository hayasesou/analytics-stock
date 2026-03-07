from __future__ import annotations

from datetime import datetime, timezone

from src.execution.risk import StrategyRiskThresholds, evaluate_strategy_risk_gate


def test_strategy_risk_warning_on_three_consecutive_sharpe_breaches() -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    history = [
        {"as_of_date": "2026-02-20", "sharpe_20d": 0.2},
        {"as_of_date": "2026-02-19", "sharpe_20d": 0.25},
        {"as_of_date": "2026-02-18", "sharpe_20d": 0.1},
    ]
    state, triggers, cooldown_until = evaluate_strategy_risk_gate(
        now=now,
        drawdown=-0.01,
        sharpe_20d=0.2,
        history_desc=history,
        thresholds=StrategyRiskThresholds(),
        existing_cooldown_until=None,
    )

    assert state == "warning"
    assert triggers["warning_breach"] is True
    assert triggers["halt_breach"] is False
    assert cooldown_until is None


def test_strategy_risk_halt_on_two_consecutive_negative_sharpe() -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    history = [
        {"as_of_date": "2026-02-20", "sharpe_20d": -0.05},
        {"as_of_date": "2026-02-19", "sharpe_20d": -0.02},
    ]
    state, triggers, cooldown_until = evaluate_strategy_risk_gate(
        now=now,
        drawdown=-0.01,
        sharpe_20d=-0.05,
        history_desc=history,
        thresholds=StrategyRiskThresholds(),
        existing_cooldown_until=None,
    )

    assert state == "halted"
    assert triggers["halt_breach"] is True
    assert cooldown_until is not None
    assert cooldown_until > now


def test_strategy_risk_halt_on_drawdown_breach_even_if_sharpe_ok() -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    state, triggers, cooldown_until = evaluate_strategy_risk_gate(
        now=now,
        drawdown=-0.05,
        sharpe_20d=0.8,
        history_desc=[{"as_of_date": "2026-02-20", "sharpe_20d": 0.8}],
        thresholds=StrategyRiskThresholds(),
        existing_cooldown_until=None,
    )

    assert state == "halted"
    assert triggers["drawdown_breach"] is True
    assert cooldown_until is not None


def test_strategy_risk_cooldown_state_rejects_new_entries() -> None:
    now = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
    cooldown_until = datetime(2026, 2, 21, 12, 0, tzinfo=timezone.utc)
    state, triggers, returned_cooldown = evaluate_strategy_risk_gate(
        now=now,
        drawdown=-0.01,
        sharpe_20d=0.5,
        history_desc=[{"as_of_date": "2026-02-20", "sharpe_20d": 0.5}],
        thresholds=StrategyRiskThresholds(),
        existing_cooldown_until=cooldown_until,
    )

    assert state == "cooldown"
    assert triggers["cooldown_active"] is True
    assert returned_cooldown == cooldown_until
