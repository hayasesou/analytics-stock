from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from typing import Any, Iterable

import pandas as pd


@dataclass(frozen=True)
class RiskThresholds:
    max_drawdown_breach: float = -0.03
    min_sharpe_20d: float = 0.0


@dataclass(frozen=True)
class StrategyRiskThresholds:
    max_drawdown_breach: float = -0.03
    warning_sharpe_threshold: float = 0.30
    warning_consecutive_days: int = 3
    halt_sharpe_threshold: float = 0.0
    halt_consecutive_days: int = 2
    cooldown_hours: int = 24


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _count_consecutive_sharpe_breach(history_desc: list[dict[str, Any]], threshold: float) -> int:
    count = 0
    previous_day: date | None = None
    for row in history_desc:
        day = _to_date(row.get("as_of_date", row.get("as_of")))
        if day is None:
            break
        sharpe = _to_optional_float(row.get("sharpe_20d"))
        if sharpe is None or sharpe >= threshold:
            break
        if previous_day is not None:
            gap_days = (previous_day - day).days
            # Weekend gaps are acceptable; larger gaps break the consecutive assumption.
            if gap_days < 1 or gap_days > 3:
                break
        count += 1
        previous_day = day
    return count


def evaluate_strategy_risk_gate(
    *,
    now: datetime,
    drawdown: float | None,
    sharpe_20d: float | None,
    history_desc: list[dict[str, Any]],
    thresholds: StrategyRiskThresholds,
    existing_cooldown_until: datetime | None = None,
) -> tuple[str, dict[str, Any], datetime | None]:
    now_utc = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    cooldown_until = existing_cooldown_until
    if cooldown_until is not None:
        cooldown_until = _to_datetime(cooldown_until)

    drawdown_breach = bool(drawdown is not None and drawdown <= thresholds.max_drawdown_breach)
    warning_count = _count_consecutive_sharpe_breach(history_desc, thresholds.warning_sharpe_threshold)
    halt_count = _count_consecutive_sharpe_breach(history_desc, thresholds.halt_sharpe_threshold)
    warning_breach = warning_count >= max(1, int(thresholds.warning_consecutive_days))
    halt_breach = halt_count >= max(1, int(thresholds.halt_consecutive_days))
    cooldown_active = bool(cooldown_until is not None and cooldown_until > now_utc)

    state = "normal"
    if drawdown_breach or halt_breach:
        state = "halted"
        candidate_cooldown = now_utc + timedelta(hours=max(1, int(thresholds.cooldown_hours)))
        if cooldown_until is None or candidate_cooldown > cooldown_until:
            cooldown_until = candidate_cooldown
    elif cooldown_active:
        state = "cooldown"
    elif warning_breach:
        state = "warning"

    triggers: dict[str, Any] = {
        "drawdown": drawdown,
        "sharpe_20d": sharpe_20d,
        "drawdown_breach": drawdown_breach,
        "max_drawdown_breach": thresholds.max_drawdown_breach,
        "warning_sharpe_threshold": thresholds.warning_sharpe_threshold,
        "warning_consecutive_days": thresholds.warning_consecutive_days,
        "warning_consecutive_count": warning_count,
        "warning_breach": warning_breach,
        "halt_sharpe_threshold": thresholds.halt_sharpe_threshold,
        "halt_consecutive_days": thresholds.halt_consecutive_days,
        "halt_consecutive_count": halt_count,
        "halt_breach": halt_breach,
        "cooldown_active": cooldown_active,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
    }
    return state, triggers, cooldown_until


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
