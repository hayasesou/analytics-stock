from __future__ import annotations

from datetime import datetime, timezone
import os
import time
from typing import Any

from src.config import load_runtime_secrets, load_yaml_config
from src.execution.reconcile import ReconcileSettings, reconcile_target_positions
from src.execution.risk import (
    RiskThresholds,
    StrategyRiskThresholds,
    evaluate_risk_state,
    evaluate_strategy_risk_gate,
    rolling_sharpe_annualized,
)
from src.integrations.crypto_gateway import CryptoGatewayClient
from src.integrations.discord import DiscordNotifier
from src.integrations.jp_gateway import JpGatewayClient
from src.integrations.us_gateway import USGatewayClient
from src.storage.db import NeonRepository
from src.types import (
    FillRecord,
    OrderRecord,
    PositionRecord,
    RiskSnapshot,
    StrategyRiskEvent,
    StrategyRiskSnapshot,
)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_qtys(position: dict[str, Any]) -> tuple[float, float]:
    target_qty_raw = position.get("target_qty", position.get("targetQty", 0.0))
    delta_qty_raw = position.get("delta_qty", position.get("deltaQty", target_qty_raw))
    target_qty = _to_float(target_qty_raw, 0.0)
    delta_qty = _to_float(delta_qty_raw, target_qty)
    return target_qty, delta_qty


def _infer_instrument_type(symbol: str, requested: str | None) -> str:
    if requested in {"JP_EQ", "US_EQ", "CRYPTO", "FUT", "FX", "ETF"}:
        return requested
    if symbol.startswith("JP:"):
        return "JP_EQ"
    if symbol.startswith("US:"):
        return "US_EQ"
    return "CRYPTO"


def _market_key(instrument_type: str) -> str:
    if instrument_type == "JP_EQ":
        return "JP"
    if instrument_type == "US_EQ":
        return "US"
    return "CRYPTO"


def _resolve_thresholds(cfg: dict[str, Any]) -> RiskThresholds:
    execution_cfg = cfg.get("execution", {})
    risk_cfg = execution_cfg.get("risk_gate", {})
    return RiskThresholds(
        max_drawdown_breach=_to_float(risk_cfg.get("max_drawdown_breach"), -0.03),
        min_sharpe_20d=_to_float(risk_cfg.get("min_sharpe_20d"), 0.0),
    )


def _resolve_sharpe_window_days(cfg: dict[str, Any]) -> int:
    execution_cfg = cfg.get("execution", {})
    risk_cfg = execution_cfg.get("risk_gate", {})
    try:
        return max(5, int(risk_cfg.get("rolling_sharpe_window_days", 20)))
    except (TypeError, ValueError):
        return 20


def _normalize_rating_set(values: Any, default: list[str]) -> set[str]:
    if not isinstance(values, list):
        values = list(default)
    normalized = {str(v).strip().upper() for v in values if str(v).strip()}
    if not normalized:
        normalized = {str(v).strip().upper() for v in default}
    return normalized


def _resolve_fundamental_overlay(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    overlay_cfg = execution_cfg.get("fundamental_overlay", {})
    if not isinstance(overlay_cfg, dict):
        overlay_cfg = {}
    size_multiplier_raw = overlay_cfg.get("size_multiplier_by_rating", {})
    if not isinstance(size_multiplier_raw, dict):
        size_multiplier_raw = {}
    size_multiplier: dict[str, float] = {}
    for key, value in size_multiplier_raw.items():
        try:
            size_multiplier[str(key).strip().upper()] = float(value)
        except (TypeError, ValueError):
            continue
    return {
        "enabled": bool(overlay_cfg.get("enabled", True)),
        "allow_if_missing": bool(overlay_cfg.get("allow_if_missing", True)),
        "allow_ratings": _normalize_rating_set(
            overlay_cfg.get("trade_allow_ratings"),
            default=["A", "B"],
        ),
        "size_multiplier": {
            "A": size_multiplier.get("A", 1.0),
            "B": size_multiplier.get("B", 0.6),
            "C": size_multiplier.get("C", 0.0),
        },
    }


def _resolve_executor_data_quality(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    dq_cfg = execution_cfg.get("data_quality", {})
    if not isinstance(dq_cfg, dict):
        dq_cfg = {}
    staleness_cfg = dq_cfg.get("max_price_staleness_days", {})
    if not isinstance(staleness_cfg, dict):
        staleness_cfg = {}
    max_staleness_days: dict[str, int] = {}
    defaults = {"JP": 7, "US": 7, "CRYPTO": 2}
    for market_key, default_days in defaults.items():
        raw = staleness_cfg.get(market_key, default_days)
        try:
            max_staleness_days[market_key] = max(0, int(raw))
        except (TypeError, ValueError):
            max_staleness_days[market_key] = default_days
    return {
        "enabled": bool(dq_cfg.get("enabled", False)),
        "reject_on_missing_price": bool(dq_cfg.get("reject_on_missing_price", True)),
        "max_staleness_days": max_staleness_days,
    }


def _resolve_reconcile_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    reconcile_cfg = execution_cfg.get("order_reconcile", {})
    if not isinstance(reconcile_cfg, dict):
        reconcile_cfg = {}
    return {
        "enabled": bool(reconcile_cfg.get("enabled", True)),
        "min_abs_delta_qty": max(0.0, _to_float(reconcile_cfg.get("min_abs_delta_qty"), 0.0)),
        "min_abs_delta_notional": max(0.0, _to_float(reconcile_cfg.get("min_abs_delta_notional"), 0.0)),
        "open_order_policy": str(reconcile_cfg.get("open_order_policy", "skip")).strip().lower() or "skip",
        "net_notional_epsilon": max(0.0, _to_float(reconcile_cfg.get("net_notional_epsilon"), 10.0)),
        "neutrality_strategy_types": {
            str(v).strip().lower()
            for v in (
                reconcile_cfg.get(
                    "neutrality_strategy_types",
                    ["perp_perp", "cash_carry", "pair", "stat_arb"],
                )
                or []
            )
            if str(v).strip()
        },
    }


def _resolve_strategy_risk_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    strategy_cfg = execution_cfg.get("strategy_risk_gate", {})
    if not isinstance(strategy_cfg, dict):
        strategy_cfg = {}
    return {
        "enabled": bool(strategy_cfg.get("enabled", True)),
        "max_drawdown_breach": _to_float(strategy_cfg.get("max_drawdown_breach"), -0.03),
        "warning_sharpe_threshold": _to_float(strategy_cfg.get("warning_sharpe_threshold"), 0.30),
        "warning_consecutive_days": max(1, int(_to_float(strategy_cfg.get("warning_consecutive_days"), 3))),
        "halt_sharpe_threshold": _to_float(strategy_cfg.get("halt_sharpe_threshold"), 0.0),
        "halt_consecutive_days": max(1, int(_to_float(strategy_cfg.get("halt_consecutive_days"), 2))),
        "cooldown_hours": max(1, int(_to_float(strategy_cfg.get("cooldown_hours"), 24))),
        "panic_close_on_halt": bool(strategy_cfg.get("panic_close_on_halt", True)),
        "symbol_lookback_days": max(1, int(_to_float(strategy_cfg.get("symbol_lookback_days"), 30))),
    }



__all__ = [
    "_to_float",
    "_to_optional_float",
    "_extract_qtys",
    "_infer_instrument_type",
    "_market_key",
    "_resolve_thresholds",
    "_resolve_sharpe_window_days",
    "_normalize_rating_set",
    "_resolve_fundamental_overlay",
    "_resolve_executor_data_quality",
    "_resolve_reconcile_cfg",
    "_resolve_strategy_risk_cfg",
]
