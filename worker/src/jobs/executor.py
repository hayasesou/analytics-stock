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


def _price_row_is_stale(
    latest_price_row: dict[str, Any] | None,
    market_key: str,
    now: datetime,
    max_staleness_days: dict[str, int],
) -> bool:
    if not latest_price_row:
        return True
    trade_date_value = latest_price_row.get("trade_date")
    trade_date = None
    if isinstance(trade_date_value, datetime):
        trade_date = trade_date_value.date()
    elif hasattr(trade_date_value, "year") and hasattr(trade_date_value, "month") and hasattr(trade_date_value, "day"):
        # date-like object
        try:
            trade_date = trade_date_value
        except Exception:  # noqa: BLE001
            trade_date = None
    elif trade_date_value is not None:
        text = str(trade_date_value).strip()
        if text:
            try:
                trade_date = datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            except ValueError:
                trade_date = None
    if trade_date is None:
        return True
    days_old = (now.date() - trade_date).days
    allowed = int(max_staleness_days.get(market_key, 7))
    return days_old > allowed


def _compute_sharpe_from_history(
    history_rows: list[dict[str, Any]],
    window_days: int,
) -> float | None:
    if len(history_rows) < 2:
        return None
    # Convert descending snapshots into ascending equity series.
    equities = []
    for row in reversed(history_rows):
        equity = _to_optional_float(row.get("equity"))
        if equity is not None:
            equities.append(equity)
    if len(equities) < 2:
        return None
    returns = []
    prev = equities[0]
    for current in equities[1:]:
        if prev and prev != 0:
            returns.append((current / prev) - 1.0)
        prev = current
    if not returns:
        return None
    return rolling_sharpe_annualized(returns, window=window_days, annualization=252)


def _collect_target_symbols(target_positions: list[dict[str, Any]]) -> list[str]:
    symbols: list[str] = []
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if symbol:
            symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def _build_position_qty_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    mapping: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        mapping[symbol] = _to_float(row.get("qty"), 0.0)
    return mapping


def _build_open_order_map(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    mapping: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        mapping.setdefault(symbol, []).append(row)
    return mapping


def _resolve_price_hints_for_symbols(
    repo: NeonRepository,
    *,
    target_positions: list[dict[str, Any]],
    symbols: list[str],
) -> dict[str, float]:
    result: dict[str, float] = {}
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        hinted = _to_float(position.get("price_hint"), 0.0)
        if hinted > 0:
            result[symbol] = hinted

    for symbol in symbols:
        if symbol in result:
            continue
        latest = repo.fetch_latest_price_for_symbol(symbol)
        if not latest:
            continue
        close = _to_float(latest.get("close_raw"), 0.0)
        if close > 0:
            result[symbol] = close
    return result


def _should_enforce_neutrality(
    *,
    risk_checks: dict[str, Any],
    target_positions: list[dict[str, Any]],
    reconcile_cfg: dict[str, Any],
) -> bool:
    if len(target_positions) < 2:
        return False
    if bool(risk_checks.get("delta_neutral_required")):
        return True
    if bool(risk_checks.get("require_dollar_neutral")):
        return True
    strategy_type = str(risk_checks.get("strategy_type", "")).strip().lower()
    return strategy_type in set(reconcile_cfg.get("neutrality_strategy_types", set()))


def _extract_strategy_risk_values(risk_checks: dict[str, Any]) -> tuple[float | None, float | None]:
    drawdown = _to_optional_float(risk_checks.get("strategy_drawdown"))
    if drawdown is None:
        drawdown = _to_optional_float(risk_checks.get("drawdown"))
    sharpe = _to_optional_float(risk_checks.get("strategy_sharpe_20d"))
    if sharpe is None:
        sharpe = _to_optional_float(risk_checks.get("sharpe_20d"))
    return drawdown, sharpe


def _to_utc_datetime(value: Any) -> datetime | None:
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
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _create_risk_snapshot_if_existing(
    repo: NeonRepository,
    portfolio_id: str,
    thresholds: RiskThresholds,
    now: datetime,
    fallback_risk_checks: dict[str, Any],
    sharpe_window_days: int,
) -> tuple[str, dict[str, Any]]:
    latest = repo.fetch_latest_risk_snapshot(portfolio_id)
    history = repo.fetch_recent_risk_snapshots(portfolio_id=portfolio_id, limit=max(sharpe_window_days + 5, 25))
    equity = _to_float((latest or {}).get("equity"), _to_float(fallback_risk_checks.get("equity"), 0.0))
    drawdown = _to_float((latest or {}).get("drawdown"), _to_float(fallback_risk_checks.get("drawdown"), 0.0))
    sharpe_value = (latest or {}).get("sharpe_20d", fallback_risk_checks.get("sharpe_20d"))
    sharpe = _to_float(sharpe_value, default=0.0) if sharpe_value is not None else None
    if sharpe is None:
        sharpe = _compute_sharpe_from_history(history_rows=history, window_days=sharpe_window_days)

    state, triggers = evaluate_risk_state(drawdown=drawdown, sharpe_20d=sharpe, thresholds=thresholds)
    repo.insert_risk_snapshot(
        RiskSnapshot(
            portfolio_id=portfolio_id,
            as_of=now,
            equity=equity,
            drawdown=drawdown,
            sharpe_20d=sharpe,
            gross_exposure=_to_optional_float((latest or {}).get("gross_exposure")),
            net_exposure=_to_optional_float((latest or {}).get("net_exposure")),
            state=state,
            triggers=triggers,
        )
    )
    return state, triggers


def _resolve_crypto_gateway_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    gateway_cfg = execution_cfg.get("gateway_crypto", {})
    if not isinstance(gateway_cfg, dict):
        gateway_cfg = {}
    token_env = str(gateway_cfg.get("auth_token_env", "CRYPTO_GATEWAY_AUTH_TOKEN")).strip() or "CRYPTO_GATEWAY_AUTH_TOKEN"
    return {
        "enabled": bool(gateway_cfg.get("enabled", True)),
        "broker_name": str(gateway_cfg.get("broker_name", "crypto_gateway")),
        "base_url": str(gateway_cfg.get("base_url", os.getenv("CRYPTO_GATEWAY_URL", "http://gateway-crypto:8080"))).rstrip("/"),
        "auth_token": os.getenv(token_env),
        "request_timeout_sec": max(1.0, _to_float(gateway_cfg.get("request_timeout_sec"), 8.0)),
        "default_leg_timeout_sec": max(1, int(_to_float(gateway_cfg.get("default_leg_timeout_sec"), 30.0))),
    }


def _resolve_jp_gateway_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    gateway_cfg = execution_cfg.get("gateway_jp", {})
    if not isinstance(gateway_cfg, dict):
        gateway_cfg = {}
    token_env = str(gateway_cfg.get("auth_token_env", "JP_GATEWAY_AUTH_TOKEN")).strip() or "JP_GATEWAY_AUTH_TOKEN"
    return {
        "enabled": bool(gateway_cfg.get("enabled", True)),
        "broker_name": str(gateway_cfg.get("broker_name", "gateway_jp")),
        "base_url": str(gateway_cfg.get("base_url", os.getenv("JP_GATEWAY_URL", "http://gateway-jp:8081"))).rstrip("/"),
        "auth_token": os.getenv(token_env),
        "request_timeout_sec": max(1.0, _to_float(gateway_cfg.get("request_timeout_sec"), 8.0)),
        "wait_timeout_sec": max(0.1, _to_float(gateway_cfg.get("wait_timeout_sec"), 2.0)),
    }


def _resolve_us_gateway_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    execution_cfg = cfg.get("execution", {})
    gateway_cfg = execution_cfg.get("gateway_us", {})
    if not isinstance(gateway_cfg, dict):
        gateway_cfg = {}
    token_env = str(gateway_cfg.get("auth_token_env", "US_GATEWAY_AUTH_TOKEN")).strip() or "US_GATEWAY_AUTH_TOKEN"
    return {
        "enabled": bool(gateway_cfg.get("enabled", True)),
        "broker_name": str(gateway_cfg.get("broker_name", "gateway_us")),
        "base_url": str(gateway_cfg.get("base_url", os.getenv("US_GATEWAY_URL", "http://gateway-us:8090"))).rstrip("/"),
        "auth_token": os.getenv(token_env),
        "request_timeout_sec": max(1.0, _to_float(gateway_cfg.get("request_timeout_sec"), 8.0)),
        "default_order_timeout_sec": max(1.0, _to_float(gateway_cfg.get("default_order_timeout_sec"), 20.0)),
    }


def _symbol_to_kabu_code(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw


def _build_jp_gateway_payload(
    *,
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    wait_timeout_sec: float,
) -> dict[str, Any]:
    legs: list[dict[str, Any]] = []
    for idx, position in enumerate(target_positions):
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        if instrument_type != "JP_EQ":
            continue
        target_qty, delta_qty = _extract_qtys(position)
        if abs(delta_qty) < 1e-12:
            continue
        side = "BUY" if delta_qty > 0 else ("SELL_SHORT" if target_qty < 0 else "SELL")
        margin_type = str(position.get("margin_type", "cash"))
        if side == "SELL_SHORT" and margin_type == "cash":
            margin_type = "margin_open"
        legs.append(
            {
                "leg_id": f"jp-leg-{idx + 1}",
                "symbol": symbol,
                "kabu_symbol": _symbol_to_kabu_code(symbol),
                "side": side,
                "qty": abs(delta_qty),
                "target_qty": target_qty,
                "order_type": str(position.get("order_type", "MKT")).upper(),
                "limit_price": _to_optional_float(position.get("limit_price")),
                "exchange": int(_to_float(position.get("exchange"), 1)),
                "margin_type": margin_type,
                "margin_trade_type": int(_to_float(position.get("margin_trade_type"), 3)),
                "deliv_type": int(_to_float(position.get("deliv_type"), 2)),
                "account_type": int(_to_float(position.get("account_type"), 4)),
                "expire_day": int(_to_float(position.get("expire_day"), 0)),
                "close_positions": (
                    position.get("close_positions")
                    if isinstance(position.get("close_positions"), list)
                    else None
                ),
                "cancel_replace": bool(position.get("cancel_replace", False)),
                "open_order_ids": (
                    list(position.get("reconcile_open_order_ids"))
                    if isinstance(position.get("reconcile_open_order_ids"), list)
                    else []
                ),
            }
        )

    return {
        "intent_id": intent_id,
        "strategy_version_id": strategy_version_id,
        "portfolio_id": portfolio_id,
        "idempotency_key": f"intent:{intent_id}",
        "wait_timeout_sec": wait_timeout_sec,
        "legs": legs,
    }


def _build_jp_order_records_from_gateway(
    *,
    intent_id: str,
    now: datetime,
    legs: list[dict[str, Any]],
) -> list[OrderRecord]:
    orders: list[OrderRecord] = []
    for idx, leg in enumerate(legs):
        symbol = str(leg.get("symbol", "")).strip()
        side_raw = str(leg.get("side", "BUY")).strip().upper()
        qty = abs(_to_float(leg.get("qty"), 0.0))
        if not symbol or qty <= 0:
            continue
        side = "BUY" if side_raw in {"BUY", "BUY_TO_COVER"} else ("SELL_SHORT" if side_raw == "SELL_SHORT" else "SELL")
        gateway_status = str(leg.get("status", "error")).strip().lower()
        if gateway_status == "diff_skip":
            continue
        order_status = _map_gateway_status_to_order_status(gateway_status)
        orders.append(
            OrderRecord(
                intent_id=intent_id,
                broker="gateway_jp",
                symbol=symbol,
                instrument_type="JP_EQ",
                side=side,
                order_type=str(leg.get("order_type", "MKT")).upper(),
                qty=qty,
                status=order_status,
                idempotency_key=f"{intent_id}:jp:{idx}:gateway_jp:{symbol}:{side}",
                broker_order_id=(
                    str(leg.get("broker_order_id"))
                    if leg.get("broker_order_id") is not None and str(leg.get("broker_order_id")).strip()
                    else None
                ),
                submitted_at=now,
                meta={
                    "executor": "jp_gateway",
                    "reject_reason": leg.get("reject_reason"),
                    "raw": leg,
                },
            )
        )
    return orders


def _build_positions_after_jp_gateway(
    *,
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    intent_status: str,
) -> list[PositionRecord]:
    positions: list[PositionRecord] = []
    qty_for_status = 0.0
    if intent_status == "done":
        qty_for_status = 1.0
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        if instrument_type != "JP_EQ":
            continue
        target_qty, delta_qty = _extract_qtys(position)
        intended_qty = target_qty if ("target_qty" in position or "targetQty" in position) else delta_qty
        positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol,
                instrument_type="JP_EQ",
                qty=(intended_qty * qty_for_status),
                avg_price=None,
                last_price=None,
                market_value=None,
            )
        )
    return positions


def _notify_executor_jp_gateway_failure(
    notifier: DiscordNotifier,
    *,
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_name: str | None,
    gateway_status: str,
    reason: str,
) -> None:
    notifier.send_executor_alert(
        title="jp gateway execution failure",
        details={
            "intent_id": intent_id,
            "strategy_version_id": strategy_version_id or "-",
            "portfolio": portfolio_name or "-",
            "gateway_status": gateway_status,
            "reason": reason,
        },
    )


def _normalize_us_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw


def _build_us_gateway_payload(
    *,
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    default_order_timeout_sec: float,
) -> dict[str, Any]:
    orders: list[dict[str, Any]] = []
    for idx, position in enumerate(target_positions):
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        if instrument_type != "US_EQ":
            continue
        target_qty, delta_qty = _extract_qtys(position)
        if abs(delta_qty) < 1e-12:
            continue
        side = "BUY" if delta_qty > 0 else ("SELL_SHORT" if target_qty < 0 else "SELL")
        orders.append(
            {
                "order_id": f"us-ord-{idx + 1}",
                "symbol": _normalize_us_symbol(symbol),
                "side": side,
                "qty": abs(delta_qty),
                "order_type": str(position.get("order_type", "MKT")).strip().upper(),
                "time_in_force": str(position.get("time_in_force", position.get("tif", "DAY"))).strip().upper() or "DAY",
                "limit_price": _to_optional_float(position.get("limit_price")),
                "price_hint": _to_optional_float(position.get("price_hint")),
                "cancel_replace": bool(position.get("cancel_replace", False)),
                "open_order_ids": (
                    list(position.get("reconcile_open_order_ids"))
                    if isinstance(position.get("reconcile_open_order_ids"), list)
                    else []
                ),
            }
        )
    return {
        "intent_id": intent_id,
        "strategy_version_id": strategy_version_id,
        "portfolio_id": portfolio_id,
        "idempotency_key": f"intent:{intent_id}",
        "timeout_sec": default_order_timeout_sec,
        "orders": orders,
    }


def _build_us_order_records_from_gateway(
    *,
    intent_id: str,
    now: datetime,
    orders: list[dict[str, Any]],
) -> list[OrderRecord]:
    records: list[OrderRecord] = []
    for idx, row in enumerate(orders):
        symbol = str(row.get("symbol", "")).strip().upper()
        side_raw = str(row.get("side", "BUY")).strip().upper()
        side = "BUY" if side_raw in {"BUY", "BUY_TO_COVER"} else ("SELL_SHORT" if side_raw == "SELL_SHORT" else "SELL")
        qty = abs(_to_float(row.get("qty"), 0.0))
        if not symbol or qty <= 0:
            continue
        order_status = _map_gateway_status_to_order_status(str(row.get("status", "error")))
        records.append(
            OrderRecord(
                intent_id=intent_id,
                broker="gateway_us",
                symbol=f"US:{symbol}",
                instrument_type="US_EQ",
                side=side,
                order_type=str(row.get("order_type", "MKT")).strip().upper() or "MKT",
                qty=qty,
                status=order_status,
                idempotency_key=f"{intent_id}:us:{idx}:gateway_us:{symbol}:{side}",
                broker_order_id=(
                    str(row.get("broker_order_id"))
                    if row.get("broker_order_id") is not None and str(row.get("broker_order_id")).strip()
                    else None
                ),
                submitted_at=now,
                meta={
                    "executor": "us_gateway",
                    "reject_reason": row.get("reject_reason"),
                    "raw": row,
                },
            )
        )
    return records


def _build_fill_records_from_gateway_events(
    *,
    order_id_by_broker_order_id: dict[str, str],
    fill_events: list[dict[str, Any]],
    now: datetime,
) -> list[FillRecord]:
    fills: list[FillRecord] = []
    for event in fill_events:
        if not isinstance(event, dict):
            continue
        broker_order_id = str(event.get("broker_order_id", "")).strip()
        if not broker_order_id:
            continue
        order_id = order_id_by_broker_order_id.get(broker_order_id)
        if not order_id:
            continue
        qty = abs(_to_float(event.get("qty"), 0.0))
        price = _to_float(event.get("price"), 0.0)
        if qty <= 0 or price <= 0:
            continue
        fills.append(
            FillRecord(
                order_id=order_id,
                fill_time=now,
                qty=qty,
                price=price,
                fee=_to_float(event.get("fee"), 0.0),
                meta={
                    "side": str(event.get("side", "")).strip().upper(),
                    "gateway_phase": "event",
                    "raw": event,
                },
            )
        )
    return fills


def _build_positions_after_us_gateway(
    *,
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    intent_status: str,
    orders: list[dict[str, Any]],
) -> list[PositionRecord]:
    avg_price_by_symbol: dict[str, float] = {}
    for row in orders:
        symbol = str(row.get("symbol", "")).strip().upper()
        avg_price = _to_float(row.get("avg_price"), 0.0)
        if symbol and avg_price > 0:
            avg_price_by_symbol[symbol] = avg_price

    positions: list[PositionRecord] = []
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        if instrument_type != "US_EQ":
            continue
        target_qty, delta_qty = _extract_qtys(position)
        default_qty = target_qty if ("target_qty" in position or "targetQty" in position) else delta_qty
        qty = default_qty if intent_status == "done" else 0.0
        normalized_symbol = _normalize_us_symbol(symbol)
        avg_price = avg_price_by_symbol.get(normalized_symbol)
        positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol if symbol.startswith("US:") else f"US:{normalized_symbol}",
                instrument_type="US_EQ",
                qty=qty,
                avg_price=avg_price,
                last_price=avg_price,
                market_value=(qty * avg_price) if avg_price is not None else None,
            )
        )
    return positions


def _notify_executor_us_gateway_failure(
    notifier: DiscordNotifier,
    *,
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_name: str | None,
    gateway_status: str,
    reason: str,
) -> None:
    notifier.send_executor_alert(
        title="us gateway execution failure",
        details={
            "intent_id": intent_id,
            "strategy_version_id": strategy_version_id or "-",
            "portfolio": portfolio_name or "-",
            "gateway_status": gateway_status,
            "reason": reason,
        },
    )


def _notify_risk_bulletin(
    notifier: DiscordNotifier,
    *,
    now: datetime,
    category: str,
    title: str,
    intent_id: str,
    strategy_version_id: str | None = None,
    detail: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, object] = {
        "category": category,
        "title": title,
        "intent_id": intent_id,
    }
    if strategy_version_id:
        payload["strategy_version_id"] = strategy_version_id
    if detail:
        payload["detail"] = detail
    if extra:
        payload.update(extra)
    notifier.send_risk_bulletin(now=now, items=[payload], top_n=1)


def _has_partial_fill(legs: list[dict[str, Any]]) -> bool:
    if not legs:
        return False
    filled = 0
    partial = 0
    unfilled = 0
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        status = str(leg.get("status", "")).strip().lower()
        qty = abs(_to_float(leg.get("qty"), 0.0))
        filled_qty = abs(_to_float(leg.get("filled_qty"), 0.0))
        if status in {"partially_filled", "partial_filled"} or (qty > 0 and 0 < filled_qty < qty):
            partial += 1
        elif status in {"filled", "done"} or (qty > 0 and filled_qty >= qty):
            filled += 1
        else:
            unfilled += 1
    return partial > 0 or (filled > 0 and unfilled > 0)


def _build_close_targets_from_positions(
    position_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for row in position_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        qty = _to_float(row.get("qty"), 0.0)
        if abs(qty) < 1e-12:
            continue
        instrument_type = str(row.get("instrument_type", _infer_instrument_type(symbol, None)))
        targets.append(
            {
                "symbol": symbol,
                "instrument_type": instrument_type,
                "target_qty": 0.0,
                "delta_qty": -qty,
                "price_hint": _to_optional_float(row.get("last_price")),
            }
        )
    return targets


def _upsert_zero_positions(
    repo: NeonRepository,
    *,
    portfolio_id: str,
    position_rows: list[dict[str, Any]],
) -> None:
    zero_positions: list[PositionRecord] = []
    for row in position_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        instrument_type = str(row.get("instrument_type", _infer_instrument_type(symbol, None)))
        zero_positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol,
                instrument_type=instrument_type,
                qty=0.0,
                avg_price=_to_optional_float(row.get("avg_price")),
                last_price=_to_optional_float(row.get("last_price")),
                market_value=0.0,
            )
        )
    if zero_positions:
        repo.upsert_positions(zero_positions)


def _execute_strategy_panic_close(
    *,
    repo: NeonRepository,
    now: datetime,
    intent_id: str,
    portfolio_id: str,
    strategy_version_id: str,
    broker_map: dict[str, Any],
    strategy_risk_cfg: dict[str, Any],
    jp_gateway_cfg: dict[str, Any],
    us_gateway_cfg: dict[str, Any],
    jp_gateway_client: JpGatewayClient | None,
    us_gateway_client: USGatewayClient | None,
    crypto_gateway_client: CryptoGatewayClient | None,
) -> dict[str, Any]:
    symbol_rows = repo.fetch_strategy_symbols_for_portfolio(
        strategy_version_id=strategy_version_id,
        portfolio_id=portfolio_id,
        lookback_days=int(strategy_risk_cfg.get("symbol_lookback_days", 30)),
    )
    symbols = [
        str(row.get("symbol", "")).strip()
        for row in symbol_rows
        if isinstance(row, dict) and str(row.get("symbol", "")).strip()
    ]
    if not symbols:
        return {"status": "no_positions", "closed": 0, "failed": 0}

    position_rows = repo.fetch_positions_for_portfolio(
        portfolio_id=portfolio_id,
        symbols=symbols,
    )
    close_targets = _build_close_targets_from_positions(position_rows)
    if not close_targets:
        return {"status": "already_flat", "closed": 0, "failed": 0}

    summary = {
        "status": "done",
        "closed": 0,
        "failed": 0,
        "markets": {},
    }

    jp_targets = [x for x in close_targets if str(x.get("instrument_type")) == "JP_EQ"]
    us_targets = [x for x in close_targets if str(x.get("instrument_type")) == "US_EQ"]
    crypto_targets = [x for x in close_targets if str(x.get("instrument_type")) == "CRYPTO"]

    if jp_targets and jp_gateway_client is not None and str(broker_map.get("JP", "")) == str(jp_gateway_cfg.get("broker_name", "gateway_jp")):
        payload = _build_jp_gateway_payload(
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            portfolio_id=portfolio_id,
            target_positions=jp_targets,
            wait_timeout_sec=_to_float(jp_gateway_cfg.get("wait_timeout_sec"), 2.0),
        )
        try:
            result = jp_gateway_client.execute_intent(payload)
            legs = [x for x in (result.get("legs") or []) if isinstance(x, dict)]
            orders = _build_jp_order_records_from_gateway(intent_id=intent_id, now=now, legs=legs)
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                fills = _build_fill_records_from_gateway(order_ids=order_ids, orders=orders, legs=legs, now=now)
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["JP"] = {"status": result.get("status"), "legs": len(legs)}
            summary["closed"] += len(jp_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["JP"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(jp_targets)

    if us_targets and us_gateway_client is not None and str(broker_map.get("US", "")) == str(us_gateway_cfg.get("broker_name", "gateway_us")):
        payload = _build_us_gateway_payload(
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            portfolio_id=portfolio_id,
            target_positions=us_targets,
            default_order_timeout_sec=_to_float(us_gateway_cfg.get("default_order_timeout_sec"), 20.0),
        )
        try:
            result = us_gateway_client.execute_intent(payload)
            order_rows = [x for x in (result.get("orders") or []) if isinstance(x, dict)]
            fill_events = [x for x in (result.get("fills") or []) if isinstance(x, dict)]
            orders = _build_us_order_records_from_gateway(intent_id=intent_id, now=now, orders=order_rows)
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                order_id_by_broker_order_id = {
                    str(order.broker_order_id): order_id
                    for order_id, order in zip(order_ids, orders, strict=True)
                    if order.broker_order_id
                }
                fills = _build_fill_records_from_gateway_events(
                    order_id_by_broker_order_id=order_id_by_broker_order_id,
                    fill_events=fill_events,
                    now=now,
                )
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["US"] = {"status": result.get("status"), "orders": len(order_rows)}
            summary["closed"] += len(us_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["US"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(us_targets)

    if crypto_targets and crypto_gateway_client is not None and str(broker_map.get("CRYPTO", "")) == "crypto_gateway":
        legs = []
        for idx, target in enumerate(crypto_targets):
            delta_qty = _to_float(target.get("delta_qty"), 0.0)
            legs.append(
                {
                    "leg_id": f"panic-{idx + 1}",
                    "symbol": str(target.get("symbol", "")),
                    "venue": _infer_crypto_venue(str(target.get("symbol", "")), fallback=target.get("venue")),
                    "side": "SELL" if delta_qty > 0 else "BUY",
                    "qty": abs(delta_qty),
                    "price_hint": _to_optional_float(target.get("price_hint")),
                }
            )
        try:
            result = crypto_gateway_client.panic_close({"legs": legs})
            panic_legs = [x for x in (result.get("legs") or []) if isinstance(x, dict)]
            orders = _build_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                legs=panic_legs,
                phase="risk_panic_close",
            )
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                fills = _build_fill_records_from_gateway(order_ids=order_ids, orders=orders, legs=panic_legs, now=now)
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["CRYPTO"] = {"status": result.get("status"), "legs": len(panic_legs)}
            summary["closed"] += len(crypto_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["CRYPTO"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(crypto_targets)

    _upsert_zero_positions(
        repo=repo,
        portfolio_id=portfolio_id,
        position_rows=position_rows,
    )
    if summary["failed"] > 0:
        summary["status"] = "partial_failed"
    return summary


def _infer_crypto_venue(symbol: str, fallback: str | None = None) -> str:
    if fallback:
        value = str(fallback).strip().lower()
        if value:
            return value
    raw = str(symbol).strip().upper()
    if ".BINANCE" in raw or "BINANCE" in raw:
        if ".SPOT." in raw:
            return "binance_spot"
        return "binance_perp"
    if ".HYPER" in raw or "HYPERLIQUID" in raw:
        return "hyperliquid_perp"
    return "binance_perp"


def _build_crypto_gateway_payload(
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    default_leg_timeout_sec: int,
) -> dict[str, Any]:
    legs: list[dict[str, Any]] = []
    max_leg_timeout = max(1, int(default_leg_timeout_sec))
    for idx, position in enumerate(target_positions):
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        target_qty, delta_qty = _extract_qtys(position)
        if abs(delta_qty) < 1e-12:
            continue
        side = "BUY" if delta_qty > 0 else "SELL"
        leg_timeout = int(max(1, _to_float(position.get("timeout_sec"), default_leg_timeout_sec)))
        max_leg_timeout = max(max_leg_timeout, leg_timeout)
        legs.append(
            {
                "leg_id": f"leg-{idx + 1}",
                "symbol": symbol,
                "venue": _infer_crypto_venue(symbol=symbol, fallback=position.get("venue")),
                "side": side,
                "qty": abs(delta_qty),
                "target_qty": target_qty,
                "price_hint": _to_optional_float(position.get("price_hint")),
                "timeout_sec": leg_timeout,
                "cancel_replace": bool(position.get("cancel_replace", False)),
                "open_order_ids": (
                    list(position.get("reconcile_open_order_ids"))
                    if isinstance(position.get("reconcile_open_order_ids"), list)
                    else []
                ),
            }
        )

    return {
        "intent_id": intent_id,
        "strategy_version_id": strategy_version_id,
        "portfolio_id": portfolio_id,
        "idempotency_key": f"intent:{intent_id}",
        "timeout_sec": max_leg_timeout,
        "panic": {
            "close_on_partial_fill": True,
        },
        "legs": legs,
    }


def _map_gateway_status_to_order_status(status: str) -> str:
    value = str(status).strip().lower()
    if value in {"filled", "closed"}:
        return "filled"
    if value in {"rejected"}:
        return "rejected"
    if value == "accepted":
        return "sent"
    if value in {"sent", "ack", "partially_filled"}:
        return value
    return "error"


def _build_order_records_from_gateway(
    *,
    intent_id: str,
    now: datetime,
    legs: list[dict[str, Any]],
    phase: str,
) -> list[OrderRecord]:
    orders: list[OrderRecord] = []
    for idx, leg in enumerate(legs):
        symbol = str(leg.get("symbol", "")).strip()
        side = str(leg.get("side", "BUY")).strip().upper()
        venue = str(leg.get("venue", "crypto_gateway")).strip().lower()
        qty = abs(_to_float(leg.get("qty"), 0.0))
        if not symbol or qty <= 0:
            continue
        order_status = _map_gateway_status_to_order_status(str(leg.get("status", "error")))
        orders.append(
            OrderRecord(
                intent_id=intent_id,
                broker=venue,
                symbol=symbol,
                instrument_type="CRYPTO",
                side=side,
                order_type="MKT",
                qty=qty,
                status=order_status,
                idempotency_key=f"{intent_id}:{phase}:{idx}:{venue}:{symbol}:{side}",
                broker_order_id=(
                    str(leg.get("broker_order_id"))
                    if leg.get("broker_order_id") is not None and str(leg.get("broker_order_id")).strip()
                    else None
                ),
                submitted_at=now,
                meta={
                    "executor": "crypto_gateway",
                    "phase": phase,
                    "reject_reason": leg.get("reject_reason"),
                    "raw": leg,
                },
            )
        )
    return orders


def _build_fill_records_from_gateway(
    *,
    order_ids: list[str],
    orders: list[OrderRecord],
    legs: list[dict[str, Any]],
    now: datetime,
) -> list[FillRecord]:
    fills: list[FillRecord] = []
    for order_id, order, leg in zip(order_ids, orders, legs, strict=True):
        filled_qty = abs(_to_float(leg.get("filled_qty"), 0.0))
        avg_price = _to_float(leg.get("avg_price"), 0.0)
        if filled_qty <= 0 or avg_price <= 0:
            continue
        fills.append(
            FillRecord(
                order_id=order_id,
                fill_time=now,
                qty=filled_qty,
                price=avg_price,
                fee=_to_float(leg.get("fee"), 0.0),
                meta={
                    "side": order.side,
                    "gateway_phase": order.meta.get("phase"),
                },
            )
        )
    return fills


def _build_positions_after_gateway(
    *,
    portfolio_id: str,
    intent_status: str,
    target_positions: list[dict[str, Any]],
    entry_legs: list[dict[str, Any]],
) -> list[PositionRecord]:
    by_symbol_price: dict[str, float] = {}
    for leg in entry_legs:
        symbol = str(leg.get("symbol", "")).strip()
        avg_price = _to_float(leg.get("avg_price"), 0.0)
        if symbol and avg_price > 0:
            by_symbol_price[symbol] = avg_price

    positions: list[PositionRecord] = []
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        target_qty, delta_qty = _extract_qtys(position)
        default_qty = target_qty if ("target_qty" in position or "targetQty" in position) else delta_qty
        qty = default_qty if intent_status == "done" else 0.0
        avg_price = by_symbol_price.get(symbol)
        positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol,
                instrument_type="CRYPTO",
                qty=qty,
                avg_price=avg_price,
                last_price=avg_price,
                market_value=(qty * avg_price) if (avg_price is not None) else None,
            )
        )
    return positions


def _notify_executor_gateway_failure(
    notifier: DiscordNotifier,
    *,
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_name: str | None,
    gateway_status: str,
    reason: str,
) -> None:
    notifier.send_executor_alert(
        title="crypto gateway execution failure",
        details={
            "intent_id": intent_id,
            "strategy_version_id": strategy_version_id or "-",
            "portfolio": portfolio_name or "-",
            "gateway_status": gateway_status,
            "reason": reason,
        },
    )


def run_executor_once(limit: int = 20) -> dict[str, int]:
    cfg = load_yaml_config()
    thresholds = _resolve_thresholds(cfg)
    sharpe_window_days = _resolve_sharpe_window_days(cfg)
    fundamental_overlay = _resolve_fundamental_overlay(cfg)
    data_quality = _resolve_executor_data_quality(cfg)
    reconcile_cfg = _resolve_reconcile_cfg(cfg)
    strategy_risk_cfg = _resolve_strategy_risk_cfg(cfg)
    crypto_gateway_cfg = _resolve_crypto_gateway_cfg(cfg)
    jp_gateway_cfg = _resolve_jp_gateway_cfg(cfg)
    us_gateway_cfg = _resolve_us_gateway_cfg(cfg)
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    notifier = DiscordNotifier(getattr(secrets, "discord_webhook_url", None))
    crypto_gateway_client: CryptoGatewayClient | None = None
    jp_gateway_client: JpGatewayClient | None = None
    us_gateway_client: USGatewayClient | None = None
    if bool(crypto_gateway_cfg.get("enabled")):
        base_url = str(crypto_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            crypto_gateway_client = CryptoGatewayClient(
                base_url=base_url,
                auth_token=crypto_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(crypto_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    if bool(jp_gateway_cfg.get("enabled")):
        base_url = str(jp_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            jp_gateway_client = JpGatewayClient(
                base_url=base_url,
                auth_token=jp_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(jp_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    if bool(us_gateway_cfg.get("enabled")):
        base_url = str(us_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            us_gateway_client = USGatewayClient(
                base_url=base_url,
                auth_token=us_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(us_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    now = datetime.now(timezone.utc)

    intents = repo.fetch_approved_order_intents(limit=limit)
    stats = {
        "fetched": len(intents),
        "processed": 0,
        "done": 0,
        "rejected": 0,
        "failed": 0,
        "sent": 0,
        "skipped_by_fundamental": 0,
        "skipped_by_data_quality": 0,
        "skipped_by_reconcile": 0,
        "strategy_warning": 0,
        "strategy_halt": 0,
        "strategy_cooldown_reject": 0,
        "strategy_panic_close": 0,
        "executed_via_crypto_gateway": 0,
        "executed_via_jp_gateway": 0,
        "executed_via_us_gateway": 0,
    }
    for intent in intents:
        stats["processed"] += 1
        intent_id = str(intent["intent_id"])
        portfolio_id = str(intent["portfolio_id"])
        strategy_version_id = str(intent.get("strategy_version_id") or "").strip() or None
        risk_checks = intent.get("risk_checks") or {}
        if not isinstance(risk_checks, dict):
            risk_checks = {}

        repo.update_order_intent_status(intent_id, "executing")
        risk_state, risk_triggers = _create_risk_snapshot_if_existing(
            repo=repo,
            portfolio_id=portfolio_id,
            thresholds=thresholds,
            now=now,
            fallback_risk_checks=risk_checks,
            sharpe_window_days=sharpe_window_days,
        )
        if risk_state != "normal":
            detail = (
                f"state={risk_state} drawdown={risk_triggers.get('drawdown')} "
                f"sharpe_20d={risk_triggers.get('sharpe_20d')}"
            )
            _notify_risk_bulletin(
                notifier=notifier,
                now=now,
                category="dd_sharpe_gate",
                title="portfolio risk gate reject",
                intent_id=intent_id,
                strategy_version_id=strategy_version_id,
                detail=detail,
            )
            repo.update_order_intent_status(intent_id, "rejected")
            stats["rejected"] += 1
            continue

        broker_map = intent.get("broker_map") or {}
        if not isinstance(broker_map, dict):
            broker_map = {}

        target_positions = intent.get("target_positions") or []
        if not isinstance(target_positions, list):
            target_positions = []

        if bool(strategy_risk_cfg.get("enabled")) and strategy_version_id:
            strategy_drawdown, strategy_sharpe = _extract_strategy_risk_values(risk_checks)
            latest_strategy_snapshot = repo.fetch_latest_strategy_risk_snapshot(strategy_version_id)
            existing_cooldown_until = _to_utc_datetime((latest_strategy_snapshot or {}).get("cooldown_until"))
            recent_strategy_snapshots = repo.fetch_recent_strategy_risk_snapshots(
                strategy_version_id=strategy_version_id,
                limit=max(
                    10,
                    int(strategy_risk_cfg.get("warning_consecutive_days", 3))
                    + int(strategy_risk_cfg.get("halt_consecutive_days", 2))
                    + 10,
                ),
            )
            strategy_thresholds = StrategyRiskThresholds(
                max_drawdown_breach=_to_float(strategy_risk_cfg.get("max_drawdown_breach"), -0.03),
                warning_sharpe_threshold=_to_float(strategy_risk_cfg.get("warning_sharpe_threshold"), 0.30),
                warning_consecutive_days=int(strategy_risk_cfg.get("warning_consecutive_days", 3)),
                halt_sharpe_threshold=_to_float(strategy_risk_cfg.get("halt_sharpe_threshold"), 0.0),
                halt_consecutive_days=int(strategy_risk_cfg.get("halt_consecutive_days", 2)),
                cooldown_hours=int(strategy_risk_cfg.get("cooldown_hours", 24)),
            )
            history_for_eval = [
                {
                    "as_of": now,
                    "as_of_date": now.date(),
                    "sharpe_20d": strategy_sharpe,
                },
                *recent_strategy_snapshots,
            ]
            strategy_state, strategy_triggers, cooldown_until = evaluate_strategy_risk_gate(
                now=now,
                drawdown=strategy_drawdown,
                sharpe_20d=strategy_sharpe,
                history_desc=history_for_eval,
                thresholds=strategy_thresholds,
                existing_cooldown_until=existing_cooldown_until,
            )
            repo.upsert_strategy_risk_snapshot(
                StrategyRiskSnapshot(
                    strategy_version_id=strategy_version_id,
                    as_of=now,
                    drawdown=strategy_drawdown,
                    sharpe_20d=strategy_sharpe,
                    state=strategy_state,
                    trigger_flags=strategy_triggers,
                    cooldown_until=cooldown_until,
                )
            )

            if strategy_state == "warning":
                stats["strategy_warning"] += 1
                repo.insert_strategy_risk_event(
                    StrategyRiskEvent(
                        strategy_version_id=strategy_version_id,
                        event_type="strategy_warning",
                        payload={
                            "intent_id": intent_id,
                            "triggers": strategy_triggers,
                        },
                        triggered_at=now,
                    )
                )
                notifier.send_executor_alert(
                    title="strategy risk warning",
                    details={
                        "intent_id": intent_id,
                        "strategy_version_id": strategy_version_id,
                        "warning_consecutive_count": strategy_triggers.get("warning_consecutive_count"),
                        "sharpe_20d": strategy_sharpe,
                    },
                )
                _notify_risk_bulletin(
                    notifier=notifier,
                    now=now,
                    category="dd_sharpe_gate",
                    title="strategy risk warning",
                    intent_id=intent_id,
                    strategy_version_id=strategy_version_id,
                    detail=(
                        f"warning_count={strategy_triggers.get('warning_consecutive_count')} "
                        f"sharpe_20d={strategy_sharpe}"
                    ),
                )

            if strategy_state in {"halted", "cooldown"}:
                event_type = "strategy_halt" if strategy_state == "halted" else "strategy_cooldown_reject"
                if strategy_state == "halted":
                    stats["strategy_halt"] += 1
                else:
                    stats["strategy_cooldown_reject"] += 1

                panic_summary = None
                should_panic_close = (
                    strategy_state == "halted"
                    and bool(strategy_risk_cfg.get("panic_close_on_halt", True))
                    and bool(
                        strategy_triggers.get("drawdown_breach")
                        or strategy_triggers.get("halt_breach")
                    )
                )
                if should_panic_close:
                    panic_summary = _execute_strategy_panic_close(
                        repo=repo,
                        now=now,
                        intent_id=intent_id,
                        portfolio_id=portfolio_id,
                        strategy_version_id=strategy_version_id,
                        broker_map=broker_map,
                        strategy_risk_cfg=strategy_risk_cfg,
                        jp_gateway_cfg=jp_gateway_cfg,
                        us_gateway_cfg=us_gateway_cfg,
                        jp_gateway_client=jp_gateway_client,
                        us_gateway_client=us_gateway_client,
                        crypto_gateway_client=crypto_gateway_client,
                    )
                    stats["strategy_panic_close"] += 1

                repo.insert_strategy_risk_event(
                    StrategyRiskEvent(
                        strategy_version_id=strategy_version_id,
                        event_type=event_type,
                        payload={
                            "intent_id": intent_id,
                            "state": strategy_state,
                            "triggers": strategy_triggers,
                            "panic_close": panic_summary,
                        },
                        triggered_at=now,
                    )
                )
                notifier.send_executor_alert(
                    title="strategy risk halt" if strategy_state == "halted" else "strategy cooldown reject",
                    details={
                        "intent_id": intent_id,
                        "strategy_version_id": strategy_version_id,
                        "state": strategy_state,
                        "cooldown_until": strategy_triggers.get("cooldown_until"),
                        "panic_close_status": (panic_summary or {}).get("status") if isinstance(panic_summary, dict) else None,
                    },
                )
                _notify_risk_bulletin(
                    notifier=notifier,
                    now=now,
                    category="dd_sharpe_gate",
                    title="strategy risk halt" if strategy_state == "halted" else "strategy cooldown reject",
                    intent_id=intent_id,
                    strategy_version_id=strategy_version_id,
                    detail=(
                        f"state={strategy_state} drawdown={strategy_triggers.get('drawdown')} "
                        f"sharpe_20d={strategy_triggers.get('sharpe_20d')} "
                        f"cooldown_until={strategy_triggers.get('cooldown_until')}"
                    ),
                )
                repo.update_order_intent_status(intent_id, "rejected")
                stats["rejected"] += 1
                continue

        symbols_for_rating: list[str] = []
        for position in target_positions:
            if not isinstance(position, dict):
                continue
            symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
            if symbol:
                symbols_for_rating.append(symbol)
        latest_rating_by_symbol = (
            repo.fetch_latest_fundamental_ratings_by_symbols(symbols_for_rating)
            if bool(fundamental_overlay.get("enabled")) and symbols_for_rating
            else {}
        )

        if bool(reconcile_cfg.get("enabled", True)) and target_positions:
            symbols = _collect_target_symbols(target_positions)
            current_rows = repo.fetch_positions_for_portfolio(
                portfolio_id=portfolio_id,
                symbols=symbols,
            )
            open_rows = repo.fetch_open_orders_for_portfolio(
                portfolio_id=portfolio_id,
                symbols=symbols,
            )
            price_by_symbol = _resolve_price_hints_for_symbols(
                repo=repo,
                target_positions=target_positions,
                symbols=symbols,
            )
            reconcile_result = reconcile_target_positions(
                target_positions=target_positions,
                current_position_qty_by_symbol=_build_position_qty_map(current_rows),
                open_orders_by_symbol=_build_open_order_map(open_rows),
                price_by_symbol=price_by_symbol,
                settings=ReconcileSettings(
                    min_abs_delta_qty=_to_float(reconcile_cfg.get("min_abs_delta_qty"), 0.0),
                    min_abs_delta_notional=_to_float(reconcile_cfg.get("min_abs_delta_notional"), 0.0),
                    open_order_policy=str(reconcile_cfg.get("open_order_policy", "skip")),
                    net_notional_epsilon=_to_float(reconcile_cfg.get("net_notional_epsilon"), 10.0),
                ),
                enforce_net_neutral=_should_enforce_neutrality(
                    risk_checks=risk_checks,
                    target_positions=target_positions,
                    reconcile_cfg=reconcile_cfg,
                ),
            )

            stats["skipped_by_reconcile"] += len(reconcile_result.skipped)
            if reconcile_result.reject_reason:
                repo.update_order_intent_status(intent_id, "rejected")
                stats["rejected"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                if strategy_version_id:
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type="reconcile_rejected",
                            payload={
                                "intent_id": intent_id,
                                "reason": reconcile_result.reject_reason,
                                "net_target_notional": reconcile_result.net_target_notional,
                                "net_delta_notional": reconcile_result.net_delta_notional,
                            },
                            triggered_at=now,
                        )
                    )
                notifier.send_executor_alert(
                    title="reconcile rejected intent",
                    details={
                        "intent_id": intent_id,
                        "reason": reconcile_result.reject_reason,
                        "net_target_notional": reconcile_result.net_target_notional,
                        "net_delta_notional": reconcile_result.net_delta_notional,
                    },
                )
                continue

            target_positions = list(reconcile_result.target_positions)
            if not target_positions:
                has_open_conflict = any(
                    str(row.get("reason", "")).strip().lower() == "open_order_conflict"
                    for row in reconcile_result.skipped
                    if isinstance(row, dict)
                )
                final_status = "sent" if has_open_conflict else "done"
                repo.update_order_intent_status(intent_id, final_status)
                if final_status == "sent":
                    stats["sent"] += 1
                else:
                    stats["done"] += 1
                continue

        contains_crypto_target = False
        contains_jp_target = False
        contains_us_target = False
        has_other_target = False
        for position in target_positions:
            if not isinstance(position, dict):
                continue
            symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
            if not symbol:
                continue
            instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
            if instrument_type == "CRYPTO":
                contains_crypto_target = True
                continue
            if instrument_type == "JP_EQ":
                contains_jp_target = True
                continue
            if instrument_type == "US_EQ":
                contains_us_target = True
                continue
            has_other_target = True

        crypto_broker_name = str(broker_map.get("CRYPTO", "paper"))
        should_use_crypto_gateway = (
            bool(crypto_gateway_cfg.get("enabled"))
            and crypto_gateway_client is not None
            and contains_crypto_target
            and crypto_broker_name == str(crypto_gateway_cfg.get("broker_name", "crypto_gateway"))
        )
        jp_broker_name = str(broker_map.get("JP", "paper"))
        should_use_jp_gateway = (
            bool(jp_gateway_cfg.get("enabled"))
            and jp_gateway_client is not None
            and contains_jp_target
            and not contains_crypto_target
            and not contains_us_target
            and not has_other_target
            and jp_broker_name == str(jp_gateway_cfg.get("broker_name", "gateway_jp"))
        )
        us_broker_name = str(broker_map.get("US", "paper"))
        should_use_us_gateway = (
            bool(us_gateway_cfg.get("enabled"))
            and us_gateway_client is not None
            and contains_us_target
            and not contains_crypto_target
            and not contains_jp_target
            and not has_other_target
            and us_broker_name == str(us_gateway_cfg.get("broker_name", "gateway_us"))
        )
        if should_use_jp_gateway:
            payload = _build_jp_gateway_payload(
                intent_id=intent_id,
                strategy_version_id=intent.get("strategy_version_id"),
                portfolio_id=portfolio_id,
                target_positions=target_positions,
                wait_timeout_sec=_to_float(jp_gateway_cfg.get("wait_timeout_sec"), 2.0),
            )
            if not payload["legs"]:
                repo.update_order_intent_status(intent_id, "done")
                stats["done"] += 1
                continue
            try:
                gateway_result = jp_gateway_client.execute_intent(payload)
            except Exception as exc:  # noqa: BLE001
                repo.update_order_intent_status(intent_id, "failed")
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                if strategy_version_id:
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type="jp_gateway_error",
                            payload={"intent_id": intent_id, "error": str(exc)},
                            triggered_at=now,
                        )
                    )
                _notify_executor_jp_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status="exception",
                    reason=str(exc),
                )
                continue

            gateway_status = str(gateway_result.get("status", "failed")).strip().lower()
            legs = gateway_result.get("legs") if isinstance(gateway_result.get("legs"), list) else []
            active_legs = [
                x
                for x in legs
                if isinstance(x, dict) and str(x.get("status", "")).strip().lower() != "diff_skip"
            ]
            orders = _build_jp_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                legs=active_legs,
            )
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                fills = _build_fill_records_from_gateway(
                    order_ids=order_ids,
                    orders=orders,
                    legs=active_legs,
                    now=now,
                )
                if fills:
                    repo.insert_order_fills(fills)

            if _has_partial_fill(active_legs):
                _notify_risk_bulletin(
                    notifier=notifier,
                    now=now,
                    category="partial_fill",
                    title="jp gateway partial fill",
                    intent_id=intent_id,
                    strategy_version_id=strategy_version_id,
                    detail=f"legs={len(active_legs)} gateway_status={gateway_status}",
                )

            final_intent_status = "failed"
            if gateway_status in {"ack"}:
                final_intent_status = "sent"
            elif gateway_status in {"no_change", "filled"}:
                final_intent_status = "done"

            if final_intent_status == "done":
                stats["done"] += 1
            elif final_intent_status == "sent":
                stats["sent"] += 1
            else:
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                risk_event = gateway_result.get("risk_event")
                if strategy_version_id:
                    event_type = "jp_execution_failed"
                    event_payload: dict[str, Any] = {
                        "intent_id": intent_id,
                        "status": gateway_status,
                    }
                    if isinstance(risk_event, dict):
                        event_type = str(risk_event.get("event_type", event_type))
                        if isinstance(risk_event.get("payload"), dict):
                            event_payload.update(dict(risk_event.get("payload") or {}))
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type=event_type,
                            payload=event_payload,
                            triggered_at=now,
                        )
                    )
                reason = "gateway_status_non_ack"
                if isinstance(risk_event, dict):
                    payload_obj = risk_event.get("payload")
                    if isinstance(payload_obj, dict):
                        reason = str(payload_obj.get("reason", reason))
                _notify_executor_jp_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status=gateway_status,
                    reason=reason,
                )

            positions = _build_positions_after_jp_gateway(
                portfolio_id=portfolio_id,
                target_positions=target_positions,
                intent_status=final_intent_status,
            )
            if positions:
                repo.upsert_positions(positions)

            repo.update_order_intent_status(intent_id, final_intent_status)
            stats["executed_via_jp_gateway"] += 1
            continue

        if should_use_us_gateway:
            payload = _build_us_gateway_payload(
                intent_id=intent_id,
                strategy_version_id=intent.get("strategy_version_id"),
                portfolio_id=portfolio_id,
                target_positions=target_positions,
                default_order_timeout_sec=_to_float(us_gateway_cfg.get("default_order_timeout_sec"), 20.0),
            )
            if not payload["orders"]:
                repo.update_order_intent_status(intent_id, "done")
                stats["done"] += 1
                continue

            try:
                gateway_result = us_gateway_client.execute_intent(payload)
            except Exception as exc:  # noqa: BLE001
                repo.update_order_intent_status(intent_id, "failed")
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                if strategy_version_id:
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type="us_gateway_error",
                            payload={"intent_id": intent_id, "error": str(exc)},
                            triggered_at=now,
                        )
                    )
                _notify_executor_us_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status="exception",
                    reason=str(exc),
                )
                continue

            gateway_status = str(gateway_result.get("status", "failed")).strip().lower()
            gateway_orders = gateway_result.get("orders") if isinstance(gateway_result.get("orders"), list) else []
            gateway_fill_events = gateway_result.get("fills") if isinstance(gateway_result.get("fills"), list) else []

            orders = _build_us_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                orders=[x for x in gateway_orders if isinstance(x, dict)],
            )
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                order_id_by_broker: dict[str, str] = {}
                for order, order_id in zip(orders, order_ids, strict=True):
                    if order.broker_order_id:
                        order_id_by_broker[str(order.broker_order_id)] = order_id

                fills = _build_fill_records_from_gateway_events(
                    order_id_by_broker_order_id=order_id_by_broker,
                    fill_events=[x for x in gateway_fill_events if isinstance(x, dict)],
                    now=now,
                )
                if not fills:
                    fallback_fills: list[FillRecord] = []
                    for order_id, order in zip(order_ids, orders, strict=True):
                        raw = order.meta.get("raw") if isinstance(order.meta, dict) else {}
                        if not isinstance(raw, dict):
                            raw = {}
                        filled_qty = abs(_to_float(raw.get("filled_qty"), 0.0))
                        avg_price = _to_float(raw.get("avg_price"), 0.0)
                        if filled_qty <= 0 or avg_price <= 0:
                            continue
                        fallback_fills.append(
                            FillRecord(
                                order_id=order_id,
                                fill_time=now,
                                qty=filled_qty,
                                price=avg_price,
                                fee=_to_float(raw.get("fee"), 0.0),
                                meta={
                                    "side": order.side,
                                    "gateway_phase": "summary",
                                },
                            )
                        )
                    fills = fallback_fills
                if fills:
                    repo.insert_order_fills(fills)

            normalized_gateway_orders = [x for x in gateway_orders if isinstance(x, dict)]
            if _has_partial_fill(normalized_gateway_orders):
                _notify_risk_bulletin(
                    notifier=notifier,
                    now=now,
                    category="partial_fill",
                    title="us gateway partial fill",
                    intent_id=intent_id,
                    strategy_version_id=strategy_version_id,
                    detail=f"orders={len(normalized_gateway_orders)} gateway_status={gateway_status}",
                )

            final_intent_status = "failed"
            if gateway_status in {"accepted", "ack", "sent"}:
                final_intent_status = "sent"
            elif gateway_status in {"filled", "no_change"}:
                final_intent_status = "done"

            if final_intent_status == "done":
                stats["done"] += 1
            elif final_intent_status == "sent":
                stats["sent"] += 1
            else:
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                risk_event = gateway_result.get("risk_event")
                if strategy_version_id:
                    event_type = "us_execution_failed"
                    event_payload: dict[str, Any] = {
                        "intent_id": intent_id,
                        "status": gateway_status,
                    }
                    if isinstance(risk_event, dict):
                        event_type = str(risk_event.get("event_type", event_type))
                        if isinstance(risk_event.get("payload"), dict):
                            event_payload.update(dict(risk_event.get("payload") or {}))
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type=event_type,
                            payload=event_payload,
                            triggered_at=now,
                        )
                    )
                reason = "gateway_status_non_filled"
                if isinstance(risk_event, dict):
                    payload_obj = risk_event.get("payload")
                    if isinstance(payload_obj, dict):
                        reject_reasons = payload_obj.get("reject_reasons")
                        if isinstance(reject_reasons, list) and reject_reasons:
                            reason = str(reject_reasons[0])
                _notify_executor_us_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status=gateway_status,
                    reason=reason,
                )

            positions = _build_positions_after_us_gateway(
                portfolio_id=portfolio_id,
                target_positions=target_positions,
                intent_status=final_intent_status,
                orders=[x for x in gateway_orders if isinstance(x, dict)],
            )
            if positions:
                repo.upsert_positions(positions)

            repo.update_order_intent_status(intent_id, final_intent_status)
            stats["executed_via_us_gateway"] += 1
            continue

        if should_use_crypto_gateway:
            payload = _build_crypto_gateway_payload(
                intent_id=intent_id,
                strategy_version_id=intent.get("strategy_version_id"),
                portfolio_id=portfolio_id,
                target_positions=target_positions,
                default_leg_timeout_sec=int(crypto_gateway_cfg.get("default_leg_timeout_sec", 30)),
            )
            if not payload["legs"]:
                repo.update_order_intent_status(intent_id, "failed")
                stats["failed"] += 1
                continue
            try:
                gateway_result = crypto_gateway_client.execute_intent(payload)
            except Exception as exc:  # noqa: BLE001
                repo.update_order_intent_status(intent_id, "failed")
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                if strategy_version_id:
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type="crypto_gateway_error",
                            payload={"intent_id": intent_id, "error": str(exc)},
                            triggered_at=now,
                        )
                    )
                _notify_executor_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status="exception",
                    reason=str(exc),
                )
                continue

            gateway_status = str(gateway_result.get("status", "failed")).strip().lower()
            entry_legs = gateway_result.get("legs") if isinstance(gateway_result.get("legs"), list) else []
            panic_section = gateway_result.get("panic_close") if isinstance(gateway_result.get("panic_close"), dict) else {}
            panic_legs = panic_section.get("legs") if isinstance(panic_section.get("legs"), list) else []

            entry_orders = _build_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                legs=[x for x in entry_legs if isinstance(x, dict)],
                phase="entry",
            )
            panic_orders = _build_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                legs=[x for x in panic_legs if isinstance(x, dict)],
                phase="panic_close",
            )
            all_orders = [*entry_orders, *panic_orders]
            if all_orders:
                order_ids = repo.insert_orders_bulk(all_orders)
                entry_count = len(entry_orders)
                entry_ids = order_ids[:entry_count]
                panic_ids = order_ids[entry_count:]
                fills = [
                    *_build_fill_records_from_gateway(
                        order_ids=entry_ids,
                        orders=entry_orders,
                        legs=[x for x in entry_legs if isinstance(x, dict)],
                        now=now,
                    ),
                    *_build_fill_records_from_gateway(
                        order_ids=panic_ids,
                        orders=panic_orders,
                        legs=[x for x in panic_legs if isinstance(x, dict)],
                        now=now,
                    ),
                ]
                if fills:
                    repo.insert_order_fills(fills)

            normalized_entry_legs = [x for x in entry_legs if isinstance(x, dict)]
            if _has_partial_fill(normalized_entry_legs):
                _notify_risk_bulletin(
                    notifier=notifier,
                    now=now,
                    category="partial_fill",
                    title="crypto gateway partial fill",
                    intent_id=intent_id,
                    strategy_version_id=strategy_version_id,
                    detail=f"legs={len(normalized_entry_legs)} gateway_status={gateway_status}",
                )

            final_intent_status = "done" if gateway_status == "filled" else "failed"
            positions = _build_positions_after_gateway(
                portfolio_id=portfolio_id,
                intent_status=final_intent_status,
                target_positions=target_positions,
                entry_legs=normalized_entry_legs,
            )
            if positions:
                repo.upsert_positions(positions)

            if final_intent_status == "done":
                stats["done"] += 1
            else:
                stats["failed"] += 1
                strategy_version_id = intent.get("strategy_version_id")
                risk_event = gateway_result.get("risk_event")
                if strategy_version_id:
                    event_type = "crypto_execution_failed"
                    event_payload: dict[str, Any] = {
                        "intent_id": intent_id,
                        "status": gateway_status,
                    }
                    if isinstance(risk_event, dict):
                        event_type = str(risk_event.get("event_type", event_type))
                        if isinstance(risk_event.get("payload"), dict):
                            event_payload.update(dict(risk_event.get("payload") or {}))
                    repo.insert_strategy_risk_event(
                        StrategyRiskEvent(
                            strategy_version_id=str(strategy_version_id),
                            event_type=event_type,
                            payload=event_payload,
                            triggered_at=now,
                        )
                    )
                reason = "gateway_status_non_filled"
                if isinstance(risk_event, dict):
                    payload_obj = risk_event.get("payload")
                    if isinstance(payload_obj, dict):
                        reason = str(payload_obj.get("panic_reason", reason))
                _notify_executor_gateway_failure(
                    notifier=notifier,
                    intent_id=intent_id,
                    strategy_version_id=intent.get("strategy_version_id"),
                    portfolio_name=intent.get("portfolio_name"),
                    gateway_status=gateway_status,
                    reason=reason,
                )

            repo.update_order_intent_status(intent_id, final_intent_status)
            stats["executed_via_crypto_gateway"] += 1
            continue

        orders: list[OrderRecord] = []
        fills: list[FillRecord] = []
        positions: list[PositionRecord] = []
        fill_price_by_symbol: dict[str, float] = {}
        latest_price_cache: dict[str, dict[str, Any] | None] = {}
        error_count = 0
        filtered_count = 0
        data_quality_filtered_count = 0
        missing_price_filtered_count = 0
        stale_price_filtered_count = 0

        for position in target_positions:
            if not isinstance(position, dict):
                continue
            symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
            if not symbol:
                continue

            target_qty, delta_qty = _extract_qtys(position)
            if abs(delta_qty) < 1e-12:
                continue
            rating = latest_rating_by_symbol.get(symbol)
            if bool(fundamental_overlay.get("enabled")):
                allow_if_missing = bool(fundamental_overlay.get("allow_if_missing", True))
                allow_ratings = set(fundamental_overlay.get("allow_ratings", {"A", "B"}))
                if rating is None and not allow_if_missing:
                    filtered_count += 1
                    stats["skipped_by_fundamental"] += 1
                    continue
                if rating is not None and rating not in allow_ratings:
                    filtered_count += 1
                    stats["skipped_by_fundamental"] += 1
                    continue
                multiplier = float(
                    dict(fundamental_overlay.get("size_multiplier", {})).get(
                        str(rating or "").upper(),
                        1.0,
                    )
                )
                target_qty *= multiplier
                delta_qty *= multiplier
                if abs(delta_qty) < 1e-12:
                    filtered_count += 1
                    stats["skipped_by_fundamental"] += 1
                    continue

            instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
            market_key = _market_key(instrument_type)
            broker = str(broker_map.get(market_key, "paper"))
            side = "BUY"
            if delta_qty < 0:
                side = "SELL_SHORT" if target_qty < 0 else "SELL"

            if symbol in latest_price_cache:
                latest_price_row = latest_price_cache[symbol]
            else:
                latest_price_row = repo.fetch_latest_price_for_symbol(symbol)
                latest_price_cache[symbol] = latest_price_row

            if bool(data_quality.get("enabled", True)):
                reject_on_missing = bool(data_quality.get("reject_on_missing_price", True))
                if latest_price_row is None:
                    if reject_on_missing:
                        data_quality_filtered_count += 1
                        missing_price_filtered_count += 1
                        stats["skipped_by_data_quality"] += 1
                        continue
                elif _price_row_is_stale(
                    latest_price_row=latest_price_row,
                    market_key=market_key,
                    now=now,
                    max_staleness_days=dict(data_quality.get("max_staleness_days", {})),
                ):
                    data_quality_filtered_count += 1
                    stale_price_filtered_count += 1
                    stats["skipped_by_data_quality"] += 1
                    continue

            if not latest_price_row:
                error_count += 1
                continue
            fill_price = _to_float(latest_price_row.get("close_raw"), 0.0)
            if fill_price <= 0:
                error_count += 1
                continue
            fill_price_by_symbol[symbol] = fill_price

            orders.append(
                OrderRecord(
                    intent_id=intent_id,
                    broker=broker,
                    symbol=symbol,
                    instrument_type=instrument_type,
                    side=side,
                    order_type="MKT",
                    qty=abs(delta_qty),
                    status="filled",
                    idempotency_key=f"{intent_id}:{symbol}:{side}:{abs(delta_qty):.8f}",
                    meta={
                        "executor": "paper",
                        "portfolio": intent.get("portfolio_name"),
                        "fundamental_rating": rating,
                    },
                    submitted_at=now,
                )
            )
            position_qty = target_qty if "target_qty" in position or "targetQty" in position else delta_qty
            positions.append(
                PositionRecord(
                    portfolio_id=portfolio_id,
                    symbol=symbol,
                    instrument_type=instrument_type,
                    qty=position_qty,
                    avg_price=fill_price,
                    last_price=fill_price,
                    market_value=position_qty * fill_price,
                )
            )

        if (missing_price_filtered_count + stale_price_filtered_count) > 0:
            _notify_risk_bulletin(
                notifier=notifier,
                now=now,
                category="data_freshness_ng",
                title="price freshness check failed",
                intent_id=intent_id,
                strategy_version_id=strategy_version_id,
                detail=(
                    f"missing={missing_price_filtered_count} stale={stale_price_filtered_count} "
                    f"target_positions={len(target_positions)}"
                ),
            )

        if not orders:
            if (filtered_count + data_quality_filtered_count) > 0 and error_count == 0:
                repo.update_order_intent_status(intent_id, "rejected")
                stats["rejected"] += 1
            else:
                repo.update_order_intent_status(intent_id, "failed")
                stats["failed"] += 1
            continue

        order_ids = repo.insert_orders_bulk(orders)
        for order_id, order in zip(order_ids, orders, strict=True):
            fill_price = fill_price_by_symbol.get(order.symbol, 0.0)
            fills.append(
                FillRecord(
                    order_id=order_id,
                    fill_time=now,
                    qty=order.qty,
                    price=fill_price,
                    fee=0.0,
                    meta={"side": order.side},
                )
            )
        repo.insert_order_fills(fills)
        repo.upsert_positions(positions)

        if error_count > 0:
            repo.update_order_intent_status(intent_id, "failed")
            stats["failed"] += 1
        else:
            repo.update_order_intent_status(intent_id, "done")
            stats["done"] += 1

    return stats


def run_executor(poll_seconds: int = 20, batch_limit: int = 20) -> None:
    while True:
        stats = run_executor_once(limit=batch_limit)
        print(
            "[executor] fetched=%s processed=%s done=%s sent=%s rejected=%s failed=%s reconcile_skipped=%s strat_warn=%s strat_halt=%s strat_cooldown=%s strat_panic=%s crypto_gateway=%s jp_gateway=%s us_gateway=%s"
            % (
                stats["fetched"],
                stats["processed"],
                stats["done"],
                stats.get("sent", 0),
                stats["rejected"],
                stats["failed"],
                stats.get("skipped_by_reconcile", 0),
                stats.get("strategy_warning", 0),
                stats.get("strategy_halt", 0),
                stats.get("strategy_cooldown_reject", 0),
                stats.get("strategy_panic_close", 0),
                stats.get("executed_via_crypto_gateway", 0),
                stats.get("executed_via_jp_gateway", 0),
                stats.get("executed_via_us_gateway", 0),
            ),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))
