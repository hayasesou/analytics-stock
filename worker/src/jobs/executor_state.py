from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from typing import Any

from src.execution.risk import RiskThresholds, evaluate_risk_state, rolling_sharpe_annualized
from src.types import RiskSnapshot
from src.jobs.executor_values import *


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


def _resolve_strategy_eval_date(
    *,
    now: datetime,
    risk_checks: dict[str, Any],
    recent_strategy_snapshots: list[dict[str, Any]],
) -> date:
    explicit_date = _to_date(risk_checks.get("as_of_date"))
    if explicit_date is None:
        explicit_date = _to_date(risk_checks.get("as_of"))
    if explicit_date is not None:
        return explicit_date

    latest_snapshot_date = None
    for row in recent_strategy_snapshots:
        if not isinstance(row, dict):
            continue
        latest_snapshot_date = _to_date(row.get("as_of_date", row.get("as_of")))
        if latest_snapshot_date is not None:
            break

    if latest_snapshot_date is None:
        return now.date()
    if (now.date() - latest_snapshot_date).days <= 3:
        return now.date()
    return latest_snapshot_date + timedelta(days=1)


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



__all__ = [
    "_price_row_is_stale",
    "_compute_sharpe_from_history",
    "_collect_target_symbols",
    "_build_position_qty_map",
    "_build_open_order_map",
    "_resolve_price_hints_for_symbols",
    "_should_enforce_neutrality",
    "_extract_strategy_risk_values",
    "_to_utc_datetime",
    "_create_risk_snapshot_if_existing",
    "_resolve_crypto_gateway_cfg",
    "_resolve_jp_gateway_cfg",
    "_resolve_us_gateway_cfg",
]
