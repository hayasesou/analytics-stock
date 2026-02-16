from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

from src.config import load_runtime_secrets, load_yaml_config
from src.execution.risk import RiskThresholds, evaluate_risk_state, rolling_sharpe_annualized
from src.storage.db import NeonRepository
from src.types import FillRecord, OrderRecord, PositionRecord, RiskSnapshot


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


def _create_risk_snapshot_if_existing(
    repo: NeonRepository,
    portfolio_id: str,
    thresholds: RiskThresholds,
    now: datetime,
    fallback_risk_checks: dict[str, Any],
    sharpe_window_days: int,
) -> str:
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
    return state


def run_executor_once(limit: int = 20) -> dict[str, int]:
    cfg = load_yaml_config()
    thresholds = _resolve_thresholds(cfg)
    sharpe_window_days = _resolve_sharpe_window_days(cfg)
    fundamental_overlay = _resolve_fundamental_overlay(cfg)
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    now = datetime.now(timezone.utc)

    intents = repo.fetch_approved_order_intents(limit=limit)
    stats = {
        "fetched": len(intents),
        "processed": 0,
        "done": 0,
        "rejected": 0,
        "failed": 0,
        "skipped_by_fundamental": 0,
    }
    for intent in intents:
        stats["processed"] += 1
        intent_id = str(intent["intent_id"])
        portfolio_id = str(intent["portfolio_id"])
        risk_checks = intent.get("risk_checks") or {}
        if not isinstance(risk_checks, dict):
            risk_checks = {}

        repo.update_order_intent_status(intent_id, "executing")
        risk_state = _create_risk_snapshot_if_existing(
            repo=repo,
            portfolio_id=portfolio_id,
            thresholds=thresholds,
            now=now,
            fallback_risk_checks=risk_checks,
            sharpe_window_days=sharpe_window_days,
        )
        if risk_state != "normal":
            repo.update_order_intent_status(intent_id, "rejected")
            stats["rejected"] += 1
            continue

        broker_map = intent.get("broker_map") or {}
        if not isinstance(broker_map, dict):
            broker_map = {}

        target_positions = intent.get("target_positions") or []
        if not isinstance(target_positions, list):
            target_positions = []
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

        orders: list[OrderRecord] = []
        fills: list[FillRecord] = []
        positions: list[PositionRecord] = []
        fill_price_by_symbol: dict[str, float] = {}
        error_count = 0
        filtered_count = 0

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

            latest_price_row = repo.fetch_latest_price_for_symbol(symbol)
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
                    idempotency_key=f"{intent_id}:{symbol}:{int(now.timestamp())}",
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

        if not orders:
            if filtered_count > 0 and error_count == 0:
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
            "[executor] fetched=%s processed=%s done=%s rejected=%s failed=%s"
            % (
                stats["fetched"],
                stats["processed"],
                stats["done"],
                stats["rejected"],
                stats["failed"],
            ),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))
