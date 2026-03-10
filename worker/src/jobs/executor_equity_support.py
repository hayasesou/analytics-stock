from __future__ import annotations

from datetime import datetime
from typing import Any

from src.types import FillRecord, OrderRecord, PositionRecord
from src.jobs.executor_crypto_support import _map_gateway_status_to_order_status
from src.jobs.executor_values import *


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



__all__ = [
    "_symbol_to_kabu_code",
    "_build_jp_gateway_payload",
    "_build_jp_order_records_from_gateway",
    "_build_positions_after_jp_gateway",
    "_notify_executor_jp_gateway_failure",
    "_normalize_us_symbol",
    "_build_us_gateway_payload",
    "_build_us_order_records_from_gateway",
    "_build_fill_records_from_gateway_events",
    "_build_positions_after_us_gateway",
    "_notify_executor_us_gateway_failure",
]
