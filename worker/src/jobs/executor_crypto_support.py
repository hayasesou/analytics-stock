from __future__ import annotations

from datetime import datetime
from typing import Any

from src.types import FillRecord, OrderRecord, PositionRecord
from src.jobs.executor_values import *


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



__all__ = [
    "_infer_crypto_venue",
    "_build_crypto_gateway_payload",
    "_map_gateway_status_to_order_status",
    "_build_order_records_from_gateway",
    "_build_fill_records_from_gateway",
    "_build_positions_after_gateway",
    "_notify_executor_gateway_failure",
]
