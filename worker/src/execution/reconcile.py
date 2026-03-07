from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_qtys(position: dict[str, Any]) -> tuple[float, float]:
    target_qty_raw = position.get("target_qty", position.get("targetQty"))
    delta_qty_raw = position.get("delta_qty", position.get("deltaQty"))

    if target_qty_raw is None and delta_qty_raw is None:
        return 0.0, 0.0

    if target_qty_raw is None:
        delta_qty = _to_float(delta_qty_raw, 0.0)
        return delta_qty, delta_qty

    target_qty = _to_float(target_qty_raw, 0.0)
    if delta_qty_raw is None:
        return target_qty, target_qty
    return target_qty, _to_float(delta_qty_raw, target_qty)


def _extract_symbol(position: dict[str, Any]) -> str:
    return str(position.get("symbol", position.get("security_id", ""))).strip()


def _signed_order_qty(side: str, qty: float) -> float:
    normalized = str(side).strip().upper()
    if normalized in {"SELL", "SELL_SHORT"}:
        return -abs(qty)
    return abs(qty)


@dataclass(frozen=True)
class ReconcileSettings:
    min_abs_delta_qty: float = 0.0
    min_abs_delta_notional: float = 0.0
    open_order_policy: str = "skip"  # skip | replace
    net_notional_epsilon: float = 0.0


@dataclass
class ReconcileResult:
    target_positions: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    reject_reason: str | None = None
    net_target_notional: float | None = None
    net_delta_notional: float | None = None
    gross_delta_notional: float | None = None


def reconcile_target_positions(
    *,
    target_positions: list[dict[str, Any]],
    current_position_qty_by_symbol: dict[str, float],
    open_orders_by_symbol: dict[str, list[dict[str, Any]]],
    price_by_symbol: dict[str, float],
    settings: ReconcileSettings,
    enforce_net_neutral: bool = False,
) -> ReconcileResult:
    normalized_policy = str(settings.open_order_policy).strip().lower()
    if normalized_policy not in {"skip", "replace"}:
        normalized_policy = "skip"

    planned: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    net_target_notional = 0.0
    net_delta_notional = 0.0
    gross_delta_notional = 0.0
    target_notional_count = 0
    delta_notional_count = 0

    for raw_position in target_positions:
        if not isinstance(raw_position, dict):
            continue

        symbol = _extract_symbol(raw_position)
        if not symbol:
            continue

        target_qty_raw, delta_qty_raw = _extract_qtys(raw_position)
        current_qty = _to_float(current_position_qty_by_symbol.get(symbol), 0.0)
        symbol_open_orders = open_orders_by_symbol.get(symbol, [])
        open_order_ids: list[str] = []
        open_order_net_qty = 0.0
        for row in symbol_open_orders:
            if not isinstance(row, dict):
                continue
            order_id = str(row.get("order_id", "")).strip()
            if order_id:
                open_order_ids.append(order_id)
            open_order_net_qty += _signed_order_qty(
                side=str(row.get("side", "")),
                qty=_to_float(row.get("qty"), 0.0),
            )

        if "target_qty" in raw_position or "targetQty" in raw_position:
            desired_target_qty = target_qty_raw
        else:
            # delta-only intent means "change from current now".
            desired_target_qty = current_qty + delta_qty_raw

        if symbol_open_orders and normalized_policy == "skip":
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": "open_order_conflict",
                    "current_qty": current_qty,
                    "open_order_qty": open_order_net_qty,
                    "target_qty": desired_target_qty,
                }
            )
            continue

        effective_qty = current_qty
        cancel_replace = False
        if symbol_open_orders:
            if normalized_policy == "replace":
                cancel_replace = True
            else:
                effective_qty += open_order_net_qty

        delta_qty = desired_target_qty - effective_qty
        price = _to_float(
            raw_position.get("price_hint"),
            _to_float(price_by_symbol.get(symbol), 0.0),
        )

        delta_notional_abs = abs(delta_qty * price) if price > 0 else None
        skip_reason = None
        qty_threshold = max(0.0, float(settings.min_abs_delta_qty))
        if abs(delta_qty) <= max(1e-12, qty_threshold):
            skip_reason = "delta_below_qty_threshold"
        elif (
            delta_notional_abs is not None
            and delta_notional_abs < max(0.0, float(settings.min_abs_delta_notional))
        ):
            skip_reason = "delta_below_notional_threshold"

        if skip_reason is not None:
            skipped.append(
                {
                    "symbol": symbol,
                    "reason": skip_reason,
                    "current_qty": current_qty,
                    "open_order_qty": open_order_net_qty,
                    "target_qty": desired_target_qty,
                    "delta_qty": delta_qty,
                }
            )
            continue

        if price > 0:
            target_notional_count += 1
            delta_notional_count += 1
            net_target_notional += desired_target_qty * price
            net_delta_notional += delta_qty * price
            gross_delta_notional += abs(delta_qty * price)

        planned_position = dict(raw_position)
        planned_position["symbol"] = symbol
        planned_position["target_qty"] = desired_target_qty
        planned_position["delta_qty"] = delta_qty
        planned_position["reconcile_current_qty"] = current_qty
        planned_position["reconcile_open_order_qty"] = open_order_net_qty
        planned_position["reconcile_policy"] = normalized_policy
        planned_position["reconcile_open_order_ids"] = open_order_ids
        planned_position["cancel_replace"] = cancel_replace
        if price > 0:
            planned_position.setdefault("price_hint", price)
        planned.append(planned_position)

    if enforce_net_neutral and len(planned) >= 2:
        if target_notional_count > 0:
            epsilon = max(0.0, float(settings.net_notional_epsilon))
            if epsilon >= 0 and abs(net_target_notional) > epsilon:
                return ReconcileResult(
                    target_positions=[],
                    skipped=skipped,
                    reject_reason="net_notional_violation",
                    net_target_notional=net_target_notional,
                    net_delta_notional=(net_delta_notional if delta_notional_count > 0 else None),
                    gross_delta_notional=(gross_delta_notional if delta_notional_count > 0 else None),
                )

    return ReconcileResult(
        target_positions=planned,
        skipped=skipped,
        reject_reason=None,
        net_target_notional=(net_target_notional if target_notional_count > 0 else None),
        net_delta_notional=(net_delta_notional if delta_notional_count > 0 else None),
        gross_delta_notional=(gross_delta_notional if delta_notional_count > 0 else None),
    )
