from __future__ import annotations

from datetime import datetime
from typing import Any

from src.jobs.executor_support import (
    _build_fill_records_from_gateway_events,
    _build_positions_after_us_gateway,
    _build_us_gateway_payload,
    _build_us_order_records_from_gateway,
    _has_partial_fill,
    _notify_executor_us_gateway_failure,
    _notify_risk_bulletin,
    _to_float,
)
from src.types import FillRecord, StrategyRiskEvent


def process_us_gateway_intent(
    *,
    repo,
    notifier,
    now: datetime,
    intent: dict[str, Any],
    stats: dict[str, int],
    intent_id: str,
    portfolio_id: str,
    strategy_version_id: str | None,
    target_positions: list[dict[str, Any]],
    us_gateway_cfg: dict[str, Any],
    us_gateway_client,
) -> bool:
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
        return True

    try:
        gateway_result = us_gateway_client.execute_intent(payload)
    except Exception as exc:  # noqa: BLE001
        repo.update_order_intent_status(intent_id, "failed")
        stats["failed"] += 1
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
        return True

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
    return True
