from __future__ import annotations

from datetime import datetime
from typing import Any

from src.jobs.executor_support import (
    _build_fill_records_from_gateway,
    _build_jp_gateway_payload,
    _build_jp_order_records_from_gateway,
    _build_positions_after_jp_gateway,
    _has_partial_fill,
    _notify_executor_jp_gateway_failure,
    _notify_risk_bulletin,
)
from src.types import StrategyRiskEvent


def process_jp_gateway_intent(
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
    jp_gateway_cfg: dict[str, Any],
    jp_gateway_client,
) -> bool:
    payload = _build_jp_gateway_payload(
        intent_id=intent_id,
        strategy_version_id=intent.get("strategy_version_id"),
        portfolio_id=portfolio_id,
        target_positions=target_positions,
        wait_timeout_sec=float(jp_gateway_cfg.get("wait_timeout_sec", 2.0)),
    )
    if not payload["legs"]:
        repo.update_order_intent_status(intent_id, "done")
        stats["done"] += 1
        return True
    try:
        gateway_result = jp_gateway_client.execute_intent(payload)
    except Exception as exc:  # noqa: BLE001
        repo.update_order_intent_status(intent_id, "failed")
        stats["failed"] += 1
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
        return True

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
    return True
