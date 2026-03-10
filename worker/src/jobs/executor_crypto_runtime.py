from __future__ import annotations

from datetime import datetime
from typing import Any

from src.jobs.executor_support import (
    _build_fill_records_from_gateway,
    _build_order_records_from_gateway,
    _build_positions_after_gateway,
    _build_crypto_gateway_payload,
    _has_partial_fill,
    _notify_executor_gateway_failure,
    _notify_risk_bulletin,
)
from src.types import StrategyRiskEvent


def process_crypto_gateway_intent(
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
    crypto_gateway_cfg: dict[str, Any],
    crypto_gateway_client,
) -> bool:
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
        return True
    try:
        gateway_result = crypto_gateway_client.execute_intent(payload)
    except Exception as exc:  # noqa: BLE001
        repo.update_order_intent_status(intent_id, "failed")
        stats["failed"] += 1
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
        return True

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
    return True
