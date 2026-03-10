from __future__ import annotations

import time
from typing import Any

from gateway.us.common import is_terminal, normalize_side, normalize_symbol, to_float, utc_now_iso


class ExecutionCoordinator:
    def __init__(
        self,
        store,
        order_state,
        adapter,
    ) -> None:
        self.store = store
        self.order_state = order_state
        self.adapter = adapter

    def _normalize_order(self, order: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = normalize_symbol(str(order.get("symbol", order.get("security_id", ""))))
        if not symbol:
            raise ValueError(f"order_{idx}_symbol_required")
        qty = abs(to_float(order.get("qty"), 0.0))
        if qty <= 0:
            raise ValueError(f"order_{idx}_qty_invalid")
        side = normalize_side(str(order.get("side", "BUY")))
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"order_{idx}_side_invalid")
        order_type = str(order.get("order_type", order.get("type", "MKT"))).strip().upper()
        if order_type not in {"MKT", "LMT"}:
            raise ValueError(f"order_{idx}_order_type_invalid")
        tif = str(order.get("time_in_force", order.get("tif", "DAY"))).strip().upper() or "DAY"
        return {
            "order_id": str(order.get("order_id", f"ord-{idx + 1}")),
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_type": order_type,
            "time_in_force": tif,
            "limit_price": to_float(order.get("limit_price"), 0.0) if order.get("limit_price") is not None else None,
            "price_hint": to_float(order.get("price_hint"), 0.0) if order.get("price_hint") is not None else None,
        }

    def _merge_orders(self, base: list[dict[str, Any]], updates: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        for row in base:
            broker_order_id = str(row.get("broker_order_id", "")).strip()
            merged.append({**row, **updates[broker_order_id]}) if broker_order_id and broker_order_id in updates else merged.append(dict(row))
        return merged

    def _resolve_intent_status(self, orders: list[dict[str, Any]]) -> str:
        if not orders:
            return "failed"
        statuses = [str(row.get("status", "error")).strip().lower() for row in orders]
        if all(status == "filled" for status in statuses):
            return "filled"
        if any(status in {"rejected", "error", "canceled", "expired"} for status in statuses):
            return "failed"
        if any(status in {"new", "sent", "ack", "accepted"} for status in statuses):
            return "accepted"
        if any(status == "partially_filled" for status in statuses):
            return "partial"
        return "failed"

    def _poll_and_cancel_on_timeout(
        self,
        orders: list[dict[str, Any]],
        *,
        timeout_sec: float,
        poll_interval_sec: float,
    ) -> tuple[list[dict[str, Any]], int]:
        start = time.time()
        pending = {
            str(row.get("broker_order_id", "")).strip()
            for row in orders
            if str(row.get("broker_order_id", "")).strip() and not is_terminal(str(row.get("status", "")))
        }
        current = list(orders)
        while pending and (time.time() - start) < timeout_sec:
            snapshots = self.adapter.fetch_order_statuses(list(pending))
            if snapshots:
                current = self._merge_orders(current, snapshots)
                pending = {
                    str(row.get("broker_order_id", "")).strip()
                    for row in current
                    if str(row.get("broker_order_id", "")).strip() and not is_terminal(str(row.get("status", "")))
                }
            if pending:
                time.sleep(max(0.1, poll_interval_sec))

        canceled_count = 0
        if pending:
            updates: dict[str, dict[str, Any]] = {}
            for broker_order_id in pending:
                canceled = self.adapter.cancel_order(broker_order_id)
                if isinstance(canceled, dict):
                    updates[broker_order_id] = canceled
                    canceled_count += 1
            if updates:
                current = self._merge_orders(current, updates)
        return current, canceled_count

    def resync_orders(self) -> dict[str, Any]:
        pending_ids = self.order_state.fetch_pending_order_ids(limit=500)
        snapshots = self.adapter.fetch_order_statuses(pending_ids) if pending_ids else {}
        resynced_orders = list(snapshots.values()) if snapshots else []
        open_orders = self.adapter.fetch_open_orders()
        persisted = 0
        if resynced_orders:
            persisted += self.order_state.upsert_orders(resynced_orders)
        if open_orders:
            persisted += self.order_state.upsert_orders(open_orders)
        return {
            "pending_before": len(pending_ids),
            "resynced": len(resynced_orders),
            "open_orders": len(open_orders),
            "persisted": persisted,
            "orders": self.order_state.fetch_open_orders(limit=500),
        }

    def execute_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        intent_id = str(payload.get("intent_id", "")).strip()
        if not intent_id:
            raise ValueError("intent_id_required")
        idempotency_key = str(payload.get("idempotency_key", f"intent:{intent_id}")).strip()
        if not idempotency_key:
            raise ValueError("idempotency_key_required")

        replay = self.store.fetch(idempotency_key)
        if replay:
            replay = dict(replay)
            replay["idempotency_replay"] = True
            return replay

        raw_orders = payload.get("orders")
        if not isinstance(raw_orders, list) or not raw_orders:
            raise ValueError("orders_required")
        orders = [self._normalize_order(order, idx) for idx, order in enumerate(raw_orders)]

        sync_before = self.resync_orders()
        timeout_sec = max(1.0, to_float(payload.get("timeout_sec"), 20.0))
        poll_interval_sec = max(0.1, to_float(payload.get("poll_interval_sec"), 0.5))

        executed_orders: list[dict[str, Any]] = []
        for order in orders:
            result = self.adapter.place_order(
                symbol=order["symbol"],
                side=order["side"],
                qty=order["qty"],
                order_type=order["order_type"],
                tif=order["time_in_force"],
                limit_price=order["limit_price"],
                price_hint=order["price_hint"],
            )
            if not isinstance(result, dict):
                result = {}
            executed_orders.append(
                {
                    "order_id": order["order_id"],
                    "symbol": order["symbol"],
                    "side": order["side"],
                    "qty": order["qty"],
                    "order_type": order["order_type"],
                    "time_in_force": order["time_in_force"],
                    "status": str(result.get("status", "error")).strip().lower(),
                    "filled_qty": max(0.0, to_float(result.get("filled_qty"), 0.0)),
                    "remaining_qty": max(0.0, to_float(result.get("remaining_qty"), 0.0)),
                    "avg_price": to_float(result.get("avg_price"), 0.0),
                    "broker_order_id": result.get("broker_order_id"),
                    "reject_reason": result.get("reject_reason"),
                    "contract": (
                        result.get("meta", {}).get("contract")
                        if isinstance(result.get("meta"), dict)
                        else self.adapter.resolve_contract_spec(order["symbol"])
                    ),
                    "meta": dict(result.get("meta") or {}) if isinstance(result.get("meta"), dict) else {},
                }
            )

        self.order_state.upsert_orders(executed_orders, intent_id=intent_id)
        executed_orders, canceled_on_timeout = self._poll_and_cancel_on_timeout(
            executed_orders,
            timeout_sec=timeout_sec,
            poll_interval_sec=poll_interval_sec,
        )
        self.order_state.upsert_orders(executed_orders, intent_id=intent_id)

        broker_order_ids = [str(row.get("broker_order_id", "")).strip() for row in executed_orders if str(row.get("broker_order_id", "")).strip()]
        fills = self.adapter.fetch_recent_fills(broker_order_ids)
        intent_status = self._resolve_intent_status(executed_orders)
        reject_reasons = [str(row.get("reject_reason", "")).strip() for row in executed_orders if str(row.get("reject_reason", "")).strip()]
        risk_event = None
        if intent_status != "filled":
            risk_event = {
                "event_type": "us_execution_failed",
                "payload": {
                    "intent_id": intent_id,
                    "status": intent_status,
                    "reject_reasons": reject_reasons,
                    "orders": executed_orders,
                },
            }

        response = {
            "intent_id": intent_id,
            "idempotency_key": idempotency_key,
            "idempotency_replay": False,
            "status": intent_status,
            "executed_at": utc_now_iso(),
            "orders": executed_orders,
            "fills": fills,
            "sync": {
                "before_execute": sync_before,
                "canceled_on_timeout": canceled_on_timeout,
            },
            "risk_event": risk_event,
        }
        self.store.save(idempotency_key=idempotency_key, intent_id=intent_id, status=intent_status, payload=response)
        return response
