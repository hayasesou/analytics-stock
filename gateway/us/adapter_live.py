from __future__ import annotations

import time
from typing import Any

from gateway.us.common import is_terminal, standardize_status, to_float, utc_now_iso

try:
    from ib_insync import IB
except Exception:  # noqa: BLE001
    IB = None


class IbkrTradeAdapterLiveMixin:
    def ensure_connection(self) -> bool:
        if self.dry_run or not self.live_enabled:
            return True
        if IB is None:
            return False

        with self._lock:
            if self._ib is not None and bool(self._ib.isConnected()):
                return True
            if self._ib is None:
                self._ib = IB()

        for attempt in range(self.reconnect_attempts):
            try:
                with self._lock:
                    if self._ib is None:
                        self._ib = IB()
                    if not bool(self._ib.isConnected()):
                        self._ib.connect(
                            self.host,
                            self.port,
                            clientId=self.client_id,
                            timeout=self.connect_timeout_sec,
                        )
                    if bool(self._ib.isConnected()):
                        return True
            except Exception:  # noqa: BLE001
                time.sleep(self.reconnect_backoff_sec * (attempt + 1))
        return False

    def _parse_trade(self, trade: Any, *, requested_qty: float, fallback: dict[str, Any]) -> dict[str, Any]:
        status_obj = getattr(trade, "orderStatus", None)
        raw_status = str(getattr(status_obj, "status", "")).strip()
        filled_qty = max(0.0, to_float(getattr(status_obj, "filled", 0.0), 0.0))
        remaining_qty = max(
            0.0,
            to_float(getattr(status_obj, "remaining", max(0.0, requested_qty - filled_qty)), 0.0),
        )
        avg_price = to_float(getattr(status_obj, "avgFillPrice", 0.0), 0.0)
        order_obj = getattr(trade, "order", None)
        broker_order_id = str(getattr(order_obj, "orderId", "")).strip()
        if not broker_order_id:
            broker_order_id = str(getattr(status_obj, "permId", "")).strip()
        if not broker_order_id:
            broker_order_id = str(fallback.get("broker_order_id", "")).strip()
        reject_reason = str(getattr(status_obj, "whyHeld", "")).strip() or None
        status = standardize_status(
            raw_status,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            requested_qty=requested_qty,
        )
        return {
            "broker_order_id": broker_order_id,
            "status": status,
            "filled_qty": filled_qty,
            "remaining_qty": remaining_qty,
            "avg_price": avg_price,
            "reject_reason": reject_reason,
            "meta": {
                **dict(fallback.get("meta") or {}),
                "ibkr_raw_status": raw_status,
            },
        }

    def fetch_order_statuses(self, broker_order_ids: list[str]) -> dict[str, dict[str, Any]]:
        requested = {str(x).strip() for x in broker_order_ids if str(x).strip()}
        if not requested:
            return {}

        if self.dry_run or not self.live_enabled:
            return {order_id: dict(self._shadow_orders[order_id]) for order_id in requested if order_id in self._shadow_orders}

        if not self.ensure_connection():
            return {}

        result: dict[str, dict[str, Any]] = {}
        try:
            trades = self._ib.trades()
            for trade in trades:
                order_obj = getattr(trade, "order", None)
                order_id = str(getattr(order_obj, "orderId", "")).strip()
                if not order_id or order_id not in requested:
                    continue
                shadow = self._shadow_orders.get(order_id, {})
                qty = abs(to_float(shadow.get("qty"), to_float(getattr(order_obj, "totalQuantity", 0.0), 0.0)))
                parsed = self._parse_trade(
                    trade,
                    requested_qty=qty,
                    fallback={
                        "broker_order_id": order_id,
                        "meta": shadow.get("meta") if isinstance(shadow.get("meta"), dict) else {},
                    },
                )
                enriched = {**shadow, **parsed}
                self._shadow_orders[order_id] = enriched
                result[order_id] = enriched
        except Exception:  # noqa: BLE001
            pass

        for order_id in requested:
            if order_id not in result and order_id in self._shadow_orders:
                result[order_id] = dict(self._shadow_orders[order_id])
        return result

    def fetch_open_orders(self) -> list[dict[str, Any]]:
        if self.dry_run or not self.live_enabled:
            return [dict(value) for value in self._shadow_orders.values() if not is_terminal(str(value.get("status", "")))]
        if not self.ensure_connection():
            return []
        rows: list[dict[str, Any]] = []
        try:
            for trade in self._ib.openTrades():
                order_obj = getattr(trade, "order", None)
                order_id = str(getattr(order_obj, "orderId", "")).strip()
                if not order_id:
                    continue
                shadow = self._shadow_orders.get(order_id, {})
                qty = abs(to_float(shadow.get("qty"), to_float(getattr(order_obj, "totalQuantity", 0.0), 0.0)))
                parsed = self._parse_trade(
                    trade,
                    requested_qty=qty,
                    fallback={"broker_order_id": order_id, "meta": shadow.get("meta") if isinstance(shadow.get("meta"), dict) else {}},
                )
                enriched = {**shadow, **parsed}
                self._shadow_orders[order_id] = enriched
                if not is_terminal(str(enriched.get("status"))):
                    rows.append(enriched)
        except Exception:  # noqa: BLE001
            return []
        return rows

    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        order_id = str(broker_order_id).strip()
        shadow = dict(self._shadow_orders.get(order_id, {}))
        if not order_id:
            return {"status": "error", "reject_reason": "broker_order_id_required"}

        if self.dry_run or not self.live_enabled:
            canceled = {
                **shadow,
                "broker_order_id": order_id,
                "status": "canceled",
                "reject_reason": shadow.get("reject_reason") or "timeout_cancel",
            }
            self._shadow_orders[order_id] = canceled
            return canceled

        if not self.ensure_connection():
            return {
                **shadow,
                "broker_order_id": order_id,
                "status": "error",
                "reject_reason": "ibkr_connection_failed",
            }
        try:
            target_trade = None
            for trade in self._ib.trades():
                oid = str(getattr(getattr(trade, "order", None), "orderId", "")).strip()
                if oid == order_id:
                    target_trade = trade
                    break
            if target_trade is None:
                return {
                    **shadow,
                    "broker_order_id": order_id,
                    "status": "error",
                    "reject_reason": "order_not_found",
                }
            self._ib.cancelOrder(target_trade.order)
            self._ib.waitOnUpdate(timeout=0.8)
            qty = abs(to_float(shadow.get("qty"), to_float(getattr(target_trade.order, "totalQuantity", 0.0), 0.0)))
            parsed = self._parse_trade(
                target_trade,
                requested_qty=qty,
                fallback={"broker_order_id": order_id, "meta": shadow.get("meta") if isinstance(shadow.get("meta"), dict) else {}},
            )
            canceled = {
                **shadow,
                **parsed,
            }
            if not is_terminal(canceled.get("status", "")):
                canceled["status"] = "canceled"
                canceled["reject_reason"] = canceled.get("reject_reason") or "timeout_cancel"
            self._shadow_orders[order_id] = canceled
            return canceled
        except Exception as exc:  # noqa: BLE001
            return {
                **shadow,
                "broker_order_id": order_id,
                "status": "error",
                "reject_reason": f"ibkr_cancel_error:{exc}",
            }

    def fetch_recent_fills(self, broker_order_ids: list[str]) -> list[dict[str, Any]]:
        requested = {str(x).strip() for x in broker_order_ids if str(x).strip()}
        if not requested:
            return []

        if self.dry_run or not self.live_enabled:
            return [dict(row) for row in self._shadow_fills if str(row.get("broker_order_id", "")).strip() in requested]

        if not self.ensure_connection():
            return []
        rows: list[dict[str, Any]] = []
        try:
            fills = self._ib.fills()
            for fill in fills:
                execution = getattr(fill, "execution", None)
                contract = getattr(fill, "contract", None)
                order_id = str(getattr(execution, "orderId", "")).strip()
                if order_id not in requested:
                    continue
                side_raw = str(getattr(execution, "side", "")).strip().upper()
                side = "BUY" if side_raw in {"BOT", "BUY"} else "SELL"
                rows.append(
                    {
                        "broker_order_id": order_id,
                        "symbol": str(getattr(contract, "symbol", "")).strip().upper(),
                        "side": side,
                        "qty": abs(to_float(getattr(execution, "shares", 0.0), 0.0)),
                        "price": to_float(getattr(execution, "price", 0.0), 0.0),
                        "fee": 0.0,
                        "fill_time": utc_now_iso(),
                        "meta": {"exec_id": str(getattr(execution, "execId", "")).strip()},
                    }
                )
        except Exception:  # noqa: BLE001
            return []
        return rows
