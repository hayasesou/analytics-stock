from __future__ import annotations

import threading
import time
from typing import Any
import uuid

try:
    from ib_insync import LimitOrder, MarketOrder, Stock
except Exception:  # noqa: BLE001
    LimitOrder = None
    MarketOrder = None
    Stock = None

from gateway.us.adapter_live import IbkrTradeAdapterLiveMixin
from gateway.us.common import action_from_side, is_terminal, normalize_side, normalize_symbol, standardize_status, to_float, utc_now_iso


class IbkrTradeAdapter(IbkrTradeAdapterLiveMixin):
    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        account_id: str | None,
        live_enabled: bool,
        dry_run: bool,
        connect_timeout_sec: float = 5.0,
        reconnect_attempts: int = 3,
        reconnect_backoff_sec: float = 1.0,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.account_id = account_id or None
        self.live_enabled = bool(live_enabled)
        self.dry_run = bool(dry_run)
        self.connect_timeout_sec = max(1.0, float(connect_timeout_sec))
        self.reconnect_attempts = max(1, int(reconnect_attempts))
        self.reconnect_backoff_sec = max(0.1, float(reconnect_backoff_sec))

        self._lock = threading.Lock()
        self._ib: Any | None = None
        self._shadow_orders: dict[str, dict[str, Any]] = {}
        self._shadow_fills: list[dict[str, Any]] = []

    def resolve_contract_spec(self, symbol: str) -> dict[str, Any]:
        return {
            "secType": "STK",
            "symbol": normalize_symbol(symbol),
            "exchange": "SMART",
            "currency": "USD",
        }

    def _record_shadow_order(self, order: dict[str, Any]) -> None:
        broker_order_id = str(order.get("broker_order_id", "")).strip()
        if not broker_order_id:
            return
        self._shadow_orders[broker_order_id] = dict(order)

        if order.get("status") == "filled":
            fill_qty = to_float(order.get("filled_qty"), 0.0)
            fill_price = to_float(order.get("avg_price"), 0.0)
            if fill_qty > 0 and fill_price > 0:
                self._shadow_fills.append(
                    {
                        "broker_order_id": broker_order_id,
                        "symbol": order.get("symbol"),
                        "side": order.get("side"),
                        "qty": fill_qty,
                        "price": fill_price,
                        "fee": 0.0,
                        "fill_time": utc_now_iso(),
                        "meta": {"source": "dry_run_shadow"},
                    }
                )

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        order_type: str = "MKT",
        tif: str = "DAY",
        limit_price: float | None = None,
        price_hint: float | None = None,
    ) -> dict[str, Any]:
        symbol_norm = normalize_symbol(symbol)
        side_norm = normalize_side(side)
        qty_norm = abs(float(qty))
        contract = self.resolve_contract_spec(symbol_norm)
        if not symbol_norm:
            return {
                "status": "rejected",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "remaining_qty": qty_norm,
                "avg_price": None,
                "reject_reason": "symbol_required",
                "meta": {},
            }
        if qty_norm <= 0:
            return {
                "status": "rejected",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "remaining_qty": 0.0,
                "avg_price": None,
                "reject_reason": "invalid_qty",
                "meta": {},
            }

        if self.dry_run or not self.live_enabled:
            result = {
                "status": "filled",
                "broker_order_id": f"dryrun-ibkr-{uuid.uuid4()}",
                "filled_qty": qty_norm,
                "remaining_qty": 0.0,
                "avg_price": to_float(price_hint, 0.0),
                "reject_reason": None,
                "meta": {"dry_run": True, "contract": contract},
            }
            self._record_shadow_order(
                {
                    "symbol": symbol_norm,
                    "side": side_norm,
                    "qty": qty_norm,
                    **result,
                }
            )
            return result

        if not self.ensure_connection():
            return {
                "status": "error",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "remaining_qty": qty_norm,
                "avg_price": None,
                "reject_reason": "ibkr_connection_failed",
                "meta": {"contract": contract},
            }
        if Stock is None or MarketOrder is None:
            return {
                "status": "error",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "remaining_qty": qty_norm,
                "avg_price": None,
                "reject_reason": "ib_insync_unavailable",
                "meta": {"contract": contract},
            }
        try:
            live_contract = Stock(symbol_norm, "SMART", "USD")
            qualified = self._ib.qualifyContracts(live_contract)
            if not qualified:
                return {
                    "status": "rejected",
                    "broker_order_id": None,
                    "filled_qty": 0.0,
                    "remaining_qty": qty_norm,
                    "avg_price": None,
                    "reject_reason": "contract_not_found",
                    "meta": {"contract": contract},
                }
            live_contract = qualified[0]

            action = action_from_side(side_norm)
            order_type_norm = str(order_type).strip().upper()
            tif_norm = str(tif).strip().upper() or "DAY"
            if order_type_norm == "LMT" and limit_price is not None and to_float(limit_price, 0.0) > 0 and LimitOrder is not None:
                order = LimitOrder(action=action, totalQuantity=qty_norm, lmtPrice=to_float(limit_price), tif=tif_norm)
            else:
                order = MarketOrder(action=action, totalQuantity=qty_norm, tif=tif_norm)
            if self.account_id:
                order.account = self.account_id

            trade = self._ib.placeOrder(live_contract, order)
            fallback = {
                "broker_order_id": str(getattr(getattr(trade, "order", None), "orderId", "")).strip(),
                "meta": {"contract": contract},
            }
            deadline = time.time() + max(0.5, self.connect_timeout_sec)
            snapshot = self._parse_trade(trade, requested_qty=qty_norm, fallback=fallback)
            while time.time() < deadline:
                self._ib.waitOnUpdate(timeout=0.2)
                snapshot = self._parse_trade(trade, requested_qty=qty_norm, fallback=fallback)
                if is_terminal(snapshot["status"]) or snapshot["status"] == "partially_filled":
                    break

            enriched = {
                "symbol": symbol_norm,
                "side": side_norm,
                "qty": qty_norm,
                **snapshot,
            }
            self._record_shadow_order(enriched)
            return snapshot
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "remaining_qty": qty_norm,
                "avg_price": None,
                "reject_reason": f"ibkr_order_error:{exc}",
                "meta": {"contract": contract},
            }
