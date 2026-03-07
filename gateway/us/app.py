from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
import threading
import time
from typing import Any
import uuid

from flask import Flask, jsonify, request

try:
    from ib_insync import IB, LimitOrder, MarketOrder, Stock
except Exception:  # noqa: BLE001
    IB = None
    LimitOrder = None
    MarketOrder = None
    Stock = None


TERMINAL_STATUSES = {"filled", "rejected", "canceled", "expired", "error"}
PENDING_STATUSES = {"new", "sent", "ack", "partially_filled", "accepted"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw.strip()


def _normalize_side(side: str) -> str:
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(str(side).strip().upper(), str(side).strip().upper())


def _is_terminal(status: str) -> bool:
    return str(status).strip().lower() in TERMINAL_STATUSES


def _standardize_status(raw_status: str, *, filled_qty: float, remaining_qty: float, requested_qty: float) -> str:
    normalized = str(raw_status).strip().lower()
    mapping = {
        "presubmitted": "sent",
        "submitted": "sent",
        "pendingsubmit": "ack",
        "pendingcancel": "ack",
        "apicancelled": "canceled",
        "cancelled": "canceled",
        "inactive": "rejected",
        "filled": "filled",
        "partiallyfilled": "partially_filled",
    }
    value = mapping.get(normalized, normalized)

    if value in {"sent", "ack"} and filled_qty > 0 and remaining_qty > 0:
        return "partially_filled"
    if filled_qty >= max(0.0, requested_qty) and requested_qty > 0:
        return "filled"
    if value in TERMINAL_STATUSES | PENDING_STATUSES:
        return value
    return "error"


def _action_from_side(side: str) -> str:
    return "BUY" if _normalize_side(side) == "BUY" else "SELL"


class IdempotencyStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._initialize()

    def _initialize(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intent_results (
                  idempotency_key TEXT PRIMARY KEY,
                  intent_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  response_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def fetch(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                """
                SELECT response_json
                FROM intent_results
                WHERE idempotency_key = ?
                LIMIT 1
                """,
                (idempotency_key,),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            return payload
        return None

    def save(self, idempotency_key: str, intent_id: str, status: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=True)
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO intent_results (
                  idempotency_key,
                  intent_id,
                  status,
                  response_json,
                  created_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key)
                DO UPDATE SET
                  intent_id = excluded.intent_id,
                  status = excluded.status,
                  response_json = excluded.response_json
                """,
                (idempotency_key, intent_id, status, encoded, _utc_now_iso()),
            )
            conn.commit()


class OrderStateStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._initialize()

    def _initialize(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_state (
                  broker_order_id TEXT PRIMARY KEY,
                  intent_id TEXT,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  qty REAL NOT NULL,
                  filled_qty REAL NOT NULL,
                  remaining_qty REAL NOT NULL,
                  avg_price REAL,
                  status TEXT NOT NULL,
                  reject_reason TEXT,
                  payload_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_order_state_status_updated
                ON order_state (status, updated_at DESC)
                """
            )
            conn.commit()

    def upsert_orders(self, orders: list[dict[str, Any]], *, intent_id: str | None = None) -> int:
        rows: list[tuple[Any, ...]] = []
        for order in orders:
            broker_order_id = str(order.get("broker_order_id", "")).strip()
            symbol = str(order.get("symbol", "")).strip()
            side = _normalize_side(str(order.get("side", "BUY")))
            qty = abs(_to_float(order.get("qty"), 0.0))
            if not broker_order_id or not symbol or qty <= 0:
                continue
            rows.append(
                (
                    broker_order_id,
                    str(order.get("intent_id") or intent_id or "").strip() or None,
                    symbol,
                    side,
                    qty,
                    max(0.0, _to_float(order.get("filled_qty"), 0.0)),
                    max(0.0, _to_float(order.get("remaining_qty"), 0.0)),
                    _to_float(order.get("avg_price"), 0.0),
                    str(order.get("status", "error")).strip().lower(),
                    str(order.get("reject_reason", "")).strip() or None,
                    json.dumps(order, ensure_ascii=True),
                    _utc_now_iso(),
                )
            )

        if not rows:
            return 0
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.executemany(
                """
                INSERT INTO order_state (
                  broker_order_id,
                  intent_id,
                  symbol,
                  side,
                  qty,
                  filled_qty,
                  remaining_qty,
                  avg_price,
                  status,
                  reject_reason,
                  payload_json,
                  updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(broker_order_id)
                DO UPDATE SET
                  intent_id = excluded.intent_id,
                  symbol = excluded.symbol,
                  side = excluded.side,
                  qty = excluded.qty,
                  filled_qty = excluded.filled_qty,
                  remaining_qty = excluded.remaining_qty,
                  avg_price = excluded.avg_price,
                  status = excluded.status,
                  reject_reason = excluded.reject_reason,
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def fetch_pending_order_ids(self, limit: int = 300) -> list[str]:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                """
                SELECT broker_order_id
                FROM order_state
                WHERE status IN ('new', 'sent', 'ack', 'partially_filled', 'accepted')
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            )
            rows = cur.fetchall()
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def fetch_open_orders(self, limit: int = 300) -> list[dict[str, Any]]:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                """
                SELECT payload_json
                FROM order_state
                WHERE status IN ('new', 'sent', 'ack', 'partially_filled', 'accepted')
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            )
            rows = cur.fetchall()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                body = json.loads(str(row[0]))
            except json.JSONDecodeError:
                continue
            if isinstance(body, dict):
                payloads.append(body)
        return payloads


class IbkrTradeAdapter:
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
            "symbol": _normalize_symbol(symbol),
            "exchange": "SMART",
            "currency": "USD",
        }

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
        filled_qty = max(0.0, _to_float(getattr(status_obj, "filled", 0.0), 0.0))
        remaining_qty = max(
            0.0,
            _to_float(getattr(status_obj, "remaining", max(0.0, requested_qty - filled_qty)), 0.0),
        )
        avg_price = _to_float(getattr(status_obj, "avgFillPrice", 0.0), 0.0)
        order_obj = getattr(trade, "order", None)
        broker_order_id = str(getattr(order_obj, "orderId", "")).strip()
        if not broker_order_id:
            broker_order_id = str(getattr(status_obj, "permId", "")).strip()
        if not broker_order_id:
            broker_order_id = str(fallback.get("broker_order_id", "")).strip()
        reject_reason = str(getattr(status_obj, "whyHeld", "")).strip() or None
        status = _standardize_status(
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

    def _record_shadow_order(self, order: dict[str, Any]) -> None:
        broker_order_id = str(order.get("broker_order_id", "")).strip()
        if not broker_order_id:
            return
        self._shadow_orders[broker_order_id] = dict(order)

        if order.get("status") == "filled":
            fill_qty = _to_float(order.get("filled_qty"), 0.0)
            fill_price = _to_float(order.get("avg_price"), 0.0)
            if fill_qty > 0 and fill_price > 0:
                self._shadow_fills.append(
                    {
                        "broker_order_id": broker_order_id,
                        "symbol": order.get("symbol"),
                        "side": order.get("side"),
                        "qty": fill_qty,
                        "price": fill_price,
                        "fee": 0.0,
                        "fill_time": _utc_now_iso(),
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
        symbol_norm = _normalize_symbol(symbol)
        side_norm = _normalize_side(side)
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
                "avg_price": _to_float(price_hint, 0.0),
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

            action = _action_from_side(side_norm)
            order_type_norm = str(order_type).strip().upper()
            tif_norm = str(tif).strip().upper() or "DAY"
            if order_type_norm == "LMT" and limit_price is not None and _to_float(limit_price, 0.0) > 0 and LimitOrder is not None:
                order = LimitOrder(action=action, totalQuantity=qty_norm, lmtPrice=_to_float(limit_price), tif=tif_norm)
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
                if _is_terminal(snapshot["status"]) or snapshot["status"] == "partially_filled":
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
                if not order_id:
                    continue
                if order_id not in requested:
                    continue
                shadow = self._shadow_orders.get(order_id, {})
                qty = abs(_to_float(shadow.get("qty"), _to_float(getattr(order_obj, "totalQuantity", 0.0), 0.0)))
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
            return [dict(v) for v in self._shadow_orders.values() if not _is_terminal(str(v.get("status", "")))]
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
                qty = abs(_to_float(shadow.get("qty"), _to_float(getattr(order_obj, "totalQuantity", 0.0), 0.0)))
                parsed = self._parse_trade(
                    trade,
                    requested_qty=qty,
                    fallback={"broker_order_id": order_id, "meta": shadow.get("meta") if isinstance(shadow.get("meta"), dict) else {}},
                )
                enriched = {**shadow, **parsed}
                self._shadow_orders[order_id] = enriched
                if not _is_terminal(str(enriched.get("status"))):
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
            qty = abs(_to_float(shadow.get("qty"), _to_float(getattr(target_trade.order, "totalQuantity", 0.0), 0.0)))
            parsed = self._parse_trade(
                target_trade,
                requested_qty=qty,
                fallback={"broker_order_id": order_id, "meta": shadow.get("meta") if isinstance(shadow.get("meta"), dict) else {}},
            )
            canceled = {
                **shadow,
                **parsed,
            }
            if not _is_terminal(canceled.get("status", "")):
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
            return [dict(x) for x in self._shadow_fills if str(x.get("broker_order_id", "")).strip() in requested]

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
                        "qty": abs(_to_float(getattr(execution, "shares", 0.0), 0.0)),
                        "price": _to_float(getattr(execution, "price", 0.0), 0.0),
                        "fee": 0.0,
                        "fill_time": _utc_now_iso(),
                        "meta": {"exec_id": str(getattr(execution, "execId", "")).strip()},
                    }
                )
        except Exception:  # noqa: BLE001
            return []
        return rows


class ExecutionCoordinator:
    def __init__(
        self,
        store: IdempotencyStore,
        order_state: OrderStateStore,
        adapter: IbkrTradeAdapter,
    ) -> None:
        self.store = store
        self.order_state = order_state
        self.adapter = adapter

    def _normalize_order(self, order: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = _normalize_symbol(str(order.get("symbol", order.get("security_id", ""))))
        if not symbol:
            raise ValueError(f"order_{idx}_symbol_required")
        qty = abs(_to_float(order.get("qty"), 0.0))
        if qty <= 0:
            raise ValueError(f"order_{idx}_qty_invalid")
        side = _normalize_side(str(order.get("side", "BUY")))
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
            "limit_price": _to_float(order.get("limit_price"), 0.0) if order.get("limit_price") is not None else None,
            "price_hint": _to_float(order.get("price_hint"), 0.0) if order.get("price_hint") is not None else None,
        }

    def _merge_orders(self, base: list[dict[str, Any]], updates: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        for row in base:
            broker_order_id = str(row.get("broker_order_id", "")).strip()
            if broker_order_id and broker_order_id in updates:
                merged.append({**row, **updates[broker_order_id]})
            else:
                merged.append(dict(row))
        return merged

    def _resolve_intent_status(self, orders: list[dict[str, Any]]) -> str:
        if not orders:
            return "failed"
        statuses = [str(x.get("status", "error")).strip().lower() for x in orders]
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
            str(x.get("broker_order_id", "")).strip()
            for x in orders
            if str(x.get("broker_order_id", "")).strip() and not _is_terminal(str(x.get("status", "")))
        }
        current = list(orders)
        while pending and (time.time() - start) < timeout_sec:
            snapshots = self.adapter.fetch_order_statuses(list(pending))
            if snapshots:
                current = self._merge_orders(current, snapshots)
                pending = {
                    str(x.get("broker_order_id", "")).strip()
                    for x in current
                    if str(x.get("broker_order_id", "")).strip() and not _is_terminal(str(x.get("status", "")))
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

        # Best effort recovery for orders that were pending before this intent.
        sync_before = self.resync_orders()

        timeout_sec = max(1.0, _to_float(payload.get("timeout_sec"), 20.0))
        poll_interval_sec = max(0.1, _to_float(payload.get("poll_interval_sec"), 0.5))

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
                    "filled_qty": max(0.0, _to_float(result.get("filled_qty"), 0.0)),
                    "remaining_qty": max(0.0, _to_float(result.get("remaining_qty"), 0.0)),
                    "avg_price": _to_float(result.get("avg_price"), 0.0),
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

        broker_order_ids = [str(x.get("broker_order_id", "")).strip() for x in executed_orders if str(x.get("broker_order_id", "")).strip()]
        fills = self.adapter.fetch_recent_fills(broker_order_ids)
        intent_status = self._resolve_intent_status(executed_orders)
        reject_reasons = [str(x.get("reject_reason", "")).strip() for x in executed_orders if str(x.get("reject_reason", "")).strip()]
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
            "executed_at": _utc_now_iso(),
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


def create_app() -> Flask:
    auth_token = os.getenv("US_GATEWAY_AUTH_TOKEN", "").strip()
    db_path = os.getenv("US_GATEWAY_STATE_PATH", "/data/us_gateway.sqlite3")
    idempotency_store = IdempotencyStore(db_path)
    order_state = OrderStateStore(db_path)

    dry_run_default = _env_flag("US_GATEWAY_DRY_RUN", True)
    ibkr_live_enabled = _env_flag("IBKR_LIVE_ENABLED", False)
    adapter = IbkrTradeAdapter(
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        port=int(_to_float(os.getenv("IBKR_PORT"), 4002)),
        client_id=int(_to_float(os.getenv("IBKR_CLIENT_ID"), 1301)),
        account_id=os.getenv("IBKR_ACCOUNT_ID"),
        live_enabled=ibkr_live_enabled,
        dry_run=(dry_run_default or not ibkr_live_enabled),
        connect_timeout_sec=max(1.0, _to_float(os.getenv("IBKR_CONNECT_TIMEOUT_SEC"), 5.0)),
        reconnect_attempts=int(_to_float(os.getenv("IBKR_RECONNECT_ATTEMPTS"), 3)),
        reconnect_backoff_sec=max(0.1, _to_float(os.getenv("IBKR_RECONNECT_BACKOFF_SEC"), 1.0)),
    )
    coordinator = ExecutionCoordinator(
        store=idempotency_store,
        order_state=order_state,
        adapter=adapter,
    )

    app = Flask(__name__)

    def _authorized() -> bool:
        if not auth_token:
            return True
        header = request.headers.get("Authorization", "").strip()
        return header == f"Bearer {auth_token}"

    @app.get("/healthz")
    def healthz() -> Any:
        open_orders = order_state.fetch_open_orders(limit=10)
        return jsonify(
            {
                "status": "ok",
                "time": _utc_now_iso(),
                "live_enabled": bool(adapter.live_enabled and not adapter.dry_run),
                "dry_run": bool(adapter.dry_run),
                "open_orders": len(open_orders),
            }
        )

    @app.post("/v1/intents/execute")
    def execute_intent() -> Any:
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "invalid_json"}), 400
        try:
            result = coordinator.execute_intent(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": f"internal_error:{exc}"}), 500
        code = 200 if result.get("status") == "filled" else 202
        return jsonify(result), code

    @app.post("/v1/orders/resync")
    def resync_orders() -> Any:
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        try:
            result = coordinator.resync_orders()
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": f"internal_error:{exc}"}), 500
        return jsonify(result), 200

    return app


if __name__ == "__main__":
    host = os.getenv("US_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("US_GATEWAY_PORT"), 8090))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
