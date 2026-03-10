from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from gateway.us.common import normalize_side, to_float, utc_now_iso


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
        return payload if isinstance(payload, dict) else None

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
                (idempotency_key, intent_id, status, encoded, utc_now_iso()),
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
            side = normalize_side(str(order.get("side", "BUY")))
            qty = abs(to_float(order.get("qty"), 0.0))
            if not broker_order_id or not symbol or qty <= 0:
                continue
            rows.append(
                (
                    broker_order_id,
                    str(order.get("intent_id") or intent_id or "").strip() or None,
                    symbol,
                    side,
                    qty,
                    max(0.0, to_float(order.get("filled_qty"), 0.0)),
                    max(0.0, to_float(order.get("remaining_qty"), 0.0)),
                    to_float(order.get("avg_price"), 0.0),
                    str(order.get("status", "error")).strip().lower(),
                    str(order.get("reject_reason", "")).strip() or None,
                    json.dumps(order, ensure_ascii=True),
                    utc_now_iso(),
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
