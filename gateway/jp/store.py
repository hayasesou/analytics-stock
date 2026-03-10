from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from gateway.jp.common import utc_now_iso


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intent_leg_fingerprints (
                  intent_id TEXT NOT NULL,
                  leg_id TEXT NOT NULL,
                  fingerprint TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (intent_id, leg_id)
                )
                """
            )
            conn.commit()

    def fetch_result(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                "SELECT response_json FROM intent_results WHERE idempotency_key = ? LIMIT 1",
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

    def save_result(self, idempotency_key: str, intent_id: str, status: str, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=True)
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO intent_results (
                  idempotency_key,
                  intent_id,
                  status,
                  response_json,
                  created_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key)
                DO UPDATE SET
                  intent_id = excluded.intent_id,
                  status = excluded.status,
                  response_json = excluded.response_json
                """,
                (idempotency_key, intent_id, status, data, utc_now_iso()),
            )
            conn.commit()

    def fetch_leg_fingerprint(self, intent_id: str, leg_id: str) -> str | None:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                """
                SELECT fingerprint
                FROM intent_leg_fingerprints
                WHERE intent_id = ? AND leg_id = ?
                LIMIT 1
                """,
                (intent_id, leg_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        return str(row[0])

    def upsert_leg_fingerprint(self, intent_id: str, leg_id: str, fingerprint: str) -> None:
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO intent_leg_fingerprints (
                  intent_id, leg_id, fingerprint, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(intent_id, leg_id)
                DO UPDATE SET
                  fingerprint = excluded.fingerprint,
                  updated_at = excluded.updated_at
                """,
                (intent_id, leg_id, fingerprint, utc_now_iso()),
            )
            conn.commit()
