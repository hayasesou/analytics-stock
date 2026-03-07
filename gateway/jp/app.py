from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
import hashlib
import json
import os
import sqlite3
import threading
import time
from typing import Any
import uuid

from flask import Flask, jsonify, request
import requests


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
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
    return raw


def _normalize_side(side: str) -> str:
    raw = str(side).strip().upper()
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(raw, raw)


def _normalize_margin_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"cash", "genbutsu"}:
        return "cash"
    if raw in {"margin_open", "shinyo_new", "new"}:
        return "margin_open"
    if raw in {"margin_close", "shinyo_close", "close"}:
        return "margin_close"
    return "cash"


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
                (idempotency_key, intent_id, status, data, _utc_now_iso()),
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
                (intent_id, leg_id, fingerprint, _utc_now_iso()),
            )
            conn.commit()


class RateLimiter:
    def __init__(self, *, global_limit_per_sec: int, per_symbol_limit_per_sec: int) -> None:
        self.global_limit_per_sec = max(1, int(global_limit_per_sec))
        self.per_symbol_limit_per_sec = max(1, int(per_symbol_limit_per_sec))
        self._global_events: deque[float] = deque()
        self._symbol_events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def _prune(self, now_mono: float) -> None:
        threshold = now_mono - 1.0
        while self._global_events and self._global_events[0] < threshold:
            self._global_events.popleft()
        for symbol in list(self._symbol_events):
            events = self._symbol_events[symbol]
            while events and events[0] < threshold:
                events.popleft()
            if not events:
                self._symbol_events.pop(symbol, None)

    def acquire(self, symbol: str, timeout_sec: float, sleep_sec: float = 0.05) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        normalized_symbol = _normalize_symbol(symbol)
        while True:
            with self._lock:
                now_mono = time.monotonic()
                self._prune(now_mono)
                global_ok = len(self._global_events) < self.global_limit_per_sec
                symbol_ok = len(self._symbol_events[normalized_symbol]) < self.per_symbol_limit_per_sec
                if global_ok and symbol_ok:
                    self._global_events.append(now_mono)
                    self._symbol_events[normalized_symbol].append(now_mono)
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(max(0.01, float(sleep_sec)))


class KabuStationAdapter:
    def __init__(
        self,
        *,
        base_url: str,
        api_password: str | None,
        api_token: str | None,
        dry_run: bool,
        timeout_sec: float,
        max_retries: int,
        retry_sleep_sec: float,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.api_password = api_password
        self._api_token = api_token
        self.dry_run = bool(dry_run)
        self.timeout_sec = max(1.0, float(timeout_sec))
        self.max_retries = max(0, int(max_retries))
        self.retry_sleep_sec = max(0.05, float(retry_sleep_sec))
        self.session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        token = self.ensure_token()
        return {"Content-Type": "application/json", "X-API-KEY": token}

    def ensure_token(self) -> str:
        if self.dry_run:
            return "dryrun-token"
        if self._api_token:
            return self._api_token
        if not self.api_password:
            raise RuntimeError("kabu_api_password_missing")
        url = f"{self.base_url}/token"
        response = self.session.post(
            url,
            json={"APIPassword": self.api_password},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("Token"):
            raise RuntimeError("kabu_token_invalid_response")
        self._api_token = str(payload["Token"])
        return self._api_token

    def _to_kabu_order_payload(self, leg: dict[str, Any]) -> dict[str, Any]:
        symbol = _normalize_symbol(str(leg["symbol"]))
        side = _normalize_side(str(leg["side"]))
        qty = abs(_to_int(round(_to_float(leg["qty"], 0.0)), 0))
        order_type = str(leg.get("order_type", "MKT")).strip().upper()
        margin_type = _normalize_margin_type(leg.get("margin_type"))
        price = _to_float(leg.get("limit_price"), 0.0)

        cash_margin_map = {"cash": 1, "margin_open": 2, "margin_close": 3}
        front_order_type = 10 if order_type == "MKT" else 20
        payload: dict[str, Any] = {
            "Symbol": symbol,
            "Exchange": int(_to_int(leg.get("exchange"), 1)),
            "SecurityType": 1,
            "Side": "2" if side == "BUY" else "1",
            "CashMargin": cash_margin_map[margin_type],
            "MarginTradeType": int(_to_int(leg.get("margin_trade_type"), 3)),
            "DelivType": int(_to_int(leg.get("deliv_type"), 2)),
            "AccountType": int(_to_int(leg.get("account_type"), 4)),
            "Qty": qty,
            "FrontOrderType": front_order_type,
            "Price": 0 if order_type == "MKT" else price,
            "ExpireDay": int(_to_int(leg.get("expire_day"), 0)),
        }
        if margin_type == "margin_close":
            close_positions = leg.get("close_positions")
            if isinstance(close_positions, list) and close_positions:
                payload["ClosePositions"] = close_positions
        return payload

    def place_order(self, leg: dict[str, Any]) -> dict[str, Any]:
        kabu_payload = self._to_kabu_order_payload(leg)
        if self.dry_run:
            return {
                "status": "ack",
                "broker_order_id": f"dryrun-kabu-{uuid.uuid4()}",
                "filled_qty": 0.0,
                "avg_price": None,
                "reject_reason": None,
                "meta": {"dry_run": True, "kabu_payload": kabu_payload},
            }

        url = f"{self.base_url}/sendorder"
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(
                    url,
                    headers=self._headers(),
                    json=kabu_payload,
                    timeout=self.timeout_sec,
                )
                payload = response.json()
                if response.ok:
                    if not isinstance(payload, dict):
                        payload = {}
                    order_id = payload.get("OrderId")
                    return {
                        "status": "ack",
                        "broker_order_id": str(order_id) if order_id is not None else None,
                        "filled_qty": 0.0,
                        "avg_price": None,
                        "reject_reason": None,
                        "meta": {"kabu_payload": kabu_payload, "response": payload},
                    }
                reject_code = None
                reject_message = None
                if isinstance(payload, dict):
                    reject_code = payload.get("Code")
                    reject_message = payload.get("Message")
                return {
                    "status": "rejected",
                    "broker_order_id": None,
                    "filled_qty": 0.0,
                    "avg_price": None,
                    "reject_reason": f"kabu_reject:{reject_code}:{reject_message}",
                    "meta": {"kabu_payload": kabu_payload, "response": payload},
                }
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt < self.max_retries:
                    time.sleep(self.retry_sleep_sec)
                    continue
        return {
            "status": "error",
            "broker_order_id": None,
            "filled_qty": 0.0,
            "avg_price": None,
            "reject_reason": f"kabu_request_error:{last_error}",
            "meta": {"kabu_payload": kabu_payload},
        }

    def fetch_order(self, order_id: str) -> dict[str, Any]:
        if self.dry_run:
            return {"status": "ack", "broker_order_id": order_id, "meta": {"dry_run": True}}
        response = self.session.get(
            f"{self.base_url}/orders",
            headers=self._headers(),
            params={"id": str(order_id)},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {"raw": payload}

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        if self.dry_run:
            return {"status": "ack", "broker_order_id": order_id, "meta": {"dry_run": True, "cancel": True}}
        response = self.session.put(
            f"{self.base_url}/cancelorder",
            headers=self._headers(),
            json={"OrderID": str(order_id)},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            payload = {"raw": payload}
        return payload


class ExecutionCoordinator:
    def __init__(
        self,
        *,
        store: IdempotencyStore,
        adapter: KabuStationAdapter,
        limiter: RateLimiter,
        default_wait_timeout_sec: float,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.limiter = limiter
        self.default_wait_timeout_sec = max(0.1, float(default_wait_timeout_sec))

    def _normalize_leg(self, leg: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = str(leg.get("symbol", "")).strip()
        side = _normalize_side(str(leg.get("side", "")).strip().upper())
        qty = abs(_to_float(leg.get("qty"), 0.0))
        if not symbol:
            raise ValueError(f"leg_{idx}_symbol_required")
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"leg_{idx}_side_invalid")
        if qty <= 0:
            raise ValueError(f"leg_{idx}_qty_invalid")
        order_type = str(leg.get("order_type", "MKT")).strip().upper()
        if order_type not in {"MKT", "LMT"}:
            raise ValueError(f"leg_{idx}_order_type_invalid")
        normalized = {
            "leg_id": str(leg.get("leg_id", f"leg-{idx + 1}")),
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_type": order_type,
            "limit_price": _to_float(leg.get("limit_price"), 0.0) if order_type == "LMT" else None,
            "exchange": _to_int(leg.get("exchange"), 1),
            "margin_type": _normalize_margin_type(leg.get("margin_type")),
            "margin_trade_type": _to_int(leg.get("margin_trade_type"), 3),
            "deliv_type": _to_int(leg.get("deliv_type"), 2),
            "account_type": _to_int(leg.get("account_type"), 4),
            "expire_day": _to_int(leg.get("expire_day"), 0),
            "close_positions": leg.get("close_positions") if isinstance(leg.get("close_positions"), list) else None,
            "target_qty": _to_float(leg.get("target_qty"), 0.0),
        }
        return normalized

    def _fingerprint(self, leg: dict[str, Any]) -> str:
        material = {
            "symbol": _normalize_symbol(str(leg["symbol"])),
            "side": leg["side"],
            "qty": round(_to_float(leg["qty"], 0.0), 8),
            "order_type": leg["order_type"],
            "limit_price": round(_to_float(leg.get("limit_price"), 0.0), 8),
            "exchange": leg["exchange"],
            "margin_type": leg["margin_type"],
            "margin_trade_type": leg["margin_trade_type"],
            "deliv_type": leg["deliv_type"],
            "account_type": leg["account_type"],
            "expire_day": leg["expire_day"],
        }
        raw = json.dumps(material, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def execute_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        intent_id = str(payload.get("intent_id", "")).strip()
        if not intent_id:
            raise ValueError("intent_id_required")
        idempotency_key = str(payload.get("idempotency_key", f"intent:{intent_id}")).strip()
        if not idempotency_key:
            raise ValueError("idempotency_key_required")
        replay = self.store.fetch_result(idempotency_key)
        if replay:
            replay = dict(replay)
            replay["idempotency_replay"] = True
            return replay

        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        wait_timeout_sec = _to_float(payload.get("wait_timeout_sec"), self.default_wait_timeout_sec)

        results: list[dict[str, Any]] = []
        for leg in legs:
            fingerprint = self._fingerprint(leg)
            previous = self.store.fetch_leg_fingerprint(intent_id=intent_id, leg_id=leg["leg_id"])
            if previous == fingerprint:
                results.append(
                    {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "diff_skip",
                        "filled_qty": 0.0,
                        "avg_price": None,
                        "broker_order_id": None,
                        "reject_reason": None,
                        "meta": {"fingerprint": fingerprint},
                    }
                )
                continue

            acquired = self.limiter.acquire(symbol=leg["symbol"], timeout_sec=wait_timeout_sec)
            if not acquired:
                results.append(
                    {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "error",
                        "filled_qty": 0.0,
                        "avg_price": None,
                        "broker_order_id": None,
                        "reject_reason": "rate_limit_timeout",
                        "meta": {"fingerprint": fingerprint},
                    }
                )
                continue

            sent = self.adapter.place_order(leg)
            status = str(sent.get("status", "error"))
            result = {
                "leg_id": leg["leg_id"],
                "symbol": leg["symbol"],
                "side": leg["side"],
                "qty": leg["qty"],
                "target_qty": leg["target_qty"],
                "status": status,
                "filled_qty": _to_float(sent.get("filled_qty"), 0.0),
                "avg_price": sent.get("avg_price"),
                "broker_order_id": sent.get("broker_order_id"),
                "reject_reason": sent.get("reject_reason"),
                "meta": sent.get("meta") if isinstance(sent.get("meta"), dict) else {},
            }
            results.append(result)
            if status in {"ack", "filled"}:
                self.store.upsert_leg_fingerprint(intent_id=intent_id, leg_id=leg["leg_id"], fingerprint=fingerprint)

        non_skip = [x for x in results if x["status"] != "diff_skip"]
        if non_skip and all(x["status"] == "ack" for x in non_skip):
            status = "ack"
        elif not non_skip:
            status = "no_change"
        elif any(x["status"] in {"rejected", "error"} for x in non_skip):
            status = "failed"
        else:
            status = "partial"

        risk_event = None
        if status in {"failed", "partial"}:
            risk_event = {
                "event_type": "jp_gateway_execution_failed",
                "payload": {"intent_id": intent_id, "status": status, "legs": results},
            }

        response = {
            "intent_id": intent_id,
            "idempotency_key": idempotency_key,
            "idempotency_replay": False,
            "status": status,
            "executed_at": _utc_now_iso(),
            "legs": results,
            "risk_event": risk_event,
        }
        self.store.save_result(
            idempotency_key=idempotency_key,
            intent_id=intent_id,
            status=status,
            payload=response,
        )
        return response


def create_app() -> Flask:
    auth_token = os.getenv("JP_GATEWAY_AUTH_TOKEN", "").strip()
    store = IdempotencyStore(os.getenv("JP_GATEWAY_STATE_PATH", "/data/jp_gateway.sqlite3"))
    adapter = KabuStationAdapter(
        base_url=os.getenv("KABU_STATION_BASE_URL", "http://host.docker.internal:18080/kabusapi"),
        api_password=os.getenv("KABU_STATION_API_PASSWORD"),
        api_token=os.getenv("KABU_STATION_API_TOKEN"),
        dry_run=_env_flag("JP_GATEWAY_DRY_RUN", True),
        timeout_sec=max(1.0, _to_float(os.getenv("JP_GATEWAY_HTTP_TIMEOUT_SEC"), 5.0)),
        max_retries=max(0, _to_int(os.getenv("JP_GATEWAY_MAX_RETRIES"), 2)),
        retry_sleep_sec=max(0.05, _to_float(os.getenv("JP_GATEWAY_RETRY_SLEEP_SEC"), 0.25)),
    )
    limiter = RateLimiter(
        global_limit_per_sec=max(1, _to_int(os.getenv("JP_GATEWAY_RATE_LIMIT_PER_SEC"), 5)),
        per_symbol_limit_per_sec=max(1, _to_int(os.getenv("JP_GATEWAY_PER_SYMBOL_LIMIT_PER_SEC"), 5)),
    )
    coordinator = ExecutionCoordinator(
        store=store,
        adapter=adapter,
        limiter=limiter,
        default_wait_timeout_sec=max(0.1, _to_float(os.getenv("JP_GATEWAY_WAIT_TIMEOUT_SEC"), 2.0)),
    )

    app = Flask(__name__)

    def _authorized() -> bool:
        if not auth_token:
            return True
        header = request.headers.get("Authorization", "").strip()
        return header == f"Bearer {auth_token}"

    @app.get("/healthz")
    def healthz() -> Any:
        return jsonify({"status": "ok", "time": _utc_now_iso()})

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
        code = 200 if result.get("status") in {"ack", "no_change"} else 202
        return jsonify(result), code

    return app


if __name__ == "__main__":
    host = os.getenv("JP_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("JP_GATEWAY_PORT"), 8081))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
