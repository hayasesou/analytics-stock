from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
from typing import Any
from urllib.parse import urlencode
import uuid

from flask import Flask, jsonify, request
import requests


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_symbol(symbol: str, venue: str) -> str:
    raw = str(symbol).strip()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    base = raw.split(".", 1)[0].strip().upper()
    if not base:
        return base

    venue_norm = str(venue).strip().lower()
    if "hyper" in venue_norm and base.endswith("USDT"):
        return base[:-4]
    return base


def _normalize_side(side: str) -> str:
    raw = str(side).strip().upper()
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(raw, raw)


def _opposite_side(side: str) -> str:
    if str(side).strip().upper() == "BUY":
        return "SELL"
    return "BUY"


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


class BinanceTradeAdapter:
    SPOT_BASE_URL = "https://api.binance.com"
    PERP_BASE_URL = "https://fapi.binance.com"

    def __init__(
        self,
        api_key: str | None,
        api_secret: str | None,
        dry_run: bool = True,
        timeout_sec: float = 5.0,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.dry_run = bool(dry_run)
        self.timeout_sec = max(1.0, float(timeout_sec))
        self.session = session or requests.Session()

    def _signature(self, params: dict[str, Any]) -> str:
        query = urlencode(params, doseq=False)
        digest = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        return digest

    def _simulate(
        self,
        symbol: str,
        side: str,
        qty: float,
        price_hint: float | None,
        reduce_only: bool,
    ) -> dict[str, Any]:
        return {
            "status": "filled",
            "broker_order_id": f"dryrun-binance-{uuid.uuid4()}",
            "filled_qty": qty,
            "avg_price": _to_float(price_hint, 0.0),
            "fee": 0.0,
            "reject_reason": None,
            "meta": {
                "dry_run": True,
                "symbol": symbol,
                "side": side,
                "reduce_only": bool(reduce_only),
            },
        }

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        venue: str,
        reduce_only: bool = False,
        price_hint: float | None = None,
    ) -> dict[str, Any]:
        symbol_norm = _normalize_symbol(symbol, venue)
        side_norm = _normalize_side(side)
        qty_norm = abs(float(qty))
        if qty_norm <= 0:
            return {
                "status": "rejected",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "avg_price": None,
                "fee": 0.0,
                "reject_reason": "invalid_qty",
                "meta": {"symbol": symbol_norm},
            }

        if self.dry_run or not self.api_key or not self.api_secret:
            return self._simulate(
                symbol=symbol_norm,
                side=side_norm,
                qty=qty_norm,
                price_hint=price_hint,
                reduce_only=reduce_only,
            )

        venue_norm = str(venue).strip().lower()
        is_spot = "spot" in venue_norm
        if is_spot:
            base_url = self.SPOT_BASE_URL
            path = "/api/v3/order"
        else:
            base_url = self.PERP_BASE_URL
            path = "/fapi/v1/order"

        params: dict[str, Any] = {
            "symbol": symbol_norm,
            "side": side_norm,
            "type": "MARKET",
            "quantity": f"{qty_norm:.8f}",
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000,
        }
        if reduce_only and not is_spot:
            params["reduceOnly"] = "true"

        params["signature"] = self._signature(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            resp = self.session.post(
                f"{base_url}{path}",
                params=params,
                headers=headers,
                timeout=self.timeout_sec,
            )
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "avg_price": None,
                "fee": 0.0,
                "reject_reason": f"binance_request_error:{exc}",
                "meta": {"venue": venue_norm},
            }

        if not resp.ok:
            return {
                "status": "rejected",
                "broker_order_id": str(payload.get("orderId")) if isinstance(payload, dict) else None,
                "filled_qty": 0.0,
                "avg_price": None,
                "fee": 0.0,
                "reject_reason": f"binance_http_{resp.status_code}",
                "meta": {"payload": payload},
            }

        if not isinstance(payload, dict):
            payload = {}

        filled_qty = _to_float(payload.get("executedQty"), 0.0)
        quote_qty = _to_float(payload.get("cummulativeQuoteQty"), 0.0)
        avg_price = None
        if filled_qty > 0:
            if quote_qty > 0:
                avg_price = quote_qty / filled_qty
            else:
                avg_price = _to_float(payload.get("avgPrice"), _to_float(price_hint, 0.0))
        status = "filled" if filled_qty > 0 else "error"

        return {
            "status": status,
            "broker_order_id": str(payload.get("orderId")) if payload.get("orderId") is not None else None,
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "fee": 0.0,
            "reject_reason": None if status == "filled" else "binance_zero_fill",
            "meta": {"payload": payload, "venue": venue_norm},
        }


class HyperliquidTradeAdapter:
    def __init__(
        self,
        dry_run: bool = True,
        timeout_sec: float = 5.0,
    ) -> None:
        self.dry_run = bool(dry_run)
        self.timeout_sec = max(1.0, float(timeout_sec))

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        venue: str,
        reduce_only: bool = False,
        price_hint: float | None = None,
    ) -> dict[str, Any]:
        symbol_norm = _normalize_symbol(symbol, venue)
        side_norm = _normalize_side(side)
        qty_norm = abs(float(qty))
        if qty_norm <= 0:
            return {
                "status": "rejected",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "avg_price": None,
                "fee": 0.0,
                "reject_reason": "invalid_qty",
                "meta": {"symbol": symbol_norm},
            }

        if self.dry_run:
            return {
                "status": "filled",
                "broker_order_id": f"dryrun-hyperliquid-{uuid.uuid4()}",
                "filled_qty": qty_norm,
                "avg_price": _to_float(price_hint, 0.0),
                "fee": 0.0,
                "reject_reason": None,
                "meta": {
                    "dry_run": True,
                    "symbol": symbol_norm,
                    "side": side_norm,
                    "reduce_only": bool(reduce_only),
                },
            }

        return {
            "status": "error",
            "broker_order_id": None,
            "filled_qty": 0.0,
            "avg_price": None,
            "fee": 0.0,
            "reject_reason": "hyperliquid_live_not_implemented",
            "meta": {
                "symbol": symbol_norm,
                "side": side_norm,
            },
        }


class ExecutionCoordinator:
    def __init__(
        self,
        store: IdempotencyStore,
        *,
        binance_adapter: BinanceTradeAdapter,
        hyperliquid_adapter: HyperliquidTradeAdapter,
    ) -> None:
        self.store = store
        self.binance_adapter = binance_adapter
        self.hyperliquid_adapter = hyperliquid_adapter

    def _resolve_adapter(self, venue: str) -> Any:
        venue_norm = str(venue).strip().lower()
        if "binance" in venue_norm:
            return self.binance_adapter
        if "hyperliquid" in venue_norm or "hyper" in venue_norm:
            return self.hyperliquid_adapter
        raise ValueError(f"unsupported_venue:{venue}")

    def _normalize_leg(self, leg: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = str(leg.get("symbol", "")).strip()
        venue = str(leg.get("venue", "")).strip().lower()
        qty = abs(_to_float(leg.get("qty"), 0.0))
        if not symbol:
            raise ValueError(f"leg_{idx}_symbol_required")
        if not venue:
            raise ValueError(f"leg_{idx}_venue_required")
        if qty <= 0:
            raise ValueError(f"leg_{idx}_qty_invalid")
        side = _normalize_side(str(leg.get("side", "")).strip().upper())
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"leg_{idx}_side_invalid")

        return {
            "leg_id": str(leg.get("leg_id", f"leg-{idx + 1}")),
            "symbol": symbol,
            "venue": venue,
            "side": side,
            "qty": qty,
            "price_hint": _to_float(leg.get("price_hint"), 0.0),
        }

    def _execute_leg(
        self,
        leg: dict[str, Any],
        *,
        reduce_only: bool = False,
        force_side: str | None = None,
    ) -> dict[str, Any]:
        venue = str(leg["venue"])
        side = force_side or str(leg["side"])
        qty = abs(_to_float(leg["qty"], 0.0))
        symbol = str(leg["symbol"])
        adapter = self._resolve_adapter(venue)
        result = adapter.place_market_order(
            symbol=symbol,
            side=side,
            qty=qty,
            venue=venue,
            reduce_only=reduce_only,
            price_hint=_to_float(leg.get("price_hint"), 0.0),
        )
        if not isinstance(result, dict):
            result = {}
        return {
            "leg_id": leg["leg_id"],
            "symbol": symbol,
            "venue": venue,
            "side": side,
            "qty": qty,
            "status": str(result.get("status", "error")),
            "filled_qty": _to_float(result.get("filled_qty"), 0.0),
            "avg_price": _to_float(result.get("avg_price"), 0.0),
            "fee": _to_float(result.get("fee"), 0.0),
            "broker_order_id": result.get("broker_order_id"),
            "reject_reason": result.get("reject_reason"),
            "meta": result.get("meta") if isinstance(result.get("meta"), dict) else {},
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

        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        timeout_sec = max(1.0, _to_float(payload.get("timeout_sec"), 3.0))
        panic_cfg = payload.get("panic") if isinstance(payload.get("panic"), dict) else {}
        close_on_partial_fill = bool((panic_cfg or {}).get("close_on_partial_fill", True))

        leg_results: list[dict[str, Any] | None] = [None for _ in legs]
        future_map: dict[Any, int] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(legs))) as pool:
            for idx, leg in enumerate(legs):
                fut = pool.submit(self._execute_leg, leg)
                future_map[fut] = idx
            done, pending = wait(future_map.keys(), timeout=timeout_sec)
            for fut in done:
                idx = future_map[fut]
                try:
                    leg_results[idx] = fut.result()
                except Exception as exc:  # noqa: BLE001
                    leg = legs[idx]
                    leg_results[idx] = {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "venue": leg["venue"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "error",
                        "filled_qty": 0.0,
                        "avg_price": 0.0,
                        "fee": 0.0,
                        "broker_order_id": None,
                        "reject_reason": f"leg_exec_error:{exc}",
                        "meta": {},
                    }
            for fut in pending:
                idx = future_map[fut]
                fut.cancel()
                leg = legs[idx]
                leg_results[idx] = {
                    "leg_id": leg["leg_id"],
                    "symbol": leg["symbol"],
                    "venue": leg["venue"],
                    "side": leg["side"],
                    "qty": leg["qty"],
                    "status": "error",
                    "filled_qty": 0.0,
                    "avg_price": 0.0,
                    "fee": 0.0,
                    "broker_order_id": None,
                    "reject_reason": "leg_timeout",
                    "meta": {},
                }

        results = [r for r in leg_results if isinstance(r, dict)]
        filled = [r for r in results if r["status"] == "filled" and _to_float(r["filled_qty"], 0.0) > 0.0]
        all_filled = len(filled) == len(legs)

        panic_close_legs: list[dict[str, Any]] = []
        panic_triggered = False
        panic_reason = None
        final_status = "filled"
        if not all_filled:
            final_status = "failed"
            if filled and close_on_partial_fill:
                panic_triggered = True
                panic_reason = "partial_fill_forced_flat"
                close_results = []
                for item in filled:
                    close_leg = {
                        "leg_id": f"{item['leg_id']}:close",
                        "symbol": item["symbol"],
                        "venue": item["venue"],
                        "side": _opposite_side(str(item["side"])),
                        "qty": _to_float(item["filled_qty"], 0.0),
                        "price_hint": _to_float(item.get("avg_price"), 0.0),
                    }
                    close_result = self._execute_leg(close_leg, reduce_only=True, force_side=close_leg["side"])
                    close_result["meta"] = {
                        **dict(close_result.get("meta") or {}),
                        "panic_close": True,
                    }
                    close_results.append(close_result)
                panic_close_legs = close_results
                if all(r["status"] == "filled" for r in close_results):
                    final_status = "partial_closed"
                else:
                    final_status = "failed"

        resulting_positions: list[dict[str, Any]] = []
        if final_status == "filled":
            for item in results:
                signed_qty = _to_float(item["filled_qty"], 0.0)
                if str(item["side"]).upper() == "SELL":
                    signed_qty *= -1.0
                resulting_positions.append(
                    {
                        "symbol": item["symbol"],
                        "venue": item["venue"],
                        "qty": signed_qty,
                        "avg_price": _to_float(item["avg_price"], 0.0),
                    }
                )
        else:
            for leg in legs:
                resulting_positions.append(
                    {
                        "symbol": leg["symbol"],
                        "venue": leg["venue"],
                        "qty": 0.0,
                        "avg_price": 0.0,
                    }
                )

        risk_event = None
        if final_status != "filled":
            risk_event = {
                "event_type": "crypto_partial_fill_forced_flat" if panic_triggered else "crypto_execution_failed",
                "payload": {
                    "intent_id": intent_id,
                    "idempotency_key": idempotency_key,
                    "status": final_status,
                    "panic_reason": panic_reason,
                    "legs": results,
                    "panic_close_legs": panic_close_legs,
                },
            }

        response = {
            "intent_id": intent_id,
            "idempotency_key": idempotency_key,
            "idempotency_replay": False,
            "status": final_status,
            "executed_at": _utc_now_iso(),
            "legs": results,
            "panic_close": {
                "triggered": panic_triggered,
                "reason": panic_reason,
                "legs": panic_close_legs,
            },
            "resulting_positions": resulting_positions,
            "risk_event": risk_event,
        }
        self.store.save(
            idempotency_key=idempotency_key,
            intent_id=intent_id,
            status=final_status,
            payload=response,
        )
        return response

    def panic_close(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        close_results = []
        for leg in legs:
            close_side = _opposite_side(str(leg["side"]))
            close_results.append(
                self._execute_leg(
                    leg,
                    reduce_only=True,
                    force_side=close_side,
                )
            )
        status = "done" if all(x["status"] == "filled" for x in close_results) else "partial"
        return {
            "status": status,
            "closed_at": _utc_now_iso(),
            "legs": close_results,
        }


def create_app() -> Flask:
    auth_token = os.getenv("CRYPTO_GATEWAY_AUTH_TOKEN", "").strip()
    store = IdempotencyStore(os.getenv("CRYPTO_GATEWAY_STATE_PATH", "/data/crypto_gateway.sqlite3"))
    dry_run_default = _env_flag("CRYPTO_GATEWAY_DRY_RUN", True)
    binance_live_enabled = _env_flag("BINANCE_LIVE_ENABLED", False)
    hyper_live_enabled = _env_flag("HYPERLIQUID_LIVE_ENABLED", False)
    gateway_timeout_sec = max(1.0, _to_float(os.getenv("CRYPTO_GATEWAY_HTTP_TIMEOUT_SEC"), 5.0))

    coordinator = ExecutionCoordinator(
        store=store,
        binance_adapter=BinanceTradeAdapter(
            api_key=os.getenv("GATEWAY_BINANCE_API_KEY"),
            api_secret=os.getenv("GATEWAY_BINANCE_API_SECRET"),
            dry_run=(dry_run_default or not binance_live_enabled),
            timeout_sec=gateway_timeout_sec,
        ),
        hyperliquid_adapter=HyperliquidTradeAdapter(
            dry_run=(dry_run_default or not hyper_live_enabled),
            timeout_sec=gateway_timeout_sec,
        ),
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
        code = 200 if result.get("status") == "filled" else 202
        return jsonify(result), code

    @app.post("/v1/panic-close")
    def panic_close() -> Any:
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "invalid_json"}), 400
        try:
            result = coordinator.panic_close(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": f"internal_error:{exc}"}), 500
        return jsonify(result), 200

    return app


if __name__ == "__main__":
    host = os.getenv("CRYPTO_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("CRYPTO_GATEWAY_PORT"), 8080))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
