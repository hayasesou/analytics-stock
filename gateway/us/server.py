from __future__ import annotations

import os
from typing import Any

from flask import Flask, jsonify, request

from gateway.us.adapter import IbkrTradeAdapter
from gateway.us.common import env_flag, to_float, utc_now_iso
from gateway.us.execution import ExecutionCoordinator
from gateway.us.store import IdempotencyStore, OrderStateStore


def create_app() -> Flask:
    auth_token = os.getenv("US_GATEWAY_AUTH_TOKEN", "").strip()
    db_path = os.getenv("US_GATEWAY_STATE_PATH", "/data/us_gateway.sqlite3")
    idempotency_store = IdempotencyStore(db_path)
    order_state = OrderStateStore(db_path)

    dry_run_default = env_flag("US_GATEWAY_DRY_RUN", True)
    ibkr_live_enabled = env_flag("IBKR_LIVE_ENABLED", False)
    adapter = IbkrTradeAdapter(
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        port=int(to_float(os.getenv("IBKR_PORT"), 4002)),
        client_id=int(to_float(os.getenv("IBKR_CLIENT_ID"), 1301)),
        account_id=os.getenv("IBKR_ACCOUNT_ID"),
        live_enabled=ibkr_live_enabled,
        dry_run=(dry_run_default or not ibkr_live_enabled),
        connect_timeout_sec=max(1.0, to_float(os.getenv("IBKR_CONNECT_TIMEOUT_SEC"), 5.0)),
        reconnect_attempts=int(to_float(os.getenv("IBKR_RECONNECT_ATTEMPTS"), 3)),
        reconnect_backoff_sec=max(0.1, to_float(os.getenv("IBKR_RECONNECT_BACKOFF_SEC"), 1.0)),
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
                "time": utc_now_iso(),
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
