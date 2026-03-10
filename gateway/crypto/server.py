from __future__ import annotations

import os
from typing import Any

from flask import Flask, jsonify, request

from gateway.crypto.adapter import BinanceTradeAdapter, HyperliquidTradeAdapter
from gateway.crypto.common import env_flag, to_float, utc_now_iso
from gateway.crypto.execution import ExecutionCoordinator
from gateway.crypto.store import IdempotencyStore


def create_app() -> Flask:
    auth_token = os.getenv("CRYPTO_GATEWAY_AUTH_TOKEN", "").strip()
    store = IdempotencyStore(os.getenv("CRYPTO_GATEWAY_STATE_PATH", "/data/crypto_gateway.sqlite3"))
    dry_run_default = env_flag("CRYPTO_GATEWAY_DRY_RUN", True)
    binance_live_enabled = env_flag("BINANCE_LIVE_ENABLED", False)
    hyper_live_enabled = env_flag("HYPERLIQUID_LIVE_ENABLED", False)
    gateway_timeout_sec = max(1.0, to_float(os.getenv("CRYPTO_GATEWAY_HTTP_TIMEOUT_SEC"), 5.0))

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
        return jsonify({"status": "ok", "time": utc_now_iso()})

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
