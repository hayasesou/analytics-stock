from __future__ import annotations

import os
from typing import Any

from flask import Flask, jsonify, request

from gateway.jp.adapter import KabuStationAdapter
from gateway.jp.common import env_flag, to_float, to_int, utc_now_iso
from gateway.jp.execution import ExecutionCoordinator
from gateway.jp.rate_limit import RateLimiter
from gateway.jp.store import IdempotencyStore


def create_app() -> Flask:
    auth_token = os.getenv("JP_GATEWAY_AUTH_TOKEN", "").strip()
    store = IdempotencyStore(os.getenv("JP_GATEWAY_STATE_PATH", "/data/jp_gateway.sqlite3"))
    adapter = KabuStationAdapter(
        base_url=os.getenv("KABU_STATION_BASE_URL", "http://host.docker.internal:18080/kabusapi"),
        api_password=os.getenv("KABU_STATION_API_PASSWORD"),
        api_token=os.getenv("KABU_STATION_API_TOKEN"),
        dry_run=env_flag("JP_GATEWAY_DRY_RUN", True),
        timeout_sec=max(1.0, to_float(os.getenv("JP_GATEWAY_HTTP_TIMEOUT_SEC"), 5.0)),
        max_retries=max(0, to_int(os.getenv("JP_GATEWAY_MAX_RETRIES"), 2)),
        retry_sleep_sec=max(0.05, to_float(os.getenv("JP_GATEWAY_RETRY_SLEEP_SEC"), 0.25)),
    )
    limiter = RateLimiter(
        global_limit_per_sec=max(1, to_int(os.getenv("JP_GATEWAY_RATE_LIMIT_PER_SEC"), 5)),
        per_symbol_limit_per_sec=max(1, to_int(os.getenv("JP_GATEWAY_PER_SYMBOL_LIMIT_PER_SEC"), 5)),
    )
    coordinator = ExecutionCoordinator(
        store=store,
        adapter=adapter,
        limiter=limiter,
        default_wait_timeout_sec=max(0.1, to_float(os.getenv("JP_GATEWAY_WAIT_TIMEOUT_SEC"), 2.0)),
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
        code = 200 if result.get("status") in {"ack", "no_change"} else 202
        return jsonify(result), code

    return app
