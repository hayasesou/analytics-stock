from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_gateway_jp_app_module():
    module_path = Path(__file__).resolve().parents[2] / "gateway" / "jp" / "app.py"
    spec = importlib.util.spec_from_file_location("gateway_jp_app", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load gateway/jp/app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gateway_jp_app = _load_gateway_jp_app_module()


class _FakeKabuAdapter:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def place_order(self, leg: dict) -> dict:
        self.calls.append(dict(leg))
        return {
            "status": "ack",
            "broker_order_id": f"ord-{len(self.calls)}",
            "filled_qty": 0.0,
            "avg_price": None,
            "reject_reason": None,
            "meta": {"dry_run": True},
        }


class _NeverAcquireLimiter:
    def acquire(self, symbol: str, timeout_sec: float, sleep_sec: float = 0.05) -> bool:  # noqa: ARG002
        return False


def test_execute_intent_idempotency_replays_without_duplicate_order(tmp_path) -> None:
    db_path = tmp_path / "jp_gateway.sqlite3"
    store = gateway_jp_app.IdempotencyStore(str(db_path))
    adapter = _FakeKabuAdapter()
    limiter = gateway_jp_app.RateLimiter(global_limit_per_sec=50, per_symbol_limit_per_sec=50)
    coordinator = gateway_jp_app.ExecutionCoordinator(
        store=store,
        adapter=adapter,
        limiter=limiter,
        default_wait_timeout_sec=1.0,
    )
    payload = {
        "intent_id": "intent-1",
        "idempotency_key": "intent-1",
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "JP:7203",
                "side": "BUY",
                "qty": 100,
                "order_type": "MKT",
            }
        ],
    }

    first = coordinator.execute_intent(payload)
    second = coordinator.execute_intent(payload)

    assert first["status"] == "ack"
    assert first["idempotency_replay"] is False
    assert second["status"] == "ack"
    assert second["idempotency_replay"] is True
    assert len(adapter.calls) == 1


def test_execute_intent_diff_skip_on_same_leg_fingerprint(tmp_path) -> None:
    db_path = tmp_path / "jp_gateway.sqlite3"
    store = gateway_jp_app.IdempotencyStore(str(db_path))
    adapter = _FakeKabuAdapter()
    limiter = gateway_jp_app.RateLimiter(global_limit_per_sec=50, per_symbol_limit_per_sec=50)
    coordinator = gateway_jp_app.ExecutionCoordinator(
        store=store,
        adapter=adapter,
        limiter=limiter,
        default_wait_timeout_sec=1.0,
    )

    payload_base = {
        "intent_id": "intent-diff",
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "JP:7203",
                "side": "BUY",
                "qty": 100,
                "order_type": "MKT",
            }
        ],
    }
    first = coordinator.execute_intent({**payload_base, "idempotency_key": "intent-diff:1"})
    second = coordinator.execute_intent({**payload_base, "idempotency_key": "intent-diff:2"})

    assert first["status"] == "ack"
    assert second["status"] == "no_change"
    assert second["legs"][0]["status"] == "diff_skip"
    assert len(adapter.calls) == 1


def test_execute_intent_rate_limit_timeout_marks_failed_without_order(tmp_path) -> None:
    db_path = tmp_path / "jp_gateway.sqlite3"
    store = gateway_jp_app.IdempotencyStore(str(db_path))
    adapter = _FakeKabuAdapter()
    coordinator = gateway_jp_app.ExecutionCoordinator(
        store=store,
        adapter=adapter,
        limiter=_NeverAcquireLimiter(),
        default_wait_timeout_sec=0.1,
    )
    payload = {
        "intent_id": "intent-rate-limit",
        "idempotency_key": "intent-rate-limit",
        "wait_timeout_sec": 0.1,
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "JP:6758",
                "side": "BUY",
                "qty": 100,
                "order_type": "MKT",
            }
        ],
    }

    result = coordinator.execute_intent(payload)

    assert result["status"] == "failed"
    assert result["legs"][0]["status"] == "error"
    assert result["legs"][0]["reject_reason"] == "rate_limit_timeout"
    assert result["risk_event"]["event_type"] == "jp_gateway_execution_failed"
    assert len(adapter.calls) == 0


def test_api_requires_auth_when_token_set(tmp_path, monkeypatch) -> None:
    state_path = str(tmp_path / "jp_gateway.sqlite3")
    monkeypatch.setenv("JP_GATEWAY_AUTH_TOKEN", "secret-token")
    monkeypatch.setenv("JP_GATEWAY_STATE_PATH", state_path)
    monkeypatch.setenv("JP_GATEWAY_DRY_RUN", "1")

    app = gateway_jp_app.create_app()
    client = app.test_client()
    payload = {
        "intent_id": "intent-api",
        "idempotency_key": "intent-api",
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "JP:7203",
                "side": "BUY",
                "qty": 100,
                "order_type": "MKT",
            }
        ],
    }

    unauthorized = client.post("/v1/intents/execute", json=payload)
    authorized = client.post(
        "/v1/intents/execute",
        json=payload,
        headers={"Authorization": "Bearer secret-token"},
    )

    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
    body = authorized.get_json()
    assert isinstance(body, dict)
    assert body["status"] in {"ack", "no_change"}
