from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_gateway_crypto_app_module():
    module_path = Path(__file__).resolve().parents[2] / "gateway" / "crypto" / "app.py"
    spec = importlib.util.spec_from_file_location("gateway_crypto_app", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load gateway/crypto/app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gateway_crypto_app = _load_gateway_crypto_app_module()


class _FakeAdapter:
    def __init__(self, *, fail_on_symbol: str | None = None) -> None:
        self.fail_on_symbol = fail_on_symbol
        self.calls: list[dict] = []

    def place_market_order(self, **kwargs):  # noqa: ANN003
        self.calls.append(dict(kwargs))
        symbol = str(kwargs.get("symbol", ""))
        qty = float(kwargs.get("qty", 0.0))
        reduce_only = bool(kwargs.get("reduce_only", False))
        if self.fail_on_symbol and (self.fail_on_symbol in symbol) and not reduce_only:
            return {
                "status": "error",
                "broker_order_id": None,
                "filled_qty": 0.0,
                "avg_price": None,
                "fee": 0.0,
                "reject_reason": "simulated_failure",
                "meta": {},
            }
        return {
            "status": "filled",
            "broker_order_id": f"ord-{len(self.calls)}",
            "filled_qty": qty,
            "avg_price": float(kwargs.get("price_hint") or 100.0),
            "fee": 0.0,
            "reject_reason": None,
            "meta": {"reduce_only": reduce_only},
        }


def test_execute_intent_idempotency_replays_without_duplicate_legs(tmp_path) -> None:
    db_path = tmp_path / "crypto_gateway.sqlite3"
    store = gateway_crypto_app.IdempotencyStore(str(db_path))
    binance = _FakeAdapter()
    hyper = _FakeAdapter()
    coordinator = gateway_crypto_app.ExecutionCoordinator(
        store=store,
        binance_adapter=binance,
        hyperliquid_adapter=hyper,
    )

    payload = {
        "intent_id": "intent-1",
        "idempotency_key": "intent-1",
        "timeout_sec": 2,
        "legs": [
            {
                "leg_id": "leg-a",
                "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                "venue": "binance_perp",
                "side": "BUY",
                "qty": 1.0,
                "price_hint": 100.0,
            },
            {
                "leg_id": "leg-b",
                "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                "venue": "hyperliquid_perp",
                "side": "SELL",
                "qty": 1.0,
                "price_hint": 100.0,
            },
        ],
    }

    first = coordinator.execute_intent(payload)
    second = coordinator.execute_intent(payload)

    assert first["status"] == "filled"
    assert first["idempotency_replay"] is False
    assert second["status"] == "filled"
    assert second["idempotency_replay"] is True
    assert len(binance.calls) == 1
    assert len(hyper.calls) == 1


def test_execute_intent_partial_fill_triggers_panic_close(tmp_path) -> None:
    db_path = tmp_path / "crypto_gateway.sqlite3"
    store = gateway_crypto_app.IdempotencyStore(str(db_path))
    binance = _FakeAdapter()
    hyper = _FakeAdapter(fail_on_symbol="ETH")
    coordinator = gateway_crypto_app.ExecutionCoordinator(
        store=store,
        binance_adapter=binance,
        hyperliquid_adapter=hyper,
    )

    payload = {
        "intent_id": "intent-partial",
        "idempotency_key": "intent-partial",
        "timeout_sec": 2,
        "panic": {"close_on_partial_fill": True},
        "legs": [
            {
                "leg_id": "leg-filled",
                "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                "venue": "binance_perp",
                "side": "BUY",
                "qty": 1.0,
                "price_hint": 100.0,
            },
            {
                "leg_id": "leg-failed",
                "symbol": "CRYPTO:ETHUSDT.PERP.HYPER",
                "venue": "hyperliquid_perp",
                "side": "SELL",
                "qty": 1.0,
                "price_hint": 100.0,
            },
        ],
    }

    result = coordinator.execute_intent(payload)

    assert result["status"] == "partial_closed"
    panic_close = result["panic_close"]
    assert panic_close["triggered"] is True
    assert panic_close["reason"] == "partial_fill_forced_flat"
    assert len(panic_close["legs"]) == 1
    assert result["risk_event"]["event_type"] == "crypto_partial_fill_forced_flat"
    # filled leg + panic close leg
    assert len(binance.calls) == 2
    assert bool(binance.calls[-1].get("reduce_only")) is True


def test_api_requires_auth_when_token_set(tmp_path, monkeypatch) -> None:
    state_path = str(tmp_path / "crypto_gateway.sqlite3")
    monkeypatch.setenv("CRYPTO_GATEWAY_AUTH_TOKEN", "secret-token")
    monkeypatch.setenv("CRYPTO_GATEWAY_STATE_PATH", state_path)
    monkeypatch.setenv("CRYPTO_GATEWAY_DRY_RUN", "1")
    monkeypatch.setenv("BINANCE_LIVE_ENABLED", "0")
    monkeypatch.setenv("HYPERLIQUID_LIVE_ENABLED", "0")

    app = gateway_crypto_app.create_app()
    client = app.test_client()
    payload = {
        "intent_id": "intent-api",
        "idempotency_key": "intent-api",
        "legs": [
            {
                "leg_id": "leg-a",
                "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                "venue": "binance_perp",
                "side": "BUY",
                "qty": 1.0,
            },
            {
                "leg_id": "leg-b",
                "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                "venue": "hyperliquid_perp",
                "side": "SELL",
                "qty": 1.0,
            },
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
    assert body["status"] == "filled"
