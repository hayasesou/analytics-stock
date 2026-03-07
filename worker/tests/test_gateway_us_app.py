from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_gateway_us_app_module():
    module_path = Path(__file__).resolve().parents[2] / "gateway" / "us" / "app.py"
    spec = importlib.util.spec_from_file_location("gateway_us_app", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load gateway/us/app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gateway_us_app = _load_gateway_us_app_module()


class _FakeAdapter:
    def __init__(self) -> None:
        self.place_calls = 0
        self.cancel_calls: list[str] = []
        self.fetch_order_statuses_payloads: list[list[str]] = []
        self._mode = "filled"
        self._open_orders: list[dict] = []
        self._status_updates: dict[str, dict] = {}

    def resolve_contract_spec(self, symbol: str) -> dict:
        return {
            "secType": "STK",
            "symbol": str(symbol).upper(),
            "exchange": "SMART",
            "currency": "USD",
        }

    def place_order(self, **kwargs):  # noqa: ANN003
        self.place_calls += 1
        symbol = str(kwargs.get("symbol", "")).upper()
        side = str(kwargs.get("side", "BUY")).upper()
        qty = float(kwargs.get("qty", 0.0))
        if self._mode == "pending":
            return {
                "status": "sent",
                "broker_order_id": "ord-1",
                "filled_qty": 0.0,
                "remaining_qty": qty,
                "avg_price": 0.0,
                "reject_reason": None,
                "meta": {"contract": self.resolve_contract_spec(symbol)},
            }
        return {
            "status": "filled",
            "broker_order_id": "ord-1",
            "filled_qty": qty,
            "remaining_qty": 0.0,
            "avg_price": float(kwargs.get("price_hint") or 210.0),
            "reject_reason": None,
            "meta": {"contract": self.resolve_contract_spec(symbol), "side": side},
        }

    def fetch_order_statuses(self, broker_order_ids: list[str]) -> dict[str, dict]:
        self.fetch_order_statuses_payloads.append(list(broker_order_ids))
        result: dict[str, dict] = {}
        for broker_order_id in broker_order_ids:
            if broker_order_id in self._status_updates:
                result[broker_order_id] = dict(self._status_updates[broker_order_id])
        return result

    def cancel_order(self, broker_order_id: str) -> dict:
        self.cancel_calls.append(str(broker_order_id))
        return {
            "broker_order_id": str(broker_order_id),
            "status": "canceled",
            "filled_qty": 0.0,
            "remaining_qty": 1.0,
            "avg_price": 0.0,
            "reject_reason": "timeout_cancel",
        }

    def fetch_recent_fills(self, broker_order_ids: list[str]) -> list[dict]:
        if "ord-1" not in broker_order_ids:
            return []
        if self._mode == "pending":
            return []
        return [
            {
                "broker_order_id": "ord-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1.0,
                "price": 210.0,
                "fee": 0.0,
                "fill_time": gateway_us_app._utc_now_iso(),  # noqa: SLF001
                "meta": {},
            }
        ]

    def fetch_open_orders(self) -> list[dict]:
        return [dict(x) for x in self._open_orders]


def test_ibkr_contract_resolution_normalizes_symbol() -> None:
    adapter = gateway_us_app.IbkrTradeAdapter(
        host="127.0.0.1",
        port=4002,
        client_id=1301,
        account_id=None,
        live_enabled=False,
        dry_run=True,
    )

    spec = adapter.resolve_contract_spec("us:aapl")

    assert spec["secType"] == "STK"
    assert spec["symbol"] == "AAPL"
    assert spec["exchange"] == "SMART"
    assert spec["currency"] == "USD"


def test_execute_intent_idempotency_replays_without_duplicate_order(tmp_path) -> None:
    db_path = tmp_path / "us_gateway.sqlite3"
    store = gateway_us_app.IdempotencyStore(str(db_path))
    order_state = gateway_us_app.OrderStateStore(str(db_path))
    adapter = _FakeAdapter()
    coordinator = gateway_us_app.ExecutionCoordinator(store=store, order_state=order_state, adapter=adapter)
    payload = {
        "intent_id": "intent-1",
        "idempotency_key": "intent-1",
        "orders": [
            {
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1,
                "order_type": "MKT",
                "time_in_force": "DAY",
                "price_hint": 210.0,
            }
        ],
    }

    first = coordinator.execute_intent(payload)
    second = coordinator.execute_intent(payload)

    assert first["status"] == "filled"
    assert first["idempotency_replay"] is False
    assert second["status"] == "filled"
    assert second["idempotency_replay"] is True
    assert adapter.place_calls == 1


def test_execute_intent_timeout_cancels_pending_order(tmp_path) -> None:
    db_path = tmp_path / "us_gateway.sqlite3"
    store = gateway_us_app.IdempotencyStore(str(db_path))
    order_state = gateway_us_app.OrderStateStore(str(db_path))
    adapter = _FakeAdapter()
    adapter._mode = "pending"
    coordinator = gateway_us_app.ExecutionCoordinator(store=store, order_state=order_state, adapter=adapter)
    payload = {
        "intent_id": "intent-timeout",
        "idempotency_key": "intent-timeout",
        "timeout_sec": 1,
        "poll_interval_sec": 0.1,
        "orders": [
            {
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1,
                "order_type": "MKT",
                "time_in_force": "DAY",
            }
        ],
    }

    result = coordinator.execute_intent(payload)

    assert result["status"] == "failed"
    assert result["sync"]["canceled_on_timeout"] == 1
    assert result["orders"][0]["status"] == "canceled"
    assert result["risk_event"]["event_type"] == "us_execution_failed"
    assert adapter.cancel_calls == ["ord-1"]


def test_resync_orders_updates_pending_and_open_orders(tmp_path) -> None:
    db_path = tmp_path / "us_gateway.sqlite3"
    store = gateway_us_app.IdempotencyStore(str(db_path))
    order_state = gateway_us_app.OrderStateStore(str(db_path))
    adapter = _FakeAdapter()
    coordinator = gateway_us_app.ExecutionCoordinator(store=store, order_state=order_state, adapter=adapter)

    order_state.upsert_orders(
        [
            {
                "broker_order_id": "ord-1",
                "intent_id": "intent-x",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1.0,
                "filled_qty": 0.0,
                "remaining_qty": 1.0,
                "avg_price": 0.0,
                "status": "sent",
                "reject_reason": None,
                "meta": {},
            }
        ]
    )
    adapter._status_updates = {
        "ord-1": {
            "broker_order_id": "ord-1",
            "intent_id": "intent-x",
            "symbol": "AAPL",
            "side": "BUY",
            "qty": 1.0,
            "filled_qty": 1.0,
            "remaining_qty": 0.0,
            "avg_price": 210.0,
            "status": "filled",
            "reject_reason": None,
            "meta": {},
        }
    }
    adapter._open_orders = [
        {
            "broker_order_id": "ord-2",
            "intent_id": "intent-y",
            "symbol": "MSFT",
            "side": "BUY",
            "qty": 2.0,
            "filled_qty": 0.0,
            "remaining_qty": 2.0,
            "avg_price": 0.0,
            "status": "sent",
            "reject_reason": None,
            "meta": {},
        }
    ]

    result = coordinator.resync_orders()

    assert result["pending_before"] == 1
    assert result["resynced"] == 1
    assert result["open_orders"] == 1
    assert result["persisted"] == 2
    open_ids = {str(x.get("broker_order_id")) for x in result["orders"]}
    assert open_ids == {"ord-2"}


def test_api_requires_auth_when_token_set(tmp_path, monkeypatch) -> None:
    state_path = str(tmp_path / "us_gateway.sqlite3")
    monkeypatch.setenv("US_GATEWAY_AUTH_TOKEN", "secret-token")
    monkeypatch.setenv("US_GATEWAY_STATE_PATH", state_path)
    monkeypatch.setenv("US_GATEWAY_DRY_RUN", "1")
    monkeypatch.setenv("IBKR_LIVE_ENABLED", "0")

    app = gateway_us_app.create_app()
    client = app.test_client()
    payload = {
        "intent_id": "intent-api",
        "idempotency_key": "intent-api",
        "orders": [
            {
                "order_id": "ord-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1,
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
    assert body["status"] == "filled"


@pytest.mark.parametrize(
    ("raw_status", "filled_qty", "remaining_qty", "requested_qty", "expected"),
    [
        ("Submitted", 0.0, 1.0, 1.0, "sent"),
        ("Filled", 1.0, 0.0, 1.0, "filled"),
        ("Inactive", 0.0, 1.0, 1.0, "rejected"),
        ("Submitted", 0.4, 0.6, 1.0, "partially_filled"),
    ],
)
def test_standardize_status(raw_status, filled_qty, remaining_qty, requested_qty, expected) -> None:
    status = gateway_us_app._standardize_status(  # noqa: SLF001
        raw_status=raw_status,
        filled_qty=filled_qty,
        remaining_qty=remaining_qty,
        requested_qty=requested_qty,
    )
    assert status == expected
