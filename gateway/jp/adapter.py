from __future__ import annotations

from typing import Any
import time
import uuid

import requests

from gateway.jp.common import normalize_margin_type, normalize_side, normalize_symbol, to_float, to_int


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
        symbol = normalize_symbol(str(leg["symbol"]))
        side = normalize_side(str(leg["side"]))
        qty = abs(to_int(round(to_float(leg["qty"], 0.0)), 0))
        order_type = str(leg.get("order_type", "MKT")).strip().upper()
        margin_type = normalize_margin_type(leg.get("margin_type"))
        price = to_float(leg.get("limit_price"), 0.0)

        cash_margin_map = {"cash": 1, "margin_open": 2, "margin_close": 3}
        front_order_type = 10 if order_type == "MKT" else 20
        payload: dict[str, Any] = {
            "Symbol": symbol,
            "Exchange": int(to_int(leg.get("exchange"), 1)),
            "SecurityType": 1,
            "Side": "2" if side == "BUY" else "1",
            "CashMargin": cash_margin_map[margin_type],
            "MarginTradeType": int(to_int(leg.get("margin_trade_type"), 3)),
            "DelivType": int(to_int(leg.get("deliv_type"), 2)),
            "AccountType": int(to_int(leg.get("account_type"), 4)),
            "Qty": qty,
            "FrontOrderType": front_order_type,
            "Price": 0 if order_type == "MKT" else price,
            "ExpireDay": int(to_int(leg.get("expire_day"), 0)),
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
