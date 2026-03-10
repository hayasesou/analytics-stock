from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode
import uuid

import requests

from gateway.crypto.common import normalize_side, normalize_symbol, to_float


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
        return hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

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
            "avg_price": to_float(price_hint, 0.0),
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
        symbol_norm = normalize_symbol(symbol, venue)
        side_norm = normalize_side(side)
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
        base_url = self.SPOT_BASE_URL if is_spot else self.PERP_BASE_URL
        path = "/api/v3/order" if is_spot else "/fapi/v1/order"

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

        filled_qty = to_float(payload.get("executedQty"), 0.0)
        quote_qty = to_float(payload.get("cummulativeQuoteQty"), 0.0)
        avg_price = None
        if filled_qty > 0:
            avg_price = quote_qty / filled_qty if quote_qty > 0 else to_float(payload.get("avgPrice"), to_float(price_hint, 0.0))
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
        symbol_norm = normalize_symbol(symbol, venue)
        side_norm = normalize_side(side)
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
                "avg_price": to_float(price_hint, 0.0),
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
