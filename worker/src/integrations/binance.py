from __future__ import annotations

from datetime import datetime, timezone
import json
from time import perf_counter
from typing import Any

import requests

from src.types import CryptoMarketSnapshot

try:
    import websocket  # type: ignore[import-untyped]
except Exception:  # noqa: BLE001
    websocket = None


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_mid_and_spread_bps(best_bid: float | None, best_ask: float | None) -> tuple[float | None, float | None]:
    if best_bid is None or best_ask is None:
        return None, None
    if best_bid <= 0 or best_ask <= 0:
        return None, None
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None, None
    spread_bps = ((best_ask - best_bid) / mid) * 10000.0
    return mid, spread_bps


def _compute_basis_bps(mark_price: float | None, index_price: float | None) -> float | None:
    if mark_price is None or index_price is None or index_price <= 0:
        return None
    return ((mark_price - index_price) / index_price) * 10000.0


class BinanceMarketClient:
    SPOT_REST_BASE_URL = "https://api.binance.com"
    PERP_REST_BASE_URL = "https://fapi.binance.com"
    SPOT_WS_BASE_URL = "wss://stream.binance.com:9443/ws"
    PERP_WS_BASE_URL = "wss://fstream.binance.com/ws"

    def __init__(
        self,
        rest_timeout_sec: float = 5.0,
        ws_timeout_sec: float = 1.5,
        session: requests.Session | None = None,
    ) -> None:
        self.rest_timeout_sec = float(rest_timeout_sec)
        self.ws_timeout_sec = float(ws_timeout_sec)
        self._session = session or requests.Session()

    def _rest_get(self, market_type: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
        market = str(market_type).strip().lower()
        base_url = self.SPOT_REST_BASE_URL if market == "spot" else self.PERP_REST_BASE_URL
        resp = self._session.get(
            f"{base_url}{path}",
            params=params,
            timeout=max(1.0, self.rest_timeout_sec),
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Binance payload type for {path}")
        return payload

    def fetch_book_ticker_rest(self, symbol: str, market_type: str = "perp") -> dict[str, Any]:
        normalized_market = str(market_type).strip().lower()
        path = "/api/v3/ticker/bookTicker" if normalized_market == "spot" else "/fapi/v1/ticker/bookTicker"
        payload = self._rest_get(
            market_type=normalized_market,
            path=path,
            params={"symbol": str(symbol).strip().upper()},
        )
        return {
            "best_bid": _to_optional_float(payload.get("bidPrice")),
            "best_ask": _to_optional_float(payload.get("askPrice")),
            "raw": payload,
        }

    def fetch_book_ticker_ws(self, symbol: str, market_type: str = "perp") -> dict[str, Any]:
        if websocket is None:
            raise RuntimeError("websocket-client is not available")

        normalized_market = str(market_type).strip().lower()
        base_url = self.SPOT_WS_BASE_URL if normalized_market == "spot" else self.PERP_WS_BASE_URL
        stream_symbol = str(symbol).strip().lower()
        ws_url = f"{base_url}/{stream_symbol}@bookTicker"
        ws = websocket.create_connection(ws_url, timeout=max(0.2, self.ws_timeout_sec))
        try:
            message = ws.recv()
        finally:
            ws.close()

        payload: dict[str, Any]
        if isinstance(message, (bytes, bytearray)):
            payload = json.loads(message.decode("utf-8"))
        else:
            payload = json.loads(str(message))
        if not isinstance(payload, dict):
            raise RuntimeError("unexpected Binance ws payload type")

        return {
            "best_bid": _to_optional_float(payload.get("b")),
            "best_ask": _to_optional_float(payload.get("a")),
            "raw": payload,
        }

    def fetch_perp_metrics_rest(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = str(symbol).strip().upper()
        premium = self._rest_get(
            market_type="perp",
            path="/fapi/v1/premiumIndex",
            params={"symbol": normalized_symbol},
        )
        open_interest = self._rest_get(
            market_type="perp",
            path="/fapi/v1/openInterest",
            params={"symbol": normalized_symbol},
        )

        funding_rate = _to_optional_float(premium.get("lastFundingRate"))
        mark_price = _to_optional_float(premium.get("markPrice"))
        index_price = _to_optional_float(premium.get("indexPrice"))
        oi = _to_optional_float(open_interest.get("openInterest"))

        return {
            "funding_rate": funding_rate,
            "mark_price": mark_price,
            "index_price": index_price,
            "open_interest": oi,
            "basis_bps": _compute_basis_bps(mark_price, index_price),
            "raw": {
                "premium_index": premium,
                "open_interest": open_interest,
            },
        }

    def fetch_market_snapshot(
        self,
        symbol: str,
        market_type: str = "perp",
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshot:
        normalized_symbol = str(symbol).strip().upper()
        normalized_market = str(market_type).strip().lower()
        if normalized_market not in {"spot", "perp"}:
            raise ValueError(f"unsupported market_type={market_type!r}")

        if observed_at is None:
            ts = datetime.now(timezone.utc)
        elif observed_at.tzinfo is None:
            ts = observed_at.replace(tzinfo=timezone.utc)
        else:
            ts = observed_at.astimezone(timezone.utc)

        started = perf_counter()
        source_mode = "ws"
        ws_failed = False
        ws_error: str | None = None

        try:
            book = self.fetch_book_ticker_ws(normalized_symbol, market_type=normalized_market)
        except Exception as exc:  # noqa: BLE001
            ws_failed = True
            ws_error = str(exc)
            source_mode = "rest"
            book = self.fetch_book_ticker_rest(normalized_symbol, market_type=normalized_market)

        metrics: dict[str, Any]
        if normalized_market == "perp":
            metrics = self.fetch_perp_metrics_rest(normalized_symbol)
        else:
            metrics = {
                "funding_rate": None,
                "mark_price": None,
                "index_price": None,
                "open_interest": None,
                "basis_bps": None,
                "raw": {},
            }

        best_bid = _to_optional_float(book.get("best_bid"))
        best_ask = _to_optional_float(book.get("best_ask"))
        mid, spread_bps = _compute_mid_and_spread_bps(best_bid, best_ask)

        latency_ms = (perf_counter() - started) * 1000.0
        quality: dict[str, Any] = {
            "ws_failed": ws_failed,
            "market_type": normalized_market,
        }
        if ws_error:
            quality["ws_error"] = ws_error

        return CryptoMarketSnapshot(
            exchange="binance",
            symbol=normalized_symbol,
            market_type=normalized_market,
            observed_at=ts,
            best_bid=best_bid,
            best_ask=best_ask,
            mid=mid,
            spread_bps=spread_bps,
            funding_rate=_to_optional_float(metrics.get("funding_rate")),
            open_interest=_to_optional_float(metrics.get("open_interest")),
            mark_price=_to_optional_float(metrics.get("mark_price")),
            index_price=_to_optional_float(metrics.get("index_price")),
            basis_bps=_to_optional_float(metrics.get("basis_bps")),
            source_mode=source_mode,
            latency_ms=latency_ms,
            data_quality=quality,
            raw_payload={
                "book": book.get("raw"),
                "metrics": metrics.get("raw"),
            },
        )
