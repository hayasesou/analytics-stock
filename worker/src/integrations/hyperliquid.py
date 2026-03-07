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


def _extract_first_price(levels: Any) -> float | None:
    if not isinstance(levels, list) or not levels:
        return None
    first = levels[0]
    if isinstance(first, dict):
        for key in ("px", "price"):
            value = _to_optional_float(first.get(key))
            if value is not None:
                return value
        return None
    if isinstance(first, list) and first:
        return _to_optional_float(first[0])
    return None


def _extract_l2_prices(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    levels = payload.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return None, None
    best_bid = _extract_first_price(levels[0])
    best_ask = _extract_first_price(levels[1])
    return best_bid, best_ask


class HyperliquidMarketClient:
    REST_INFO_URL = "https://api.hyperliquid.xyz/info"
    WS_URL = "wss://api.hyperliquid.xyz/ws"

    def __init__(
        self,
        rest_timeout_sec: float = 5.0,
        ws_timeout_sec: float = 1.5,
        session: requests.Session | None = None,
    ) -> None:
        self.rest_timeout_sec = float(rest_timeout_sec)
        self.ws_timeout_sec = float(ws_timeout_sec)
        self._session = session or requests.Session()

    def _post_info(self, payload: dict[str, Any]) -> Any:
        resp = self._session.post(
            self.REST_INFO_URL,
            json=payload,
            timeout=max(1.0, self.rest_timeout_sec),
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_l2_book_rest(self, symbol: str) -> dict[str, Any]:
        payload = self._post_info({"type": "l2Book", "coin": str(symbol).strip().upper()})
        if not isinstance(payload, dict):
            raise RuntimeError("unexpected Hyperliquid l2Book payload type")
        best_bid, best_ask = _extract_l2_prices(payload)
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "raw": payload,
        }

    def fetch_l2_book_ws(self, symbol: str) -> dict[str, Any]:
        if websocket is None:
            raise RuntimeError("websocket-client is not available")

        normalized_symbol = str(symbol).strip().upper()
        ws = websocket.create_connection(self.WS_URL, timeout=max(0.2, self.ws_timeout_sec))
        try:
            ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {
                            "type": "l2Book",
                            "coin": normalized_symbol,
                        },
                    }
                )
            )
            for _ in range(8):
                message = ws.recv()
                if isinstance(message, (bytes, bytearray)):
                    raw = json.loads(message.decode("utf-8"))
                else:
                    raw = json.loads(str(message))
                if not isinstance(raw, dict):
                    continue
                candidate = raw.get("data")
                if not isinstance(candidate, dict):
                    candidate = raw
                best_bid, best_ask = _extract_l2_prices(candidate)
                if best_bid is None and best_ask is None:
                    continue
                return {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "raw": raw,
                }
        finally:
            ws.close()
        raise RuntimeError("did not receive Hyperliquid l2 book snapshot over ws")

    def fetch_market_context_rest(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = str(symbol).strip().upper()
        payload = self._post_info({"type": "metaAndAssetCtxs"})
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError("unexpected Hyperliquid metaAndAssetCtxs payload type")

        meta = payload[0]
        contexts = payload[1]
        if not isinstance(meta, dict) or not isinstance(contexts, list):
            raise RuntimeError("unexpected Hyperliquid metaAndAssetCtxs structure")

        universe = meta.get("universe")
        if not isinstance(universe, list):
            raise RuntimeError("Hyperliquid metaAndAssetCtxs missing universe[]")

        target_index: int | None = None
        for idx, row in enumerate(universe):
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", "")).strip().upper()
            if name == normalized_symbol:
                target_index = idx
                break
        if target_index is None or target_index >= len(contexts):
            raise RuntimeError(f"symbol not found in Hyperliquid universe: {normalized_symbol}")

        ctx = contexts[target_index]
        if not isinstance(ctx, dict):
            raise RuntimeError("unexpected Hyperliquid context row")

        funding_rate = _to_optional_float(ctx.get("funding"))
        if funding_rate is None:
            funding_rate = _to_optional_float(ctx.get("fundingRate"))

        open_interest = _to_optional_float(ctx.get("openInterest"))
        if open_interest is None:
            open_interest = _to_optional_float(ctx.get("openInterestUsd"))
        if open_interest is None:
            open_interest = _to_optional_float(ctx.get("openInterestValue"))

        mark_price = _to_optional_float(ctx.get("markPx"))
        if mark_price is None:
            mark_price = _to_optional_float(ctx.get("markPrice"))

        index_price = _to_optional_float(ctx.get("oraclePx"))
        if index_price is None:
            index_price = _to_optional_float(ctx.get("oraclePrice"))
        if index_price is None:
            index_price = _to_optional_float(ctx.get("indexPx"))

        return {
            "funding_rate": funding_rate,
            "open_interest": open_interest,
            "mark_price": mark_price,
            "index_price": index_price,
            "basis_bps": _compute_basis_bps(mark_price, index_price),
            "raw": {
                "meta": meta,
                "context": ctx,
            },
        }

    def fetch_market_snapshot(
        self,
        symbol: str,
        market_type: str = "perp",
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshot:
        normalized_market = str(market_type).strip().lower()
        if normalized_market != "perp":
            raise ValueError("Hyperliquid supports perp market_type only")

        normalized_symbol = str(symbol).strip().upper()
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
            book = self.fetch_l2_book_ws(normalized_symbol)
        except Exception as exc:  # noqa: BLE001
            ws_failed = True
            ws_error = str(exc)
            source_mode = "rest"
            book = self.fetch_l2_book_rest(normalized_symbol)

        metrics = self.fetch_market_context_rest(normalized_symbol)

        best_bid = _to_optional_float(book.get("best_bid"))
        best_ask = _to_optional_float(book.get("best_ask"))
        mid, spread_bps = _compute_mid_and_spread_bps(best_bid, best_ask)
        latency_ms = (perf_counter() - started) * 1000.0

        quality: dict[str, Any] = {"ws_failed": ws_failed}
        if ws_error:
            quality["ws_error"] = ws_error

        return CryptoMarketSnapshot(
            exchange="hyperliquid",
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
