from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any

from gateway.crypto.common import normalize_side, opposite_side, to_float, utc_now_iso


class ExecutionCoordinator:
    def __init__(
        self,
        store,
        *,
        binance_adapter,
        hyperliquid_adapter,
    ) -> None:
        self.store = store
        self.binance_adapter = binance_adapter
        self.hyperliquid_adapter = hyperliquid_adapter

    def _resolve_adapter(self, venue: str) -> Any:
        venue_norm = str(venue).strip().lower()
        if "binance" in venue_norm:
            return self.binance_adapter
        if "hyperliquid" in venue_norm or "hyper" in venue_norm:
            return self.hyperliquid_adapter
        raise ValueError(f"unsupported_venue:{venue}")

    def _normalize_leg(self, leg: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = str(leg.get("symbol", "")).strip()
        venue = str(leg.get("venue", "")).strip().lower()
        qty = abs(to_float(leg.get("qty"), 0.0))
        if not symbol:
            raise ValueError(f"leg_{idx}_symbol_required")
        if not venue:
            raise ValueError(f"leg_{idx}_venue_required")
        if qty <= 0:
            raise ValueError(f"leg_{idx}_qty_invalid")
        side = normalize_side(str(leg.get("side", "")).strip().upper())
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"leg_{idx}_side_invalid")

        return {
            "leg_id": str(leg.get("leg_id", f"leg-{idx + 1}")),
            "symbol": symbol,
            "venue": venue,
            "side": side,
            "qty": qty,
            "price_hint": to_float(leg.get("price_hint"), 0.0),
        }

    def _execute_leg(
        self,
        leg: dict[str, Any],
        *,
        reduce_only: bool = False,
        force_side: str | None = None,
    ) -> dict[str, Any]:
        venue = str(leg["venue"])
        side = force_side or str(leg["side"])
        qty = abs(to_float(leg["qty"], 0.0))
        symbol = str(leg["symbol"])
        adapter = self._resolve_adapter(venue)
        result = adapter.place_market_order(
            symbol=symbol,
            side=side,
            qty=qty,
            venue=venue,
            reduce_only=reduce_only,
            price_hint=to_float(leg.get("price_hint"), 0.0),
        )
        if not isinstance(result, dict):
            result = {}
        return {
            "leg_id": leg["leg_id"],
            "symbol": symbol,
            "venue": venue,
            "side": side,
            "qty": qty,
            "status": str(result.get("status", "error")),
            "filled_qty": to_float(result.get("filled_qty"), 0.0),
            "avg_price": to_float(result.get("avg_price"), 0.0),
            "fee": to_float(result.get("fee"), 0.0),
            "broker_order_id": result.get("broker_order_id"),
            "reject_reason": result.get("reject_reason"),
            "meta": result.get("meta") if isinstance(result.get("meta"), dict) else {},
        }

    def execute_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        intent_id = str(payload.get("intent_id", "")).strip()
        if not intent_id:
            raise ValueError("intent_id_required")
        idempotency_key = str(payload.get("idempotency_key", f"intent:{intent_id}")).strip()
        if not idempotency_key:
            raise ValueError("idempotency_key_required")

        replay = self.store.fetch(idempotency_key)
        if replay:
            replay = dict(replay)
            replay["idempotency_replay"] = True
            return replay

        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        timeout_sec = max(1.0, to_float(payload.get("timeout_sec"), 3.0))
        panic_cfg = payload.get("panic") if isinstance(payload.get("panic"), dict) else {}
        close_on_partial_fill = bool((panic_cfg or {}).get("close_on_partial_fill", True))

        leg_results: list[dict[str, Any] | None] = [None for _ in legs]
        future_map: dict[Any, int] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(legs))) as pool:
            for idx, leg in enumerate(legs):
                fut = pool.submit(self._execute_leg, leg)
                future_map[fut] = idx
            done, pending = wait(future_map.keys(), timeout=timeout_sec)
            for fut in done:
                idx = future_map[fut]
                try:
                    leg_results[idx] = fut.result()
                except Exception as exc:  # noqa: BLE001
                    leg = legs[idx]
                    leg_results[idx] = {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "venue": leg["venue"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "error",
                        "filled_qty": 0.0,
                        "avg_price": 0.0,
                        "fee": 0.0,
                        "broker_order_id": None,
                        "reject_reason": f"leg_exec_error:{exc}",
                        "meta": {},
                    }
            for fut in pending:
                idx = future_map[fut]
                fut.cancel()
                leg = legs[idx]
                leg_results[idx] = {
                    "leg_id": leg["leg_id"],
                    "symbol": leg["symbol"],
                    "venue": leg["venue"],
                    "side": leg["side"],
                    "qty": leg["qty"],
                    "status": "error",
                    "filled_qty": 0.0,
                    "avg_price": 0.0,
                    "fee": 0.0,
                    "broker_order_id": None,
                    "reject_reason": "leg_timeout",
                    "meta": {},
                }

        results = [row for row in leg_results if isinstance(row, dict)]
        filled = [row for row in results if row["status"] == "filled" and to_float(row["filled_qty"], 0.0) > 0.0]
        all_filled = len(filled) == len(legs)

        panic_close_legs: list[dict[str, Any]] = []
        panic_triggered = False
        panic_reason = None
        final_status = "filled"
        if not all_filled:
            final_status = "failed"
            if filled and close_on_partial_fill:
                panic_triggered = True
                panic_reason = "partial_fill_forced_flat"
                close_results = []
                for item in filled:
                    close_leg = {
                        "leg_id": f"{item['leg_id']}:close",
                        "symbol": item["symbol"],
                        "venue": item["venue"],
                        "side": opposite_side(str(item["side"])),
                        "qty": to_float(item["filled_qty"], 0.0),
                        "price_hint": to_float(item.get("avg_price"), 0.0),
                    }
                    close_result = self._execute_leg(close_leg, reduce_only=True, force_side=close_leg["side"])
                    close_result["meta"] = {
                        **dict(close_result.get("meta") or {}),
                        "panic_close": True,
                    }
                    close_results.append(close_result)
                panic_close_legs = close_results
                if all(row["status"] == "filled" for row in close_results):
                    final_status = "partial_closed"
                else:
                    final_status = "failed"

        resulting_positions: list[dict[str, Any]] = []
        if final_status == "filled":
            for item in results:
                signed_qty = to_float(item["filled_qty"], 0.0)
                if str(item["side"]).upper() == "SELL":
                    signed_qty *= -1.0
                resulting_positions.append(
                    {
                        "symbol": item["symbol"],
                        "venue": item["venue"],
                        "qty": signed_qty,
                        "avg_price": to_float(item["avg_price"], 0.0),
                    }
                )
        else:
            for leg in legs:
                resulting_positions.append(
                    {
                        "symbol": leg["symbol"],
                        "venue": leg["venue"],
                        "qty": 0.0,
                        "avg_price": 0.0,
                    }
                )

        risk_event = None
        if final_status != "filled":
            risk_event = {
                "event_type": "crypto_partial_fill_forced_flat" if panic_triggered else "crypto_execution_failed",
                "payload": {
                    "intent_id": intent_id,
                    "idempotency_key": idempotency_key,
                    "status": final_status,
                    "panic_reason": panic_reason,
                    "legs": results,
                    "panic_close_legs": panic_close_legs,
                },
            }

        response = {
            "intent_id": intent_id,
            "idempotency_key": idempotency_key,
            "idempotency_replay": False,
            "status": final_status,
            "executed_at": utc_now_iso(),
            "legs": results,
            "panic_close": {
                "triggered": panic_triggered,
                "reason": panic_reason,
                "legs": panic_close_legs,
            },
            "resulting_positions": resulting_positions,
            "risk_event": risk_event,
        }
        self.store.save(
            idempotency_key=idempotency_key,
            intent_id=intent_id,
            status=final_status,
            payload=response,
        )
        return response

    def panic_close(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        close_results = []
        for leg in legs:
            close_side = opposite_side(str(leg["side"]))
            close_results.append(
                self._execute_leg(
                    leg,
                    reduce_only=True,
                    force_side=close_side,
                )
            )
        status = "done" if all(item["status"] == "filled" for item in close_results) else "partial"
        return {
            "status": status,
            "closed_at": utc_now_iso(),
            "legs": close_results,
        }
