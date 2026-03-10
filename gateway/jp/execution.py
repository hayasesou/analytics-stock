from __future__ import annotations

import hashlib
import json
from typing import Any

from gateway.jp.common import normalize_margin_type, normalize_side, normalize_symbol, to_float, to_int, utc_now_iso


class ExecutionCoordinator:
    def __init__(
        self,
        *,
        store,
        adapter,
        limiter,
        default_wait_timeout_sec: float,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.limiter = limiter
        self.default_wait_timeout_sec = max(0.1, float(default_wait_timeout_sec))

    def _normalize_leg(self, leg: dict[str, Any], idx: int) -> dict[str, Any]:
        symbol = str(leg.get("symbol", "")).strip()
        side = normalize_side(str(leg.get("side", "")).strip().upper())
        qty = abs(to_float(leg.get("qty"), 0.0))
        if not symbol:
            raise ValueError(f"leg_{idx}_symbol_required")
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"leg_{idx}_side_invalid")
        if qty <= 0:
            raise ValueError(f"leg_{idx}_qty_invalid")
        order_type = str(leg.get("order_type", "MKT")).strip().upper()
        if order_type not in {"MKT", "LMT"}:
            raise ValueError(f"leg_{idx}_order_type_invalid")
        return {
            "leg_id": str(leg.get("leg_id", f"leg-{idx + 1}")),
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_type": order_type,
            "limit_price": to_float(leg.get("limit_price"), 0.0) if order_type == "LMT" else None,
            "exchange": to_int(leg.get("exchange"), 1),
            "margin_type": normalize_margin_type(leg.get("margin_type")),
            "margin_trade_type": to_int(leg.get("margin_trade_type"), 3),
            "deliv_type": to_int(leg.get("deliv_type"), 2),
            "account_type": to_int(leg.get("account_type"), 4),
            "expire_day": to_int(leg.get("expire_day"), 0),
            "close_positions": leg.get("close_positions") if isinstance(leg.get("close_positions"), list) else None,
            "target_qty": to_float(leg.get("target_qty"), 0.0),
        }

    def _fingerprint(self, leg: dict[str, Any]) -> str:
        material = {
            "symbol": normalize_symbol(str(leg["symbol"])),
            "side": leg["side"],
            "qty": round(to_float(leg["qty"], 0.0), 8),
            "order_type": leg["order_type"],
            "limit_price": round(to_float(leg.get("limit_price"), 0.0), 8),
            "exchange": leg["exchange"],
            "margin_type": leg["margin_type"],
            "margin_trade_type": leg["margin_trade_type"],
            "deliv_type": leg["deliv_type"],
            "account_type": leg["account_type"],
            "expire_day": leg["expire_day"],
        }
        raw = json.dumps(material, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def execute_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        intent_id = str(payload.get("intent_id", "")).strip()
        if not intent_id:
            raise ValueError("intent_id_required")
        idempotency_key = str(payload.get("idempotency_key", f"intent:{intent_id}")).strip()
        if not idempotency_key:
            raise ValueError("idempotency_key_required")
        replay = self.store.fetch_result(idempotency_key)
        if replay:
            replay = dict(replay)
            replay["idempotency_replay"] = True
            return replay

        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or not raw_legs:
            raise ValueError("legs_required")
        legs = [self._normalize_leg(leg, idx) for idx, leg in enumerate(raw_legs)]
        wait_timeout_sec = to_float(payload.get("wait_timeout_sec"), self.default_wait_timeout_sec)

        results: list[dict[str, Any]] = []
        for leg in legs:
            fingerprint = self._fingerprint(leg)
            previous = self.store.fetch_leg_fingerprint(intent_id=intent_id, leg_id=leg["leg_id"])
            if previous == fingerprint:
                results.append(
                    {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "diff_skip",
                        "filled_qty": 0.0,
                        "avg_price": None,
                        "broker_order_id": None,
                        "reject_reason": None,
                        "meta": {"fingerprint": fingerprint},
                    }
                )
                continue

            acquired = self.limiter.acquire(symbol=leg["symbol"], timeout_sec=wait_timeout_sec)
            if not acquired:
                results.append(
                    {
                        "leg_id": leg["leg_id"],
                        "symbol": leg["symbol"],
                        "side": leg["side"],
                        "qty": leg["qty"],
                        "status": "error",
                        "filled_qty": 0.0,
                        "avg_price": None,
                        "broker_order_id": None,
                        "reject_reason": "rate_limit_timeout",
                        "meta": {"fingerprint": fingerprint},
                    }
                )
                continue

            sent = self.adapter.place_order(leg)
            status = str(sent.get("status", "error"))
            result = {
                "leg_id": leg["leg_id"],
                "symbol": leg["symbol"],
                "side": leg["side"],
                "qty": leg["qty"],
                "target_qty": leg["target_qty"],
                "status": status,
                "filled_qty": to_float(sent.get("filled_qty"), 0.0),
                "avg_price": sent.get("avg_price"),
                "broker_order_id": sent.get("broker_order_id"),
                "reject_reason": sent.get("reject_reason"),
                "meta": sent.get("meta") if isinstance(sent.get("meta"), dict) else {},
            }
            results.append(result)
            if status in {"ack", "filled"}:
                self.store.upsert_leg_fingerprint(intent_id=intent_id, leg_id=leg["leg_id"], fingerprint=fingerprint)

        non_skip = [item for item in results if item["status"] != "diff_skip"]
        if non_skip and all(item["status"] == "ack" for item in non_skip):
            status = "ack"
        elif not non_skip:
            status = "no_change"
        elif any(item["status"] in {"rejected", "error"} for item in non_skip):
            status = "failed"
        else:
            status = "partial"

        risk_event = None
        if status in {"failed", "partial"}:
            risk_event = {
                "event_type": "jp_gateway_execution_failed",
                "payload": {"intent_id": intent_id, "status": status, "legs": results},
            }

        response = {
            "intent_id": intent_id,
            "idempotency_key": idempotency_key,
            "idempotency_replay": False,
            "status": status,
            "executed_at": utc_now_iso(),
            "legs": results,
            "risk_event": risk_event,
        }
        self.store.save_result(
            idempotency_key=idempotency_key,
            intent_id=intent_id,
            status=status,
            payload=response,
        )
        return response
