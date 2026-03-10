from __future__ import annotations

from datetime import datetime
from typing import Any

from src.types import OrderRecord, PositionRecord
from src.jobs.executor_equity_support import *
from src.jobs.executor_state import *
from src.jobs.executor_values import _infer_instrument_type, _to_float, _to_optional_float


def _notify_risk_bulletin(
    notifier: DiscordNotifier,
    *,
    now: datetime,
    category: str,
    title: str,
    intent_id: str,
    strategy_version_id: str | None = None,
    detail: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, object] = {
        "category": category,
        "title": title,
        "intent_id": intent_id,
    }
    if strategy_version_id:
        payload["strategy_version_id"] = strategy_version_id
    if detail:
        payload["detail"] = detail
    if extra:
        payload.update(extra)
    notifier.send_risk_bulletin(now=now, items=[payload], top_n=1)


def _has_partial_fill(legs: list[dict[str, Any]]) -> bool:
    if not legs:
        return False
    filled = 0
    partial = 0
    unfilled = 0
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        status = str(leg.get("status", "")).strip().lower()
        qty = abs(_to_float(leg.get("qty"), 0.0))
        filled_qty = abs(_to_float(leg.get("filled_qty"), 0.0))
        if status in {"partially_filled", "partial_filled"} or (qty > 0 and 0 < filled_qty < qty):
            partial += 1
        elif status in {"filled", "done"} or (qty > 0 and filled_qty >= qty):
            filled += 1
        else:
            unfilled += 1
    return partial > 0 or (filled > 0 and unfilled > 0)


def _build_close_targets_from_positions(
    position_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for row in position_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        qty = _to_float(row.get("qty"), 0.0)
        if abs(qty) < 1e-12:
            continue
        instrument_type = str(row.get("instrument_type", _infer_instrument_type(symbol, None)))
        targets.append(
            {
                "symbol": symbol,
                "instrument_type": instrument_type,
                "target_qty": 0.0,
                "delta_qty": -qty,
                "price_hint": _to_optional_float(row.get("last_price")),
            }
        )
    return targets


def _upsert_zero_positions(
    repo: NeonRepository,
    *,
    portfolio_id: str,
    position_rows: list[dict[str, Any]],
) -> None:
    zero_positions: list[PositionRecord] = []
    for row in position_rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        instrument_type = str(row.get("instrument_type", _infer_instrument_type(symbol, None)))
        zero_positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol,
                instrument_type=instrument_type,
                qty=0.0,
                avg_price=_to_optional_float(row.get("avg_price")),
                last_price=_to_optional_float(row.get("last_price")),
                market_value=0.0,
            )
        )
    if zero_positions:
        repo.upsert_positions(zero_positions)


def _execute_strategy_panic_close(
    *,
    repo: NeonRepository,
    now: datetime,
    intent_id: str,
    portfolio_id: str,
    strategy_version_id: str,
    broker_map: dict[str, Any],
    strategy_risk_cfg: dict[str, Any],
    jp_gateway_cfg: dict[str, Any],
    us_gateway_cfg: dict[str, Any],
    jp_gateway_client: JpGatewayClient | None,
    us_gateway_client: USGatewayClient | None,
    crypto_gateway_client: CryptoGatewayClient | None,
) -> dict[str, Any]:
    symbol_rows = repo.fetch_strategy_symbols_for_portfolio(
        strategy_version_id=strategy_version_id,
        portfolio_id=portfolio_id,
        lookback_days=int(strategy_risk_cfg.get("symbol_lookback_days", 30)),
    )
    symbols = [
        str(row.get("symbol", "")).strip()
        for row in symbol_rows
        if isinstance(row, dict) and str(row.get("symbol", "")).strip()
    ]
    if not symbols:
        return {"status": "no_positions", "closed": 0, "failed": 0}

    position_rows = repo.fetch_positions_for_portfolio(
        portfolio_id=portfolio_id,
        symbols=symbols,
    )
    close_targets = _build_close_targets_from_positions(position_rows)
    if not close_targets:
        return {"status": "already_flat", "closed": 0, "failed": 0}

    summary = {
        "status": "done",
        "closed": 0,
        "failed": 0,
        "markets": {},
    }

    jp_targets = [x for x in close_targets if str(x.get("instrument_type")) == "JP_EQ"]
    us_targets = [x for x in close_targets if str(x.get("instrument_type")) == "US_EQ"]
    crypto_targets = [x for x in close_targets if str(x.get("instrument_type")) == "CRYPTO"]

    if jp_targets and jp_gateway_client is not None and str(broker_map.get("JP", "")) == str(jp_gateway_cfg.get("broker_name", "gateway_jp")):
        payload = _build_jp_gateway_payload(
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            portfolio_id=portfolio_id,
            target_positions=jp_targets,
            wait_timeout_sec=_to_float(jp_gateway_cfg.get("wait_timeout_sec"), 2.0),
        )
        try:
            result = jp_gateway_client.execute_intent(payload)
            legs = [x for x in (result.get("legs") or []) if isinstance(x, dict)]
            orders = _build_jp_order_records_from_gateway(intent_id=intent_id, now=now, legs=legs)
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                fills = _build_fill_records_from_gateway(order_ids=order_ids, orders=orders, legs=legs, now=now)
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["JP"] = {"status": result.get("status"), "legs": len(legs)}
            summary["closed"] += len(jp_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["JP"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(jp_targets)

    if us_targets and us_gateway_client is not None and str(broker_map.get("US", "")) == str(us_gateway_cfg.get("broker_name", "gateway_us")):
        payload = _build_us_gateway_payload(
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            portfolio_id=portfolio_id,
            target_positions=us_targets,
            default_order_timeout_sec=_to_float(us_gateway_cfg.get("default_order_timeout_sec"), 20.0),
        )
        try:
            result = us_gateway_client.execute_intent(payload)
            order_rows = [x for x in (result.get("orders") or []) if isinstance(x, dict)]
            fill_events = [x for x in (result.get("fills") or []) if isinstance(x, dict)]
            orders = _build_us_order_records_from_gateway(intent_id=intent_id, now=now, orders=order_rows)
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                order_id_by_broker_order_id = {
                    str(order.broker_order_id): order_id
                    for order_id, order in zip(order_ids, orders, strict=True)
                    if order.broker_order_id
                }
                fills = _build_fill_records_from_gateway_events(
                    order_id_by_broker_order_id=order_id_by_broker_order_id,
                    fill_events=fill_events,
                    now=now,
                )
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["US"] = {"status": result.get("status"), "orders": len(order_rows)}
            summary["closed"] += len(us_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["US"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(us_targets)

    if crypto_targets and crypto_gateway_client is not None and str(broker_map.get("CRYPTO", "")) == "crypto_gateway":
        legs = []
        for idx, target in enumerate(crypto_targets):
            delta_qty = _to_float(target.get("delta_qty"), 0.0)
            legs.append(
                {
                    "leg_id": f"panic-{idx + 1}",
                    "symbol": str(target.get("symbol", "")),
                    "venue": _infer_crypto_venue(str(target.get("symbol", "")), fallback=target.get("venue")),
                    "side": "SELL" if delta_qty > 0 else "BUY",
                    "qty": abs(delta_qty),
                    "price_hint": _to_optional_float(target.get("price_hint")),
                }
            )
        try:
            result = crypto_gateway_client.panic_close({"legs": legs})
            panic_legs = [x for x in (result.get("legs") or []) if isinstance(x, dict)]
            orders = _build_order_records_from_gateway(
                intent_id=intent_id,
                now=now,
                legs=panic_legs,
                phase="risk_panic_close",
            )
            if orders:
                order_ids = repo.insert_orders_bulk(orders)
                fills = _build_fill_records_from_gateway(order_ids=order_ids, orders=orders, legs=panic_legs, now=now)
                if fills:
                    repo.insert_order_fills(fills)
            summary["markets"]["CRYPTO"] = {"status": result.get("status"), "legs": len(panic_legs)}
            summary["closed"] += len(crypto_targets)
        except Exception as exc:  # noqa: BLE001
            summary["markets"]["CRYPTO"] = {"status": "error", "reason": str(exc)}
            summary["failed"] += len(crypto_targets)

    _upsert_zero_positions(
        repo=repo,
        portfolio_id=portfolio_id,
        position_rows=position_rows,
    )
    if summary["failed"] > 0:
        summary["status"] = "partial_failed"
    return summary



__all__ = [
    "_notify_risk_bulletin",
    "_has_partial_fill",
    "_build_close_targets_from_positions",
    "_upsert_zero_positions",
    "_execute_strategy_panic_close",
]
