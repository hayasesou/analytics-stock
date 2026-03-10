from __future__ import annotations

from datetime import datetime
from typing import Any

from src.jobs.executor_support import (
    _extract_qtys,
    _infer_instrument_type,
    _market_key,
    _notify_risk_bulletin,
    _price_row_is_stale,
    _to_float,
)
from src.types import FillRecord, OrderRecord, PositionRecord


def process_paper_intent(
    *,
    repo,
    notifier,
    now: datetime,
    intent: dict[str, Any],
    stats: dict[str, int],
    intent_id: str,
    strategy_version_id: str | None,
    portfolio_id: str,
    broker_map: dict[str, Any],
    target_positions: list[dict[str, Any]],
    latest_rating_by_symbol: dict[str, str],
    fundamental_overlay: dict[str, Any],
    data_quality: dict[str, Any],
) -> None:
    orders: list[OrderRecord] = []
    fills: list[FillRecord] = []
    positions: list[PositionRecord] = []
    fill_price_by_symbol: dict[str, float] = {}
    latest_price_cache: dict[str, dict[str, Any] | None] = {}
    error_count = 0
    filtered_count = 0
    data_quality_filtered_count = 0
    missing_price_filtered_count = 0
    stale_price_filtered_count = 0

    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue

        target_qty, delta_qty = _extract_qtys(position)
        if abs(delta_qty) < 1e-12:
            continue
        rating = latest_rating_by_symbol.get(symbol)
        if bool(fundamental_overlay.get("enabled")):
            allow_if_missing = bool(fundamental_overlay.get("allow_if_missing", True))
            allow_ratings = set(fundamental_overlay.get("allow_ratings", {"A", "B"}))
            if rating is None and not allow_if_missing:
                filtered_count += 1
                stats["skipped_by_fundamental"] += 1
                continue
            if rating is not None and rating not in allow_ratings:
                filtered_count += 1
                stats["skipped_by_fundamental"] += 1
                continue
            multiplier = float(
                dict(fundamental_overlay.get("size_multiplier", {})).get(
                    str(rating or "").upper(),
                    1.0,
                )
            )
            target_qty *= multiplier
            delta_qty *= multiplier
            if abs(delta_qty) < 1e-12:
                filtered_count += 1
                stats["skipped_by_fundamental"] += 1
                continue

        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        market_key = _market_key(instrument_type)
        broker = str(broker_map.get(market_key, "paper"))
        side = "BUY"
        if delta_qty < 0:
            side = "SELL_SHORT" if target_qty < 0 else "SELL"

        if symbol in latest_price_cache:
            latest_price_row = latest_price_cache[symbol]
        else:
            latest_price_row = repo.fetch_latest_price_for_symbol(symbol)
            latest_price_cache[symbol] = latest_price_row

        if bool(data_quality.get("enabled", True)):
            reject_on_missing = bool(data_quality.get("reject_on_missing_price", True))
            if latest_price_row is None:
                if reject_on_missing:
                    data_quality_filtered_count += 1
                    missing_price_filtered_count += 1
                    stats["skipped_by_data_quality"] += 1
                    continue
            elif _price_row_is_stale(
                latest_price_row=latest_price_row,
                market_key=market_key,
                now=now,
                max_staleness_days=dict(data_quality.get("max_staleness_days", {})),
            ):
                data_quality_filtered_count += 1
                stale_price_filtered_count += 1
                stats["skipped_by_data_quality"] += 1
                continue

        if not latest_price_row:
            error_count += 1
            continue
        fill_price = _to_float(latest_price_row.get("close_raw"), 0.0)
        if fill_price <= 0:
            error_count += 1
            continue
        fill_price_by_symbol[symbol] = fill_price

        orders.append(
            OrderRecord(
                intent_id=intent_id,
                broker=broker,
                symbol=symbol,
                instrument_type=instrument_type,
                side=side,
                order_type="MKT",
                qty=abs(delta_qty),
                status="filled",
                idempotency_key=f"{intent_id}:{symbol}:{side}:{abs(delta_qty):.8f}",
                meta={
                    "executor": "paper",
                    "portfolio": intent.get("portfolio_name"),
                    "fundamental_rating": rating,
                },
                submitted_at=now,
            )
        )
        position_qty = target_qty if "target_qty" in position or "targetQty" in position else delta_qty
        positions.append(
            PositionRecord(
                portfolio_id=portfolio_id,
                symbol=symbol,
                instrument_type=instrument_type,
                qty=position_qty,
                avg_price=fill_price,
                last_price=fill_price,
                market_value=position_qty * fill_price,
            )
        )

    if (missing_price_filtered_count + stale_price_filtered_count) > 0:
        _notify_risk_bulletin(
            notifier=notifier,
            now=now,
            category="data_freshness_ng",
            title="price freshness check failed",
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            detail=(
                f"missing={missing_price_filtered_count} stale={stale_price_filtered_count} "
                f"target_positions={len(target_positions)}"
            ),
        )

    if not orders:
        if (filtered_count + data_quality_filtered_count) > 0 and error_count == 0:
            repo.update_order_intent_status(intent_id, "rejected")
            stats["rejected"] += 1
        else:
            repo.update_order_intent_status(intent_id, "failed")
            stats["failed"] += 1
        return

    order_ids = repo.insert_orders_bulk(orders)
    for order_id, order in zip(order_ids, orders, strict=True):
        fill_price = fill_price_by_symbol.get(order.symbol, 0.0)
        fills.append(
            FillRecord(
                order_id=order_id,
                fill_time=now,
                qty=order.qty,
                price=fill_price,
                fee=0.0,
                meta={"side": order.side},
            )
        )
    repo.insert_order_fills(fills)
    repo.upsert_positions(positions)

    if error_count > 0:
        repo.update_order_intent_status(intent_id, "failed")
        stats["failed"] += 1
    else:
        repo.update_order_intent_status(intent_id, "done")
        stats["done"] += 1
