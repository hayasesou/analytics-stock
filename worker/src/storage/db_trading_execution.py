from __future__ import annotations

from typing import Any
import json
from uuid import uuid4

from src.types import FillRecord, OrderIntent, OrderRecord, PortfolioSpec, PositionRecord


class NeonRepositoryTradingExecutionMixin:
    def fetch_strategy_symbols_for_portfolio(
        self,
        strategy_version_id: str,
        portfolio_id: str,
        lookback_days: int = 30,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    COALESCE(tp.value->>'symbol', tp.value->>'security_id') AS symbol,
                    NULLIF(tp.value->>'instrument_type', '') AS instrument_type
                FROM order_intents oi
                CROSS JOIN LATERAL jsonb_array_elements(oi.target_positions) AS tp(value)
                WHERE oi.strategy_version_id = %s::uuid
                  AND oi.portfolio_id = %s::uuid
                  AND oi.created_at >= NOW() - make_interval(days => %s)
                  AND oi.status IN ('approved', 'executing', 'sent', 'done')
                  AND COALESCE(tp.value->>'symbol', tp.value->>'security_id') IS NOT NULL
                """,
                (
                    strategy_version_id,
                    portfolio_id,
                    max(1, int(lookback_days)),
                ),
            )
            rows = cur.fetchall()
        return rows

    def upsert_portfolio(self, portfolio: PortfolioSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO portfolios (name, base_currency, broker_map)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (name)
                DO UPDATE SET base_currency = EXCLUDED.base_currency,
                              broker_map = EXCLUDED.broker_map
                RETURNING id::text
                """,
                (
                    portfolio.name,
                    portfolio.base_currency,
                    json.dumps(portfolio.broker_map),
                ),
            )
            portfolio_id = cur.fetchone()["id"]
            conn.commit()
        return portfolio_id

    def insert_order_intent(self, intent: OrderIntent) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO order_intents (
                    portfolio_id, strategy_version_id, as_of, target_positions, reason,
                    risk_checks, status, approved_at, approved_by
                )
                VALUES (%s::uuid, %s::uuid, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s)
                RETURNING id::text
                """,
                (
                    intent.portfolio_id,
                    intent.strategy_version_id,
                    intent.as_of,
                    json.dumps(intent.target_positions),
                    intent.reason,
                    json.dumps(intent.risk_checks),
                    intent.status,
                    intent.approved_at,
                    intent.approved_by,
                ),
            )
            intent_id = cur.fetchone()["id"]
            conn.commit()
        return intent_id

    def insert_orders_bulk(self, orders: list[OrderRecord]) -> list[str]:
        if not orders:
            return []

        ids = [str(uuid4()) for _ in orders]
        rows: list[tuple[Any, ...]] = []
        for order_id, order in zip(ids, orders, strict=True):
            rows.append(
                (
                    order_id,
                    order.intent_id,
                    order.broker,
                    order.account_id,
                    order.symbol,
                    order.instrument_type,
                    order.side,
                    order.order_type,
                    float(order.qty),
                    order.limit_price,
                    order.stop_price,
                    order.time_in_force,
                    order.status,
                    order.broker_order_id,
                    order.idempotency_key,
                    order.submitted_at,
                    json.dumps(order.meta),
                )
            )

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO orders (
                    id, intent_id, broker, account_id, symbol, instrument_type, side, order_type,
                    qty, limit_price, stop_price, time_in_force, status, broker_order_id,
                    idempotency_key, submitted_at, meta
                )
                VALUES (
                    %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s::jsonb
                )
                ON CONFLICT (broker, idempotency_key)
                DO UPDATE SET account_id = EXCLUDED.account_id,
                              symbol = EXCLUDED.symbol,
                              instrument_type = EXCLUDED.instrument_type,
                              side = EXCLUDED.side,
                              order_type = EXCLUDED.order_type,
                              qty = EXCLUDED.qty,
                              limit_price = EXCLUDED.limit_price,
                              stop_price = EXCLUDED.stop_price,
                              time_in_force = EXCLUDED.time_in_force,
                              status = EXCLUDED.status,
                              broker_order_id = EXCLUDED.broker_order_id,
                              submitted_at = EXCLUDED.submitted_at,
                              updated_at = NOW(),
                              meta = EXCLUDED.meta
                """,
                rows,
            )
            conn.commit()
        return ids

    def insert_order_fills(self, fills: list[FillRecord]) -> None:
        if not fills:
            return

        rows = [
            (
                fill.order_id,
                fill.fill_time,
                float(fill.qty),
                float(fill.price),
                float(fill.fee),
                json.dumps(fill.meta),
            )
            for fill in fills
        ]

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO order_fills (
                    order_id, fill_time, qty, price, fee, meta
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb)
                """,
                rows,
            )
            conn.commit()

    def upsert_positions(self, positions: list[PositionRecord]) -> None:
        if not positions:
            return

        rows = [
            (
                position.portfolio_id,
                position.symbol,
                position.instrument_type,
                float(position.qty),
                position.avg_price,
                position.last_price,
                position.market_value,
                position.unrealized_pnl,
                position.realized_pnl,
            )
            for position in positions
        ]

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO positions (
                    portfolio_id, symbol, instrument_type, qty, avg_price, last_price,
                    market_value, unrealized_pnl, realized_pnl
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (portfolio_id, symbol, instrument_type)
                DO UPDATE SET qty = EXCLUDED.qty,
                              avg_price = EXCLUDED.avg_price,
                              last_price = EXCLUDED.last_price,
                              market_value = EXCLUDED.market_value,
                              unrealized_pnl = EXCLUDED.unrealized_pnl,
                              realized_pnl = EXCLUDED.realized_pnl,
                              updated_at = NOW()
                """,
                rows,
            )
            conn.commit()
