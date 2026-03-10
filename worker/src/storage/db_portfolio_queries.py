from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


class NeonRepositoryPortfolioQueriesMixin:
    def fetch_approved_order_intents(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.id::text AS intent_id,
                    oi.portfolio_id::text AS portfolio_id,
                    oi.strategy_version_id::text AS strategy_version_id,
                    oi.as_of,
                    oi.target_positions,
                    oi.reason,
                    oi.risk_checks,
                    oi.status,
                    p.name AS portfolio_name,
                    p.base_currency,
                    p.broker_map
                FROM order_intents oi
                JOIN portfolios p
                  ON p.id = oi.portfolio_id
                WHERE oi.status = 'approved'
                ORDER BY oi.created_at ASC
                LIMIT %s
                """,
                (max(1, int(limit)),),
            )
            rows = cur.fetchall()
        return rows

    def has_recent_open_intent_for_strategy(
        self,
        strategy_version_id: str,
        lookback_minutes: int = 180,
    ) -> bool:
        lookback = max(1, int(lookback_minutes))
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM order_intents oi
                    WHERE oi.strategy_version_id = %s::uuid
                      AND oi.status IN ('proposed', 'approved', 'sent', 'executing')
                      AND oi.created_at >= NOW() - make_interval(mins => %s)
                ) AS exists_flag
                """,
                (strategy_version_id, lookback),
            )
            row = cur.fetchone()
        return bool((row or {}).get("exists_flag", False))

    def update_order_intent_status(self, intent_id: str, status: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE order_intents
                SET status = %s
                WHERE id = %s::uuid
                """,
                (status, intent_id),
            )
            conn.commit()

    def fetch_positions_for_portfolio(
        self,
        portfolio_id: str,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        symbol_filter = [str(symbol).strip() for symbol in (symbols or []) if str(symbol).strip()]
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    symbol,
                    instrument_type,
                    qty,
                    avg_price,
                    last_price,
                    market_value,
                    unrealized_pnl,
                    realized_pnl,
                    updated_at
                FROM positions
                WHERE portfolio_id = %s::uuid
                  AND (%s::text[] IS NULL OR symbol = ANY(%s))
                ORDER BY symbol, instrument_type
                """,
                (
                    portfolio_id,
                    symbol_filter if symbol_filter else None,
                    symbol_filter if symbol_filter else None,
                ),
            )
            rows = cur.fetchall()
        return rows

    def fetch_open_orders_for_portfolio(
        self,
        portfolio_id: str,
        symbols: list[str] | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        symbol_filter = [str(symbol).strip() for symbol in (symbols or []) if str(symbol).strip()]
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    o.id::text AS order_id,
                    o.intent_id::text AS intent_id,
                    o.broker,
                    o.symbol,
                    o.instrument_type,
                    o.side,
                    o.qty,
                    o.status,
                    o.submitted_at,
                    o.updated_at,
                    o.meta
                FROM orders o
                JOIN order_intents oi
                  ON oi.id = o.intent_id
                WHERE oi.portfolio_id = %s::uuid
                  AND o.status IN ('new', 'sent', 'ack', 'partially_filled')
                  AND (%s::text[] IS NULL OR o.symbol = ANY(%s))
                ORDER BY o.updated_at DESC
                LIMIT %s
                """,
                (
                    portfolio_id,
                    symbol_filter if symbol_filter else None,
                    symbol_filter if symbol_filter else None,
                    max(1, int(limit)),
                ),
            )
            rows = cur.fetchall()
        return rows

    def fetch_latest_risk_snapshot(self, portfolio_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    portfolio_id::text AS portfolio_id,
                    as_of,
                    equity,
                    drawdown,
                    sharpe_20d,
                    gross_exposure,
                    net_exposure,
                    state,
                    triggers
                FROM risk_snapshots
                WHERE portfolio_id = %s::uuid
                ORDER BY as_of DESC
                LIMIT 1
                """,
                (portfolio_id,),
            )
            row = cur.fetchone()
        return row

    def fetch_recent_risk_snapshots(self, portfolio_id: str, limit: int = 40) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    as_of,
                    equity,
                    drawdown,
                    sharpe_20d,
                    state
                FROM risk_snapshots
                WHERE portfolio_id = %s::uuid
                ORDER BY as_of DESC
                LIMIT %s
                """,
                (portfolio_id, max(2, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def fetch_latest_price_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.security_id,
                    s.market,
                    s.ticker,
                    pd.trade_date,
                    pd.close_raw
                FROM securities s
                JOIN LATERAL (
                    SELECT trade_date, close_raw
                    FROM prices_daily p
                    WHERE p.security_id = s.id
                    ORDER BY trade_date DESC
                    LIMIT 1
                ) pd ON TRUE
                WHERE s.security_id = %s
                   OR UPPER(s.ticker) = UPPER(%s)
                ORDER BY CASE WHEN s.security_id = %s THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (symbol, symbol, symbol),
            )
            row = cur.fetchone()
        return row

    def fetch_price_history_for_security(
        self,
        security_id: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.security_id,
                    s.market,
                    p.trade_date,
                    p.open_raw,
                    p.high_raw,
                    p.low_raw,
                    p.close_raw
                FROM prices_daily p
                JOIN securities s
                  ON s.id = p.security_id
                WHERE s.security_id = %s
                  AND p.trade_date BETWEEN %s AND %s
                ORDER BY p.trade_date
                """,
                (security_id, start_date, end_date),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(
                columns=[
                    "security_id",
                    "market",
                    "trade_date",
                    "open_raw",
                    "high_raw",
                    "low_raw",
                    "close_raw",
                ]
            )
        return pd.DataFrame(rows)

    def fetch_latest_fundamental_ratings_by_symbols(self, symbols: list[str]) -> dict[str, str]:
        cleaned = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        if not cleaned:
            return {}
        unique = list(dict.fromkeys(cleaned))
        ticker_candidates = []
        for symbol in unique:
            if ":" in symbol:
                ticker_candidates.append(symbol.split(":", 1)[1].upper())
            else:
                ticker_candidates.append(symbol.upper())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.security_id,
                    UPPER(s.ticker) AS ticker_upper,
                    fs.rating
                FROM securities s
                JOIN LATERAL (
                    SELECT rating
                    FROM fundamental_snapshots f
                    WHERE f.security_id = s.id
                    ORDER BY f.as_of_date DESC, f.created_at DESC
                    LIMIT 1
                ) fs ON TRUE
                WHERE s.security_id = ANY(%s)
                   OR UPPER(s.ticker) = ANY(%s)
                """,
                (unique, ticker_candidates),
            )
            rows = cur.fetchall()

        rating_by_security: dict[str, str] = {}
        rating_by_ticker: dict[str, str] = {}
        for row in rows:
            rating = str(row.get("rating", "")).upper().strip()
            security_id = str(row.get("security_id", "")).strip()
            ticker = str(row.get("ticker_upper", "")).strip()
            if rating:
                if security_id:
                    rating_by_security[security_id] = rating
                if ticker:
                    rating_by_ticker[ticker] = rating

        output: dict[str, str] = {}
        for symbol in unique:
            if symbol in rating_by_security:
                output[symbol] = rating_by_security[symbol]
                continue
            ticker = symbol.split(":", 1)[1].upper() if ":" in symbol else symbol.upper()
            if ticker in rating_by_ticker:
                output[symbol] = rating_by_ticker[ticker]
        return output

