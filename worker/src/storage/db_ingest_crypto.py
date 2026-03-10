from __future__ import annotations

import json
from typing import Any

from src.types import CryptoDataQualitySnapshot, CryptoMarketSnapshot


class NeonRepositoryIngestCryptoMixin:
    def insert_crypto_market_snapshots(self, snapshots: list[CryptoMarketSnapshot]) -> int:
        if not snapshots:
            return 0

        rows = [
            (
                s.exchange,
                s.symbol,
                s.market_type,
                s.observed_at,
                s.best_bid,
                s.best_ask,
                s.mid,
                s.spread_bps,
                s.funding_rate,
                s.open_interest,
                s.mark_price,
                s.index_price,
                s.basis_bps,
                s.source_mode,
                s.latency_ms,
                json.dumps(s.data_quality or {}),
                json.dumps(s.raw_payload or {}),
            )
            for s in snapshots
        ]

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO crypto_market_snapshots (
                    exchange,
                    symbol,
                    market_type,
                    observed_at,
                    best_bid,
                    best_ask,
                    mid,
                    spread_bps,
                    funding_rate,
                    open_interest,
                    mark_price,
                    index_price,
                    basis_bps,
                    source_mode,
                    latency_ms,
                    data_quality,
                    raw_payload
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb
                )
                ON CONFLICT (exchange, symbol, market_type, observed_at)
                DO UPDATE SET best_bid = EXCLUDED.best_bid,
                              best_ask = EXCLUDED.best_ask,
                              mid = EXCLUDED.mid,
                              spread_bps = EXCLUDED.spread_bps,
                              funding_rate = EXCLUDED.funding_rate,
                              open_interest = EXCLUDED.open_interest,
                              mark_price = EXCLUDED.mark_price,
                              index_price = EXCLUDED.index_price,
                              basis_bps = EXCLUDED.basis_bps,
                              source_mode = EXCLUDED.source_mode,
                              latency_ms = EXCLUDED.latency_ms,
                              data_quality = EXCLUDED.data_quality,
                              raw_payload = EXCLUDED.raw_payload
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def insert_crypto_data_quality_snapshots(self, rows: list[CryptoDataQualitySnapshot]) -> int:
        if not rows:
            return 0

        values = [
            (
                row.exchange,
                row.symbol,
                row.market_type,
                row.window_start,
                row.window_end,
                int(row.sample_count),
                int(row.missing_count),
                float(row.missing_ratio),
                row.latency_p95_ms,
                int(row.ws_failover_count),
                bool(row.eligible_for_edge),
                json.dumps(row.details or {}),
            )
            for row in rows
        ]

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO crypto_data_quality (
                    exchange,
                    symbol,
                    market_type,
                    window_start,
                    window_end,
                    sample_count,
                    missing_count,
                    missing_ratio,
                    latency_p95_ms,
                    ws_failover_count,
                    eligible_for_edge,
                    details
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (exchange, symbol, market_type, window_start, window_end)
                DO UPDATE SET sample_count = EXCLUDED.sample_count,
                              missing_count = EXCLUDED.missing_count,
                              missing_ratio = EXCLUDED.missing_ratio,
                              latency_p95_ms = EXCLUDED.latency_p95_ms,
                              ws_failover_count = EXCLUDED.ws_failover_count,
                              eligible_for_edge = EXCLUDED.eligible_for_edge,
                              details = EXCLUDED.details
                """,
                values,
            )
            conn.commit()
        return len(values)

    def fetch_latest_crypto_market_snapshots(
        self,
        exchanges: list[str] | None = None,
        symbols: list[str] | None = None,
        market_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        exchange_filter = [str(x).strip().lower() for x in (exchanges or []) if str(x).strip()]
        symbol_filter = [str(x).strip().upper() for x in (symbols or []) if str(x).strip()]
        market_type_filter = str(market_type).strip().lower() if market_type is not None else None
        if market_type_filter == "":
            market_type_filter = None

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    exchange,
                    symbol,
                    market_type,
                    observed_at,
                    best_bid,
                    best_ask,
                    mid,
                    spread_bps,
                    funding_rate,
                    open_interest,
                    mark_price,
                    index_price,
                    basis_bps,
                    source_mode,
                    latency_ms,
                    data_quality,
                    raw_payload,
                    created_at
                FROM crypto_market_snapshots
                WHERE (%s::text[] IS NULL OR exchange = ANY(%s))
                  AND (%s::text[] IS NULL OR symbol = ANY(%s))
                  AND (%s::text IS NULL OR market_type = %s)
                ORDER BY observed_at DESC, exchange, symbol, market_type
                LIMIT %s
                """,
                (
                    exchange_filter if exchange_filter else None,
                    exchange_filter if exchange_filter else None,
                    symbol_filter if symbol_filter else None,
                    symbol_filter if symbol_filter else None,
                    market_type_filter,
                    market_type_filter,
                    max(1, int(limit)),
                ),
            )
            rows = cur.fetchall()
        return rows

    def fetch_crypto_market_inputs_for_edge(
        self,
        max_missing_ratio: float = 0.25,
        max_latency_ms: float = 3000.0,
        lookback_minutes: int = 60,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_snapshots AS (
                    SELECT DISTINCT ON (exchange, symbol, market_type)
                        id::text AS id,
                        exchange,
                        symbol,
                        market_type,
                        observed_at,
                        best_bid,
                        best_ask,
                        mid,
                        spread_bps,
                        funding_rate,
                        open_interest,
                        mark_price,
                        index_price,
                        basis_bps,
                        source_mode,
                        latency_ms,
                        data_quality,
                        raw_payload,
                        created_at
                    FROM crypto_market_snapshots
                    WHERE observed_at >= NOW() - (%s::int * INTERVAL '1 minute')
                    ORDER BY exchange, symbol, market_type, observed_at DESC
                ),
                latest_quality AS (
                    SELECT DISTINCT ON (exchange, symbol, market_type)
                        exchange,
                        symbol,
                        market_type,
                        window_end,
                        sample_count,
                        missing_count,
                        missing_ratio,
                        latency_p95_ms,
                        ws_failover_count,
                        eligible_for_edge,
                        details
                    FROM crypto_data_quality
                    WHERE window_end >= NOW() - (%s::int * INTERVAL '1 minute')
                    ORDER BY exchange, symbol, market_type, window_end DESC
                )
                SELECT
                    ls.*,
                    lq.window_end,
                    lq.sample_count,
                    lq.missing_count,
                    lq.missing_ratio,
                    lq.latency_p95_ms,
                    lq.ws_failover_count,
                    lq.eligible_for_edge,
                    lq.details AS quality_details
                FROM latest_snapshots ls
                LEFT JOIN latest_quality lq
                  ON lq.exchange = ls.exchange
                 AND lq.symbol = ls.symbol
                 AND lq.market_type = ls.market_type
                WHERE COALESCE(lq.missing_ratio, 0.0) <= %s
                  AND COALESCE(lq.latency_p95_ms, 0.0) <= %s
                  AND COALESCE(lq.eligible_for_edge, TRUE) = TRUE
                ORDER BY ls.observed_at DESC, ls.exchange, ls.symbol, ls.market_type
                LIMIT %s
                """,
                (
                    max(1, int(lookback_minutes)),
                    max(1, int(lookback_minutes)),
                    float(max_missing_ratio),
                    float(max_latency_ms),
                    max(1, int(limit)),
                ),
            )
            rows = cur.fetchall()
        return rows
