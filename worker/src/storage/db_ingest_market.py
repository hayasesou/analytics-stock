from __future__ import annotations

from datetime import date
import json

import pandas as pd

from src.storage.db_base import _chunks
from src.types import Security


class NeonRepositoryIngestMarketMixin:
    def upsert_securities(self, securities: list[Security]) -> dict[str, str]:
        if not securities:
            return {}

        values = [
            (
                s.security_id,
                s.market,
                s.ticker,
                s.name,
                s.sector,
                s.industry,
                s.currency,
                json.dumps(s.metadata or {}),
            )
            for s in securities
        ]

        mapping: dict[str, str] = {}
        with self._conn() as conn, conn.cursor() as cur:
            for batch in _chunks(values):
                cur.executemany(
                    """
                    INSERT INTO securities (
                        security_id, market, ticker, name, sector, industry, currency, metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (security_id) DO UPDATE
                    SET market = EXCLUDED.market,
                        ticker = EXCLUDED.ticker,
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        currency = EXCLUDED.currency,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    RETURNING security_id, id::text
                    """,
                    batch,
                    returning=True,
                )
                for row in cur.fetchall():
                    mapping[row["security_id"]] = row["id"]
            conn.commit()

        if len(mapping) < len(securities):
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT security_id, id::text FROM securities WHERE security_id = ANY(%s)",
                    ([s.security_id for s in securities],),
                )
                for row in cur.fetchall():
                    mapping[row["security_id"]] = row["id"]
        return mapping

    def upsert_universe_membership(
        self,
        security_uuid_map: dict[str, str],
        universe: str,
        as_of_date: date,
        source: str,
        reset_existing: bool = True,
    ) -> None:
        values = [(sec_uuid, universe, as_of_date, True, source) for sec_uuid in security_uuid_map.values()]

        with self._conn() as conn, conn.cursor() as cur:
            if reset_existing:
                cur.execute(
                    """
                    UPDATE universe_membership
                    SET is_member = false,
                        source = %s,
                        retrieved_at = NOW()
                    WHERE universe = %s
                      AND as_of_date = %s
                      AND is_member = true
                    """,
                    (source, universe, as_of_date),
                )

            if not values:
                conn.commit()
                return

            for batch in _chunks(values):
                cur.executemany(
                    """
                    INSERT INTO universe_membership (security_id, universe, as_of_date, is_member, source)
                    VALUES (%s::uuid, %s, %s, %s, %s)
                    ON CONFLICT (security_id, universe, as_of_date)
                    DO UPDATE SET is_member = EXCLUDED.is_member,
                                  source = EXCLUDED.source,
                                  retrieved_at = NOW()
                    """,
                    batch,
                )
            conn.commit()

    def upsert_prices(self, prices: pd.DataFrame, security_uuid_map: dict[str, str]) -> None:
        if prices.empty:
            return

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_prices_daily_stage (
                    security_id UUID NOT NULL,
                    trade_date DATE NOT NULL,
                    open_raw NUMERIC(18, 6) NOT NULL,
                    high_raw NUMERIC(18, 6) NOT NULL,
                    low_raw NUMERIC(18, 6) NOT NULL,
                    close_raw NUMERIC(18, 6) NOT NULL,
                    volume BIGINT,
                    adjusted_close NUMERIC(18, 6),
                    adjustment_factor NUMERIC(18, 8) NOT NULL,
                    source TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )

            has_rows = False
            with cur.copy(
                """
                COPY tmp_prices_daily_stage (
                    security_id, trade_date, open_raw, high_raw, low_raw, close_raw,
                    volume, adjusted_close, adjustment_factor, source
                ) FROM STDIN
                """
            ) as copy:
                for row in prices.itertuples(index=False):
                    sec_uuid = security_uuid_map.get(getattr(row, "security_id"))
                    if not sec_uuid:
                        continue

                    close_raw = float(getattr(row, "close_raw"))
                    adjusted_close = getattr(row, "adjusted_close", close_raw)
                    adjustment_factor = getattr(row, "adjustment_factor", 1.0)
                    source = getattr(row, "source", "unknown")

                    copy.write_row(
                        (
                            sec_uuid,
                            getattr(row, "trade_date"),
                            float(getattr(row, "open_raw")),
                            float(getattr(row, "high_raw")),
                            float(getattr(row, "low_raw")),
                            close_raw,
                            int(getattr(row, "volume")),
                            float(adjusted_close),
                            float(adjustment_factor),
                            str(source),
                        )
                    )
                    has_rows = True

            if not has_rows:
                conn.rollback()
                return

            cur.execute(
                """
                INSERT INTO prices_daily (
                    security_id, trade_date, open_raw, high_raw, low_raw, close_raw,
                    volume, adjusted_close, adjustment_factor, source
                )
                SELECT
                    security_id, trade_date, open_raw, high_raw, low_raw, close_raw,
                    volume, adjusted_close, adjustment_factor, source
                FROM tmp_prices_daily_stage
                ON CONFLICT (security_id, trade_date)
                DO UPDATE SET open_raw = EXCLUDED.open_raw,
                              high_raw = EXCLUDED.high_raw,
                              low_raw = EXCLUDED.low_raw,
                              close_raw = EXCLUDED.close_raw,
                              volume = EXCLUDED.volume,
                              adjusted_close = EXCLUDED.adjusted_close,
                              adjustment_factor = EXCLUDED.adjustment_factor,
                              source = EXCLUDED.source,
                              retrieved_at = NOW()
                """
            )
            conn.commit()

    def delete_prices_range(self, security_ids: list[str], start_date: date, end_date: date) -> None:
        if not security_ids:
            return

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM prices_daily
                WHERE security_id = ANY(%s::uuid[])
                  AND trade_date BETWEEN %s AND %s
                """,
                (security_ids, start_date, end_date),
            )
            conn.commit()

    def upsert_fx(self, fx_df: pd.DataFrame) -> None:
        if fx_df.empty:
            return

        rows = [(row["pair"], row["trade_date"], float(row["rate"]), row.get("source", "unknown")) for _, row in fx_df.iterrows()]

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO fx_rates_daily (pair, trade_date, rate, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pair, trade_date)
                DO UPDATE SET rate = EXCLUDED.rate,
                              source = EXCLUDED.source,
                              retrieved_at = NOW()
                """,
                rows,
            )
            conn.commit()
