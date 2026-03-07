from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Iterator
from uuid import uuid4

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.types import (
    BacktestResult,
    CitationItem,
    CryptoDataQualitySnapshot,
    CryptoMarketSnapshot,
    EdgeRisk,
    EdgeState,
    ExperimentSpec,
    EventItem,
    FillRecord,
    FundamentalSnapshot,
    IdeaEvidenceSpec,
    IdeaSpec,
    LessonSpec,
    OrderIntent,
    OrderRecord,
    PortfolioSpec,
    ResearchArtifactSpec,
    ResearchArtifactRunSpec,
    ResearchExternalInput,
    ResearchHypothesisOutcomeSpec,
    ResearchHypothesisSpec,
    PositionRecord,
    ReportItem,
    RiskSnapshot,
    Security,
    StrategyEvaluation,
    StrategyLifecycleReview,
    StrategyRiskEvent,
    StrategyRiskSnapshot,
    StrategySpec,
    StrategyVersionSpec,
)


def _chunks(seq: list[Any], size: int = 1000) -> Iterator[list[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _normalize_edge_risk_payload(value: Any) -> dict[str, Any]:
    return EdgeRisk.from_mapping(value).to_dict()


def _merge_edge_risk_payload(
    primary: dict[str, Any],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    keys = set(primary.keys()) | set(fallback.keys())
    for key in keys:
        if key == "extra":
            merged_extra: dict[str, Any] = {}
            if isinstance(fallback.get("extra"), dict):
                merged_extra.update(dict(fallback["extra"]))
            if isinstance(primary.get("extra"), dict):
                merged_extra.update(dict(primary["extra"]))
            output[key] = merged_extra
            continue
        primary_value = primary.get(key)
        fallback_value = fallback.get(key)
        output[key] = primary_value if primary_value is not None else fallback_value
    return output


class NeonRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    @contextmanager
    def _conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            yield conn

    def create_run(self, run_type: str, config_version: str, metadata: dict[str, Any] | None = None) -> str:
        payload = metadata or {}
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runs (run_type, status, config_version, metadata)
                VALUES (%s, 'running', %s, %s)
                RETURNING id::text
                """,
                (run_type, config_version, json.dumps(payload)),
            )
            run_id = cur.fetchone()["id"]
            conn.commit()
        return run_id

    def create_chat_session(self, title: str | None = None) -> str:
        session_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_sessions (id, title)
                VALUES (%s::uuid, %s)
                """,
                (session_id, title),
            )
            conn.commit()
        return session_id

    def finish_run(self, run_id: str, status: str, metadata: dict[str, Any] | None = None) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            if metadata:
                cur.execute(
                    """
                    UPDATE runs
                    SET status = %s,
                        finished_at = NOW(),
                        metadata = metadata || %s::jsonb
                    WHERE id = %s::uuid
                    """,
                    (status, json.dumps(metadata), run_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE runs
                    SET status = %s,
                        finished_at = NOW()
                    WHERE id = %s::uuid
                    """,
                    (status, run_id),
                )
            conn.commit()

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
            # ON CONFLICT + RETURNING は driver 次第で欠落するケースがあるため補完
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
        values = [
            (sec_uuid, universe, as_of_date, True, source)
            for sec_uuid in security_uuid_map.values()
        ]

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

        rows = [
            (r["pair"], r["trade_date"], float(r["rate"]), r.get("source", "unknown"))
            for _, r in fx_df.iterrows()
        ]

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
                r.exchange,
                r.symbol,
                r.market_type,
                r.window_start,
                r.window_end,
                int(r.sample_count),
                int(r.missing_count),
                float(r.missing_ratio),
                r.latency_p95_ms,
                int(r.ws_failover_count),
                bool(r.eligible_for_edge),
                json.dumps(r.details or {}),
            )
            for r in rows
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

    def insert_scores(self, run_id: str, score_df: pd.DataFrame, security_uuid_map: dict[str, str]) -> None:
        if score_df.empty:
            return

        rows = []
        for _, r in score_df.iterrows():
            sec_uuid = security_uuid_map.get(r["security_id"])
            if not sec_uuid:
                continue
            rows.append(
                (
                    run_id,
                    sec_uuid,
                    r["as_of_date"],
                    float(r["quality"]),
                    float(r["growth"]),
                    float(r["value"]),
                    float(r["momentum"]),
                    float(r["catalyst"]),
                    float(r["combined_score"]),
                    float(r["missing_ratio"]),
                    bool(r["liquidity_flag"]),
                    bool(r["exclusion_flag"]),
                    r["confidence"],
                    int(r["market_rank"]),
                    json.dumps(
                        {
                            "edge_score": float(r.get("edge_score", 0.0)),
                        }
                    ),
                )
            )

        with self._conn() as conn, conn.cursor() as cur:
            for batch in _chunks(rows):
                cur.executemany(
                    """
                    INSERT INTO score_snapshots (
                        run_id, security_id, as_of_date, quality, growth, value, momentum, catalyst,
                        combined_score, missing_ratio, liquidity_flag, exclusion_flag, confidence,
                        market_rank, flags
                    )
                    VALUES (
                        %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (run_id, security_id)
                    DO UPDATE SET quality = EXCLUDED.quality,
                                  growth = EXCLUDED.growth,
                                  value = EXCLUDED.value,
                                  momentum = EXCLUDED.momentum,
                                  catalyst = EXCLUDED.catalyst,
                                  combined_score = EXCLUDED.combined_score,
                                  missing_ratio = EXCLUDED.missing_ratio,
                                  liquidity_flag = EXCLUDED.liquidity_flag,
                                  exclusion_flag = EXCLUDED.exclusion_flag,
                                  confidence = EXCLUDED.confidence,
                                  market_rank = EXCLUDED.market_rank,
                                  flags = EXCLUDED.flags
                    """,
                    batch,
                )
            conn.commit()

    def insert_top50(self, run_id: str, top50: pd.DataFrame, security_uuid_map: dict[str, str]) -> None:
        if top50.empty:
            return

        rows = []
        for _, r in top50.iterrows():
            sec_uuid = security_uuid_map.get(r["security_id"])
            if not sec_uuid:
                continue
            rows.append((run_id, sec_uuid, int(r["mixed_rank"]), r.get("selection_reason", "score_rank")))

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM top50_membership WHERE run_id = %s::uuid", (run_id,))
            cur.executemany(
                """
                INSERT INTO top50_membership (run_id, security_id, rank, reason)
                VALUES (%s::uuid, %s::uuid, %s, %s)
                """,
                rows,
            )
            conn.commit()

    def insert_signals(self, run_id: str, signals: pd.DataFrame, security_uuid_map: dict[str, str]) -> None:
        if signals.empty:
            return

        rows = []
        for _, r in signals.iterrows():
            sec_uuid = security_uuid_map.get(r["security_id"])
            if not sec_uuid:
                continue
            rows.append(
                (
                    run_id,
                    sec_uuid,
                    r["as_of_date"],
                    bool(r["is_signal"]),
                    bool(r.get("entry_allowed", False)),
                    r["reason"],
                    int(r["mixed_rank"]),
                    r["confidence"],
                    r["valid_until"],
                )
            )

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO signals (
                    run_id, security_id, as_of_date, is_signal, entry_allowed, reason, rank, confidence, valid_until
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, security_id)
                DO UPDATE SET as_of_date = EXCLUDED.as_of_date,
                              is_signal = EXCLUDED.is_signal,
                              entry_allowed = EXCLUDED.entry_allowed,
                              reason = EXCLUDED.reason,
                              rank = EXCLUDED.rank,
                              confidence = EXCLUDED.confidence,
                              valid_until = EXCLUDED.valid_until
                """,
                rows,
            )
            conn.commit()

    def fetch_signals_for_diagnostics(
        self,
        as_of_date: date,
        lookback_days: int = 730,
    ) -> pd.DataFrame:
        columns = ["security_id", "as_of_date"]
        lookback = max(1, int(lookback_days))
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sec.security_id,
                    s.as_of_date
                FROM signals s
                JOIN securities sec
                  ON sec.id = s.security_id
                JOIN runs r
                  ON r.id = s.run_id
                WHERE s.is_signal = TRUE
                  AND r.run_type = 'weekly'
                  AND s.as_of_date >= %s::date - (%s::int * INTERVAL '1 day')
                  AND s.as_of_date <= %s::date
                ORDER BY s.as_of_date ASC
                """,
                (as_of_date, lookback, as_of_date),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(rows).reindex(columns=columns)

    def upsert_signal_diagnostics_weekly(
        self,
        run_id: str,
        diagnostics: list[dict[str, Any]],
    ) -> None:
        if not diagnostics:
            return

        rows: list[tuple[Any, ...]] = []
        for d in diagnostics:
            rows.append(
                (
                    run_id,
                    int(d["horizon_days"]),
                    float(d["hit_rate"]),
                    float(d["median_return"]) if d.get("median_return") is not None else None,
                    float(d["p10_return"]) if d.get("p10_return") is not None else None,
                    float(d["p90_return"]) if d.get("p90_return") is not None else None,
                    int(d.get("sample_size", 0)),
                )
            )

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO signal_diagnostics_weekly (
                    run_id, horizon_days, hit_rate, median_return, p10_return, p90_return, sample_size
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, horizon_days)
                DO UPDATE SET hit_rate = EXCLUDED.hit_rate,
                              median_return = EXCLUDED.median_return,
                              p10_return = EXCLUDED.p10_return,
                              p90_return = EXCLUDED.p90_return,
                              sample_size = EXCLUDED.sample_size
                """,
                rows,
            )
            conn.commit()

    def _ensure_document_version(self, cur: psycopg.Cursor, doc_version_id: str, source_url: str | None = None) -> None:
        ext_doc_id = doc_version_id
        src = source_url or "https://example.com/evidence"
        cur.execute(
            """
            INSERT INTO documents (external_doc_id, source_system, source_url, title, published_at)
            VALUES (%s, 'mock', %s, %s, NOW())
            ON CONFLICT (source_system, external_doc_id)
            DO UPDATE SET source_url = EXCLUDED.source_url
            RETURNING id::text
            """,
            (ext_doc_id, src, f"Evidence {ext_doc_id[:8]}"),
        )
        doc_id = cur.fetchone()["id"]

        sha = hashlib.sha256(doc_version_id.encode("utf-8")).hexdigest()
        cur.execute(
            """
            INSERT INTO document_versions (
                id, document_id, retrieved_at, sha256, mime_type, r2_object_key, r2_text_key, page_count
            )
            VALUES (%s::uuid, %s::uuid, NOW(), %s, 'text/plain', %s, %s, 1)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                doc_version_id,
                doc_id,
                sha,
                f"mock/evidence/{doc_version_id}.txt",
                f"mock/evidence/{doc_version_id}.chunk.txt",
            ),
        )

    def upsert_document_with_version(
        self,
        *,
        external_doc_id: str,
        source_system: str,
        source_url: str,
        title: str | None,
        published_at: datetime | None,
        retrieved_at: datetime | None,
        sha256: str,
        mime_type: str,
        r2_object_key: str,
        r2_text_key: str | None = None,
        page_count: int | None = None,
    ) -> str:
        normalized_sha = str(sha256).strip().lower()
        if len(normalized_sha) != 64:
            raise ValueError("sha256 must be a 64-char hex string")

        fetched_at = retrieved_at or datetime.utcnow()
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (
                    external_doc_id, source_system, source_url, title, published_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_system, external_doc_id)
                DO UPDATE SET source_url = EXCLUDED.source_url,
                              title = COALESCE(EXCLUDED.title, documents.title),
                              published_at = COALESCE(EXCLUDED.published_at, documents.published_at)
                RETURNING id::text
                """,
                (
                    external_doc_id,
                    source_system,
                    source_url,
                    title,
                    published_at,
                ),
            )
            doc_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO document_versions (
                    document_id, retrieved_at, sha256, mime_type, r2_object_key, r2_text_key, page_count
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, sha256)
                DO UPDATE SET retrieved_at = EXCLUDED.retrieved_at,
                              mime_type = EXCLUDED.mime_type,
                              r2_object_key = EXCLUDED.r2_object_key,
                              r2_text_key = EXCLUDED.r2_text_key,
                              page_count = EXCLUDED.page_count
                RETURNING id::text
                """,
                (
                    doc_id,
                    fetched_at,
                    normalized_sha,
                    mime_type,
                    r2_object_key,
                    r2_text_key,
                    page_count,
                ),
            )
            doc_version_id = cur.fetchone()["id"]
            conn.commit()

        return doc_version_id

    def insert_events(
        self,
        run_id: str,
        events: list[EventItem],
        security_uuid_map: dict[str, str],
    ) -> None:
        if not events:
            return

        with self._conn() as conn, conn.cursor() as cur:
            for e in events:
                sec_uuid = security_uuid_map.get(e.security_id) if e.security_id else None
                doc_version_id = e.doc_version_id
                if doc_version_id:
                    self._ensure_document_version(cur, doc_version_id, e.source_url)

                cur.execute(
                    """
                    INSERT INTO events (
                        run_id, security_id, event_type, importance, event_time,
                        title, summary, source_url, doc_version_id, metadata
                    )
                    VALUES (
                        %s::uuid, %s::uuid, %s, %s, %s,
                        %s, %s, %s, %s::uuid, %s::jsonb
                    )
                    """,
                    (
                        run_id,
                        sec_uuid,
                        e.event_type,
                        e.importance,
                        e.event_time,
                        e.title,
                        e.summary,
                        e.source_url,
                        doc_version_id,
                        json.dumps(e.metadata),
                    ),
                )
            conn.commit()

    def get_evidence_stats(
        self,
        security_ids: list[str],
        lookback_days: int = 30,
    ) -> pd.DataFrame:
        columns = [
            "security_id",
            "primary_source_count",
            "has_key_numbers",
            "has_major_contradiction",
            "catalyst_bonus",
        ]
        if not security_ids:
            return pd.DataFrame(columns=columns)

        lookback = max(1, int(lookback_days))
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH target AS (
                    SELECT id, security_id
                    FROM securities
                    WHERE security_id = ANY(%s)
                ),
                citation_stats AS (
                    SELECT
                        r.security_id,
                        COUNT(DISTINCT c.doc_version_id) AS primary_source_count,
                        BOOL_OR(c.quote_text ~ '[0-9]') AS has_key_numbers
                    FROM reports r
                    JOIN report_claims rc
                      ON rc.report_id = r.id
                    JOIN citations c
                      ON c.report_id = rc.report_id
                     AND c.claim_id = rc.claim_id
                    WHERE r.security_id IN (SELECT id FROM target)
                      AND r.created_at >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY r.security_id
                ),
                contradiction_stats AS (
                    SELECT
                        e.security_id,
                        BOOL_OR(
                            e.title ~* '(下方修正|訂正|撤回)'
                            OR COALESCE(e.summary, '') ~* '(下方修正|訂正|撤回)'
                        ) AS has_major_contradiction
                    FROM events e
                    WHERE e.security_id IN (SELECT id FROM target)
                      AND e.event_time >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY e.security_id
                ),
                catalyst_stats AS (
                    SELECT
                        e.security_id,
                        COUNT(*) FILTER (WHERE e.importance = 'high') AS high_count,
                        COUNT(*) FILTER (WHERE e.importance = 'medium') AS medium_count
                    FROM events e
                    WHERE e.security_id IN (SELECT id FROM target)
                      AND e.event_time >= NOW() - (%s::int * INTERVAL '1 day')
                    GROUP BY e.security_id
                )
                SELECT
                    t.security_id,
                    COALESCE(cs.primary_source_count, 0)::int AS primary_source_count,
                    COALESCE(cs.has_key_numbers, FALSE) AS has_key_numbers,
                    COALESCE(ct.has_major_contradiction, FALSE) AS has_major_contradiction,
                    LEAST(
                        COALESCE(cat.high_count, 0) * 0.15 + COALESCE(cat.medium_count, 0) * 0.05,
                        0.30
                    )::double precision AS catalyst_bonus
                FROM target t
                LEFT JOIN citation_stats cs
                    ON cs.security_id = t.id
                LEFT JOIN contradiction_stats ct
                    ON ct.security_id = t.id
                LEFT JOIN catalyst_stats cat
                    ON cat.security_id = t.id
                ORDER BY t.security_id
                """,
                (security_ids, lookback, lookback, lookback),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(rows).reindex(columns=columns)

    def get_recent_citations_by_security(
        self,
        security_ids: list[str],
        lookback_days: int = 30,
        per_security_limit: int = 3,
    ) -> dict[str, list[CitationItem]]:
        if not security_ids:
            return {}

        lookback = max(1, int(lookback_days))
        limit = max(1, int(per_security_limit))
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH target AS (
                    SELECT id, security_id
                    FROM securities
                    WHERE security_id = ANY(%s)
                ),
                ranked AS (
                    SELECT
                        t.security_id,
                        c.claim_id,
                        c.doc_version_id::text AS doc_version_id,
                        c.page_ref,
                        c.quote_text,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.id
                            ORDER BY r.created_at DESC, c.created_at DESC
                        ) AS row_num
                    FROM target t
                    JOIN reports r
                      ON r.security_id = t.id
                    JOIN citations c
                      ON c.report_id = r.id
                    WHERE r.created_at >= NOW() - (%s::int * INTERVAL '1 day')
                )
                SELECT security_id, claim_id, doc_version_id, page_ref, quote_text
                FROM ranked
                WHERE row_num <= %s
                ORDER BY security_id, row_num
                """,
                (security_ids, lookback, limit),
            )
            rows = cur.fetchall()

        result: dict[str, list[CitationItem]] = {}
        for row in rows:
            result.setdefault(row["security_id"], []).append(
                CitationItem(
                    claim_id=row["claim_id"],
                    doc_version_id=row["doc_version_id"],
                    page_ref=row["page_ref"],
                    quote_text=row["quote_text"],
                )
            )
        return result

    def insert_report(self, run_id: str, report: ReportItem, security_uuid_map: dict[str, str]) -> str:
        sec_uuid = security_uuid_map.get(report.security_id) if report.security_id else None
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reports (
                    run_id, security_id, report_type, title, body_md,
                    conclusion, falsification_conditions, confidence
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
                RETURNING id::text
                """,
                (
                    run_id,
                    sec_uuid,
                    report.report_type,
                    report.title,
                    report.body_md,
                    report.conclusion,
                    report.falsification_conditions,
                    report.confidence,
                ),
            )
            report_id = cur.fetchone()["id"]

            for c in report.claims:
                cur.execute(
                    """
                    INSERT INTO report_claims (report_id, claim_id, claim_text, claim_type, status)
                    VALUES (%s::uuid, %s, %s, 'important', %s)
                    ON CONFLICT (report_id, claim_id)
                    DO UPDATE SET claim_text = EXCLUDED.claim_text,
                                  status = EXCLUDED.status
                    """,
                    (report_id, c["claim_id"], c["claim_text"], c.get("status", "supported")),
                )

            for citation in report.citations:
                self._ensure_document_version(cur, citation.doc_version_id)
                cur.execute(
                    """
                    INSERT INTO citations (
                        report_id, claim_id, doc_version_id, page_ref, quote_text, locator
                    )
                    VALUES (%s::uuid, %s, %s::uuid, %s, %s, %s::jsonb)
                    """,
                    (
                        report_id,
                        citation.claim_id,
                        citation.doc_version_id,
                        citation.page_ref,
                        citation.quote_text,
                        json.dumps({"source": "worker"}),
                    ),
                )

            conn.commit()
        return report_id

    def insert_reports_bulk(
        self,
        run_id: str,
        reports: list[ReportItem],
        security_uuid_map: dict[str, str],
        batch_size: int = 10,
    ) -> list[str]:
        if not reports:
            return []

        report_ids: list[str] = []
        with self._conn() as conn, conn.cursor() as cur:
            for batch in _chunks(reports, size=batch_size):
                current_ids = [str(uuid4()) for _ in batch]
                report_rows: list[tuple[Any, ...]] = []
                claim_rows: list[tuple[Any, ...]] = []
                citation_rows: list[tuple[Any, ...]] = []
                doc_sources: dict[str, str | None] = {}

                for report_id, report in zip(current_ids, batch, strict=True):
                    sec_uuid = security_uuid_map.get(report.security_id) if report.security_id else None
                    report_rows.append(
                        (
                            report_id,
                            run_id,
                            sec_uuid,
                            report.report_type,
                            report.title,
                            report.body_md,
                            report.conclusion,
                            report.falsification_conditions,
                            report.confidence,
                        )
                    )

                    for claim in report.claims:
                        claim_rows.append(
                            (
                                report_id,
                                claim["claim_id"],
                                claim["claim_text"],
                                claim.get("status", "supported"),
                            )
                        )

                    for citation in report.citations:
                        if citation.doc_version_id not in doc_sources:
                            doc_sources[citation.doc_version_id] = None
                        citation_rows.append(
                            (
                                report_id,
                                citation.claim_id,
                                citation.doc_version_id,
                                citation.page_ref,
                                citation.quote_text,
                                json.dumps({"source": "worker"}),
                            )
                        )

                for doc_version_id, source_url in doc_sources.items():
                    self._ensure_document_version(cur, doc_version_id, source_url)

                cur.executemany(
                    """
                    INSERT INTO reports (
                        id, run_id, security_id, report_type, title, body_md,
                        conclusion, falsification_conditions, confidence
                    )
                    VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
                    """,
                    report_rows,
                )

                if claim_rows:
                    cur.executemany(
                        """
                        INSERT INTO report_claims (report_id, claim_id, claim_text, claim_type, status)
                        VALUES (%s::uuid, %s, %s, 'important', %s)
                        ON CONFLICT (report_id, claim_id)
                        DO UPDATE SET claim_text = EXCLUDED.claim_text,
                                      status = EXCLUDED.status
                        """,
                        claim_rows,
                    )

                if citation_rows:
                    cur.executemany(
                        """
                        INSERT INTO citations (
                            report_id, claim_id, doc_version_id, page_ref, quote_text, locator
                        )
                        VALUES (%s::uuid, %s, %s::uuid, %s, %s, %s::jsonb)
                        """,
                        citation_rows,
                    )

                conn.commit()
                report_ids.extend(current_ids)

        return report_ids

    def create_backtest_run(
        self,
        run_id: str,
        as_of_date: date,
        period_start: date,
        period_end: date,
        common_period_start: date,
        common_period_end: date,
    ) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO backtest_runs (
                    run_id, as_of_date, period_start, period_end, common_period_start, common_period_end
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s)
                RETURNING id::text
                """,
                (
                    run_id,
                    as_of_date,
                    period_start,
                    period_end,
                    common_period_start,
                    common_period_end,
                ),
            )
            backtest_run_id = cur.fetchone()["id"]
            conn.commit()
        return backtest_run_id

    def insert_backtest_results(
        self,
        backtest_run_id: str,
        results: list[BacktestResult],
        security_uuid_map: dict[str, str],
    ) -> None:
        if not results:
            return

        with self._conn() as conn, conn.cursor() as cur:
            for result in results:
                cur.execute(
                    """
                    INSERT INTO backtest_metrics (
                        backtest_run_id, cost_profile, market_scope,
                        cagr, max_dd, sharpe, sortino, volatility,
                        win_rate, avg_win, avg_loss, alpha_simple, information_ratio_simple
                    )
                    VALUES (
                        %s::uuid, %s, 'MIXED',
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (backtest_run_id, cost_profile, market_scope)
                    DO UPDATE SET cagr = EXCLUDED.cagr,
                                  max_dd = EXCLUDED.max_dd,
                                  sharpe = EXCLUDED.sharpe,
                                  sortino = EXCLUDED.sortino,
                                  volatility = EXCLUDED.volatility,
                                  win_rate = EXCLUDED.win_rate,
                                  avg_win = EXCLUDED.avg_win,
                                  avg_loss = EXCLUDED.avg_loss,
                                  alpha_simple = EXCLUDED.alpha_simple,
                                  information_ratio_simple = EXCLUDED.information_ratio_simple
                    """,
                    (
                        backtest_run_id,
                        result.cost_profile,
                        result.metrics.get("cagr"),
                        result.metrics.get("max_dd"),
                        result.metrics.get("sharpe"),
                        result.metrics.get("sortino"),
                        result.metrics.get("volatility"),
                        result.metrics.get("win_rate"),
                        result.metrics.get("avg_win"),
                        result.metrics.get("avg_loss"),
                        result.metrics.get("alpha_simple"),
                        result.metrics.get("information_ratio_simple"),
                    ),
                )

                curve_rows = [
                    (
                        backtest_run_id,
                        result.cost_profile,
                        p["trade_date"],
                        p["equity"],
                        p["benchmark_equity"],
                    )
                    for p in result.equity_curve
                ]

                if curve_rows:
                    cur.executemany(
                        """
                        INSERT INTO backtest_equity_curve (
                            backtest_run_id, cost_profile, trade_date, equity, benchmark_equity
                        )
                        VALUES (%s::uuid, %s, %s, %s, %s)
                        ON CONFLICT (backtest_run_id, cost_profile, trade_date)
                        DO UPDATE SET equity = EXCLUDED.equity,
                                      benchmark_equity = EXCLUDED.benchmark_equity
                        """,
                        curve_rows,
                    )

                trade_rows = []
                for t in result.trades:
                    sec_uuid = security_uuid_map.get(t.security_id)
                    trade_rows.append(
                        (
                            backtest_run_id,
                            result.cost_profile,
                            sec_uuid,
                            t.market,
                            t.entry_date,
                            t.entry_price,
                            t.exit_date,
                            t.exit_price,
                            t.quantity,
                            t.gross_pnl,
                            t.net_pnl,
                            t.cost,
                            t.exit_reason,
                            json.dumps({}),
                        )
                    )

                if trade_rows:
                    cur.executemany(
                        """
                        INSERT INTO backtest_trades (
                            backtest_run_id, cost_profile, security_id, market,
                            entry_date, entry_price, exit_date, exit_price,
                            quantity, gross_pnl, net_pnl, cost, exit_reason, meta
                        )
                        VALUES (
                            %s::uuid, %s, %s::uuid, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s::jsonb
                        )
                        """,
                        trade_rows,
                    )

            conn.commit()

    def upsert_strategy(self, strategy: StrategySpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategies (name, description, asset_scope, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name)
                DO UPDATE SET description = EXCLUDED.description,
                              asset_scope = EXCLUDED.asset_scope,
                              status = CASE
                                  WHEN strategies.status IN ('approved', 'paper', 'live', 'paused', 'retired')
                                       AND EXCLUDED.status IN ('draft', 'candidate')
                                  THEN strategies.status
                                  ELSE EXCLUDED.status
                              END,
                              updated_at = NOW()
                RETURNING id::text
                """,
                (
                    strategy.name,
                    strategy.description,
                    strategy.asset_scope,
                    strategy.status,
                ),
            )
            strategy_id = cur.fetchone()["id"]
            conn.commit()
        return strategy_id

    def fetch_strategies_for_lifecycle(
        self,
        statuses: list[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        status_values = statuses or ["candidate", "approved", "paper"]
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_versions AS (
                    SELECT DISTINCT ON (sv.strategy_id)
                        sv.strategy_id,
                        sv.id::text AS strategy_version_id,
                        sv.version
                    FROM strategy_versions sv
                    ORDER BY sv.strategy_id, sv.version DESC
                )
                SELECT
                    s.id::text AS strategy_id,
                    s.name AS strategy_name,
                    s.asset_scope,
                    s.status,
                    s.live_candidate,
                    lv.strategy_version_id,
                    lv.version,
                    s.updated_at
                FROM strategies s
                LEFT JOIN latest_versions lv
                  ON lv.strategy_id = s.id
                WHERE s.status = ANY(%s::text[])
                ORDER BY s.updated_at DESC
                LIMIT %s
                """,
                (status_values, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def fetch_strategy_paper_metrics(
        self,
        strategy_version_id: str,
        lookback_days: int = 365,
    ) -> dict[str, Any]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH intents AS (
                    SELECT
                        oi.as_of,
                        oi.as_of::date AS intent_day,
                        oi.status
                    FROM order_intents oi
                    WHERE oi.strategy_version_id = %s::uuid
                      AND oi.as_of >= NOW() - make_interval(days => %s)
                      AND oi.status IN ('sent', 'done')
                ),
                risk AS (
                    SELECT
                        srs.as_of,
                        srs.drawdown,
                        srs.sharpe_20d
                    FROM strategy_risk_snapshots srs
                    WHERE srs.strategy_version_id = %s::uuid
                      AND srs.as_of >= NOW() - make_interval(days => %s)
                ),
                latest_risk AS (
                    SELECT
                        sharpe_20d
                    FROM risk
                    ORDER BY as_of DESC
                    LIMIT 1
                )
                SELECT
                    COALESCE(COUNT(DISTINCT intents.intent_day), 0)::int AS paper_days,
                    COALESCE(COUNT(*) FILTER (WHERE intents.status = 'done'), 0)::int AS round_trips,
                    MIN(intents.as_of) AS first_intent_at,
                    MAX(intents.as_of) AS last_intent_at,
                    (SELECT MIN(drawdown) FROM risk WHERE drawdown IS NOT NULL) AS max_drawdown,
                    (SELECT sharpe_20d FROM latest_risk) AS sharpe_20d
                FROM intents
                """,
                (
                    strategy_version_id,
                    max(1, int(lookback_days)),
                    strategy_version_id,
                    max(1, int(lookback_days)),
                ),
            )
            row = cur.fetchone()
        return row or {
            "paper_days": 0,
            "round_trips": 0,
            "first_intent_at": None,
            "last_intent_at": None,
            "max_drawdown": None,
            "sharpe_20d": None,
        }

    def update_strategy_lifecycle_state(
        self,
        *,
        strategy_id: str,
        status: str,
        live_candidate: bool,
    ) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE strategies
                SET status = %s,
                    live_candidate = %s,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                (status, live_candidate, strategy_id),
            )
            conn.commit()

    def insert_strategy_lifecycle_review(self, review: StrategyLifecycleReview) -> str:
        acted_at = review.acted_at or datetime.now(timezone.utc)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_lifecycle_reviews (
                    strategy_id,
                    strategy_version_id,
                    action,
                    from_status,
                    to_status,
                    live_candidate,
                    reason,
                    recheck_condition,
                    recheck_after,
                    acted_by,
                    acted_at,
                    metadata
                )
                VALUES (
                    %s::uuid,
                    %s::uuid,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb
                )
                RETURNING id::text
                """,
                (
                    review.strategy_id,
                    review.strategy_version_id,
                    review.action,
                    review.from_status,
                    review.to_status,
                    review.live_candidate,
                    review.reason,
                    review.recheck_condition,
                    review.recheck_after,
                    review.acted_by,
                    acted_at,
                    json.dumps(review.metadata),
                ),
            )
            review_id = cur.fetchone()["id"]
            conn.commit()
        return review_id

    def upsert_strategy_version(self, version: StrategyVersionSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategies (name, asset_scope, status)
                VALUES (%s, %s, 'draft')
                ON CONFLICT (name)
                DO UPDATE SET updated_at = NOW()
                RETURNING id::text
                """,
                (version.strategy_name, version.spec.get("asset_scope", "MIXED")),
            )
            strategy_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO strategy_versions (
                    strategy_id, version, spec, code_artifact_key, sha256, created_by,
                    approved_by, approved_at, is_active
                )
                VALUES (%s::uuid, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (strategy_id, version)
                DO UPDATE SET spec = EXCLUDED.spec,
                              code_artifact_key = EXCLUDED.code_artifact_key,
                              sha256 = EXCLUDED.sha256,
                              created_by = EXCLUDED.created_by,
                              approved_by = EXCLUDED.approved_by,
                              approved_at = EXCLUDED.approved_at,
                              is_active = EXCLUDED.is_active
                RETURNING id::text
                """,
                (
                    strategy_id,
                    version.version,
                    json.dumps(version.spec),
                    version.code_artifact_key,
                    version.sha256,
                    version.created_by,
                    version.approved_by,
                    version.approved_at,
                    version.is_active,
                ),
            )
            strategy_version_id = cur.fetchone()["id"]

            if version.is_active:
                cur.execute(
                    """
                    UPDATE strategy_versions
                    SET is_active = (id = %s::uuid)
                    WHERE strategy_id = %s::uuid
                    """,
                    (strategy_version_id, strategy_id),
                )

            conn.commit()
        return strategy_version_id

    def insert_strategy_evaluation(self, evaluation: StrategyEvaluation) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_evaluations (
                    strategy_version_id, eval_type, period_start, period_end, metrics, artifacts
                )
                VALUES (%s::uuid, %s, %s, %s, %s::jsonb, %s::jsonb)
                RETURNING id::text
                """,
                (
                    evaluation.strategy_version_id,
                    evaluation.eval_type,
                    evaluation.period_start,
                    evaluation.period_end,
                    json.dumps(evaluation.metrics),
                    json.dumps(evaluation.artifacts),
                ),
            )
            evaluation_id = cur.fetchone()["id"]
            conn.commit()
        return evaluation_id

    def insert_edge_states(self, states: list[EdgeState]) -> int:
        if not states:
            return 0

        rows = []
        for state in states:
            expected_net_edge = state.expected_net_edge
            if expected_net_edge is None:
                expected_net_edge = state.expected_net_edge_bps

            distance_to_entry = state.distance_to_entry
            if distance_to_entry is None:
                distance_to_entry = state.distance_to_entry_bps

            risk_payload = _merge_edge_risk_payload(
                _normalize_edge_risk_payload(state.risk_json),
                _normalize_edge_risk_payload(state.risk),
            )
            market_regime = state.market_regime or state.market_scope

            rows.append(
                (
                    state.strategy_name,
                    state.strategy_version_id,
                    state.market_scope,
                    state.symbol,
                    state.observed_at,
                    float(state.edge_score),
                    float(expected_net_edge) if expected_net_edge is not None else None,
                    float(distance_to_entry) if distance_to_entry is not None else None,
                    float(expected_net_edge) if expected_net_edge is not None else None,
                    float(distance_to_entry) if distance_to_entry is not None else None,
                    float(state.confidence) if state.confidence is not None else None,
                    json.dumps(risk_payload),
                    json.dumps(risk_payload),
                    state.explain,
                    market_regime,
                    json.dumps(state.meta or {}),
                )
            )

        with self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO edge_states (
                    strategy_name,
                    strategy_version_id,
                    market_scope,
                    symbol,
                    observed_at,
                    edge_score,
                    expected_net_edge,
                    distance_to_entry,
                    expected_net_edge_bps,
                    distance_to_entry_bps,
                    confidence,
                    risk_json,
                    risk,
                    explain,
                    market_regime,
                    meta
                )
                VALUES (
                    %s,
                    %s::uuid,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    %s::jsonb,
                    %s,
                    %s,
                    %s::jsonb
                )
                ON CONFLICT (strategy_name, market_scope, symbol, observed_at)
                DO UPDATE SET strategy_version_id = EXCLUDED.strategy_version_id,
                              edge_score = EXCLUDED.edge_score,
                              expected_net_edge = EXCLUDED.expected_net_edge,
                              distance_to_entry = EXCLUDED.distance_to_entry,
                              expected_net_edge_bps = EXCLUDED.expected_net_edge_bps,
                              distance_to_entry_bps = EXCLUDED.distance_to_entry_bps,
                              confidence = EXCLUDED.confidence,
                              risk_json = EXCLUDED.risk_json,
                              risk = EXCLUDED.risk,
                              explain = EXCLUDED.explain,
                              market_regime = EXCLUDED.market_regime,
                              meta = EXCLUDED.meta
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def create_idea(self, idea: IdeaSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ideas (
                    source_type, source_url, title, raw_text, status, priority, created_by, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    idea.source_type,
                    idea.source_url,
                    idea.title,
                    idea.raw_text,
                    idea.status,
                    int(idea.priority),
                    idea.created_by,
                    json.dumps(idea.metadata),
                ),
            )
            idea_id = cur.fetchone()["id"]
            conn.commit()
        return idea_id

    def fetch_idea_claim_hashes_by_source_url(
        self,
        *,
        source_type: str,
        source_url: str,
        limit: int = 1000,
    ) -> set[str]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    metadata->>'claim_hash' AS claim_hash
                FROM ideas
                WHERE source_type = %s
                  AND source_url = %s
                  AND metadata ? 'claim_hash'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (source_type, source_url, max(1, int(limit))),
            )
            rows = cur.fetchall()

        output: set[str] = set()
        for row in rows:
            raw_value = (row or {}).get("claim_hash")
            if raw_value is None:
                continue
            value = str(raw_value).strip().lower()
            if value:
                output.add(value)
        return output

    def fetch_idea(self, idea_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    source_type,
                    source_url,
                    title,
                    raw_text,
                    status,
                    priority,
                    created_by,
                    metadata,
                    created_at,
                    updated_at
                FROM ideas
                WHERE id = %s::uuid
                LIMIT 1
                """,
                (idea_id,),
            )
            row = cur.fetchone()
        return row

    def fetch_research_kanban_counts(self, statuses: list[str] | None = None) -> dict[str, int]:
        lanes = statuses or ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        normalized_lanes = [str(status).strip() for status in lanes if str(status).strip()]
        if not normalized_lanes:
            normalized_lanes = ["new", "analyzing", "rejected", "candidate", "paper", "live"]

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH lane_rows AS (
                    SELECT status AS lane
                    FROM ideas
                    WHERE status = ANY(%s::text[])
                    UNION ALL
                    SELECT status AS lane
                    FROM strategies
                    WHERE status = ANY(%s::text[])
                )
                SELECT lane, COUNT(*)::int AS cnt
                FROM lane_rows
                GROUP BY lane
                """,
                (normalized_lanes, normalized_lanes),
            )
            rows = cur.fetchall()

        output = {lane: 0 for lane in normalized_lanes}
        for row in rows:
            lane = str((row or {}).get("lane", "")).strip()
            if not lane:
                continue
            try:
                output[lane] = int((row or {}).get("cnt", 0) or 0)
            except (TypeError, ValueError):
                output[lane] = 0
        return output

    def fetch_research_kanban_samples(
        self,
        *,
        statuses: list[str] | None = None,
        limit_per_lane: int = 3,
    ) -> dict[str, list[str]]:
        lanes = statuses or ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        normalized_lanes = [str(status).strip() for status in lanes if str(status).strip()]
        if not normalized_lanes:
            normalized_lanes = ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        lane_limit = max(1, int(limit_per_lane))

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH idea_ranked AS (
                    SELECT
                        status AS lane,
                        title AS item_title,
                        ROW_NUMBER() OVER (PARTITION BY status ORDER BY priority DESC, created_at DESC) AS rn
                    FROM ideas
                    WHERE status = ANY(%s::text[])
                ),
                strategy_ranked AS (
                    SELECT
                        status AS lane,
                        name AS item_title,
                        ROW_NUMBER() OVER (PARTITION BY status ORDER BY updated_at DESC, created_at DESC) AS rn
                    FROM strategies
                    WHERE status = ANY(%s::text[])
                ),
                merged AS (
                    SELECT lane, item_title, rn FROM idea_ranked
                    UNION ALL
                    SELECT lane, item_title, rn FROM strategy_ranked
                )
                SELECT lane, item_title
                FROM merged
                WHERE rn <= %s
                ORDER BY lane, rn
                """,
                (normalized_lanes, normalized_lanes, lane_limit),
            )
            rows = cur.fetchall()

        output = {lane: [] for lane in normalized_lanes}
        for row in rows:
            lane = str((row or {}).get("lane", "")).strip()
            if not lane:
                continue
            title = str((row or {}).get("item_title", "")).strip()
            if not title:
                continue
            if lane not in output:
                output[lane] = []
            output[lane].append(title)
        return output

    def update_idea_status(self, idea_id: str, status: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ideas
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                (status, idea_id),
            )
            conn.commit()

    def insert_idea_evidence(self, evidence: IdeaEvidenceSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO idea_evidence (
                    idea_id, doc_version_id, excerpt, locator
                )
                VALUES (%s::uuid, %s::uuid, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    evidence.idea_id,
                    evidence.doc_version_id,
                    evidence.excerpt,
                    json.dumps(evidence.locator),
                ),
            )
            idea_evidence_id = cur.fetchone()["id"]
            conn.commit()
        return idea_evidence_id

    def create_experiment(self, experiment: ExperimentSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO experiments (
                    idea_id, strategy_version_id, hypothesis, eval_status, metrics, artifacts
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s::jsonb, %s::jsonb)
                RETURNING id::text
                """,
                (
                    experiment.idea_id,
                    experiment.strategy_version_id,
                    experiment.hypothesis,
                    experiment.eval_status,
                    json.dumps(experiment.metrics),
                    json.dumps(experiment.artifacts),
                ),
            )
            experiment_id = cur.fetchone()["id"]
            conn.commit()
        return experiment_id

    def create_lesson(self, lesson: LessonSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lessons (
                    idea_id, experiment_id, lesson_type, summary, reusable_checklist
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    lesson.idea_id,
                    lesson.experiment_id,
                    lesson.lesson_type,
                    lesson.summary,
                    json.dumps(lesson.reusable_checklist),
                ),
            )
            lesson_id = cur.fetchone()["id"]
            conn.commit()
        return lesson_id

    def insert_strategy_risk_event(self, event: StrategyRiskEvent) -> str:
        triggered_at = event.triggered_at or datetime.utcnow()
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_risk_events (
                    strategy_version_id, event_type, payload, triggered_at
                )
                VALUES (%s::uuid, %s, %s::jsonb, %s)
                RETURNING id::text
                """,
                (
                    event.strategy_version_id,
                    event.event_type,
                    json.dumps(event.payload),
                    triggered_at,
                ),
            )
            risk_event_id = cur.fetchone()["id"]
            conn.commit()
        return risk_event_id

    def fetch_strategy_risk_events(self, strategy_version_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_version_id::text AS strategy_version_id,
                    event_type,
                    payload,
                    triggered_at,
                    created_at
                FROM strategy_risk_events
                WHERE strategy_version_id = %s::uuid
                ORDER BY triggered_at DESC, created_at DESC
                LIMIT %s
                """,
                (strategy_version_id, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def upsert_strategy_risk_snapshot(self, snapshot: StrategyRiskSnapshot) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_risk_snapshots (
                    strategy_version_id,
                    as_of,
                    as_of_date,
                    drawdown,
                    sharpe_20d,
                    state,
                    trigger_flags,
                    cooldown_until
                )
                VALUES (
                    %s::uuid,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    %s
                )
                ON CONFLICT (strategy_version_id, as_of_date)
                DO UPDATE SET
                    as_of = EXCLUDED.as_of,
                    drawdown = EXCLUDED.drawdown,
                    sharpe_20d = EXCLUDED.sharpe_20d,
                    state = EXCLUDED.state,
                    trigger_flags = EXCLUDED.trigger_flags,
                    cooldown_until = EXCLUDED.cooldown_until
                RETURNING id::text
                """,
                (
                    snapshot.strategy_version_id,
                    snapshot.as_of,
                    snapshot.as_of.date(),
                    snapshot.drawdown,
                    snapshot.sharpe_20d,
                    snapshot.state,
                    json.dumps(snapshot.trigger_flags),
                    snapshot.cooldown_until,
                ),
            )
            snapshot_id = cur.fetchone()["id"]
            conn.commit()
        return snapshot_id

    def fetch_latest_strategy_risk_snapshot(self, strategy_version_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_version_id::text AS strategy_version_id,
                    as_of,
                    as_of_date,
                    drawdown,
                    sharpe_20d,
                    state,
                    trigger_flags,
                    cooldown_until,
                    created_at
                FROM strategy_risk_snapshots
                WHERE strategy_version_id = %s::uuid
                ORDER BY as_of DESC
                LIMIT 1
                """,
                (strategy_version_id,),
            )
            row = cur.fetchone()
        return row

    def fetch_recent_strategy_risk_snapshots(
        self,
        strategy_version_id: str,
        limit: int = 40,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_version_id::text AS strategy_version_id,
                    as_of,
                    as_of_date,
                    drawdown,
                    sharpe_20d,
                    state,
                    trigger_flags,
                    cooldown_until,
                    created_at
                FROM strategy_risk_snapshots
                WHERE strategy_version_id = %s::uuid
                ORDER BY as_of DESC
                LIMIT %s
                """,
                (strategy_version_id, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

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

    def insert_risk_snapshot(self, snapshot: RiskSnapshot) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO risk_snapshots (
                    portfolio_id, as_of, equity, drawdown, sharpe_20d, gross_exposure,
                    net_exposure, state, triggers
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (portfolio_id, as_of)
                DO UPDATE SET equity = EXCLUDED.equity,
                              drawdown = EXCLUDED.drawdown,
                              sharpe_20d = EXCLUDED.sharpe_20d,
                              gross_exposure = EXCLUDED.gross_exposure,
                              net_exposure = EXCLUDED.net_exposure,
                              state = EXCLUDED.state,
                              triggers = EXCLUDED.triggers
                RETURNING id::text
                """,
                (
                    snapshot.portfolio_id,
                    snapshot.as_of,
                    float(snapshot.equity),
                    float(snapshot.drawdown),
                    snapshot.sharpe_20d,
                    snapshot.gross_exposure,
                    snapshot.net_exposure,
                    snapshot.state,
                    json.dumps(snapshot.triggers),
                ),
            )
            risk_snapshot_id = cur.fetchone()["id"]
            conn.commit()
        return risk_snapshot_id

    def upsert_fundamental_snapshot(
        self,
        snapshot: FundamentalSnapshot,
        security_uuid_map: dict[str, str] | None = None,
    ) -> None:
        security_uuid = None
        if security_uuid_map:
            security_uuid = security_uuid_map.get(snapshot.security_id)
        if not security_uuid:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id::text
                    FROM securities
                    WHERE security_id = %s
                    LIMIT 1
                    """,
                    (snapshot.security_id,),
                )
                row = cur.fetchone()
                security_uuid = row["id"] if row else None
        if not security_uuid:
            raise KeyError(f"security not found: {snapshot.security_id}")

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fundamental_snapshots (
                    security_id, as_of_date, source, rating, confidence,
                    summary, snapshot, created_by
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (security_id, as_of_date, source)
                DO UPDATE SET rating = EXCLUDED.rating,
                              confidence = EXCLUDED.confidence,
                              summary = EXCLUDED.summary,
                              snapshot = EXCLUDED.snapshot,
                              created_by = EXCLUDED.created_by
                """,
                (
                    security_uuid,
                    snapshot.as_of_date,
                    snapshot.source,
                    snapshot.rating,
                    snapshot.confidence,
                    snapshot.summary,
                    json.dumps(snapshot.snapshot),
                    snapshot.created_by,
                ),
            )
            conn.commit()

    def enqueue_agent_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int = 100,
        session_id: str | None = None,
        parent_task_id: str | None = None,
        assigned_role: str | None = None,
        dedupe_key: str | None = None,
    ) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_tasks (
                    task_type,
                    priority,
                    status,
                    payload,
                    session_id,
                    parent_task_id,
                    assigned_role,
                    dedupe_key
                )
                VALUES (%s, %s, 'queued', %s::jsonb, %s::uuid, %s::uuid, %s, %s)
                RETURNING id::text
                """,
                (
                    task_type,
                    int(priority),
                    json.dumps(payload),
                    session_id,
                    parent_task_id,
                    assigned_role,
                    dedupe_key,
                ),
            )
            task_id = cur.fetchone()["id"]
            conn.commit()
        return task_id

    def insert_research_external_input(self, spec: ResearchExternalInput) -> str:
        record_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO external_inputs (
                    id,
                    session_id,
                    message_id,
                    source_type,
                    source_url,
                    raw_text,
                    extracted_text,
                    quality_grade,
                    extraction_status,
                    user_comment,
                    metadata
                )
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    record_id,
                    spec.session_id,
                    spec.message_id,
                    spec.source_type,
                    spec.source_url,
                    spec.raw_text,
                    spec.extracted_text,
                    spec.quality_grade,
                    spec.extraction_status,
                    spec.user_comment,
                    json.dumps(spec.metadata or {}),
                ),
            )
            conn.commit()
        return record_id

    def insert_research_hypothesis(self, spec: ResearchHypothesisSpec) -> str:
        hypothesis_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_hypotheses (
                    id,
                    session_id,
                    external_input_id,
                    parent_message_id,
                    stance,
                    horizon_days,
                    thesis_md,
                    falsification_md,
                    confidence,
                    status,
                    is_favorite,
                    version,
                    metadata
                )
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    hypothesis_id,
                    spec.session_id,
                    spec.external_input_id,
                    spec.parent_message_id,
                    spec.stance,
                    int(spec.horizon_days),
                    spec.thesis_md,
                    spec.falsification_md,
                    spec.confidence,
                    spec.status,
                    bool(spec.is_favorite),
                    int(spec.version),
                    json.dumps(spec.metadata or {}),
                ),
            )
            for asset in spec.assets:
                cur.execute(
                    """
                    INSERT INTO research_hypothesis_assets (
                        id,
                        hypothesis_id,
                        asset_class,
                        security_id,
                        symbol_text,
                        weight_hint,
                        confidence
                    )
                    VALUES (%s::uuid, %s::uuid, %s, %s::uuid, %s, %s, %s)
                    """,
                    (
                        str(uuid4()),
                        hypothesis_id,
                        asset.asset_class,
                        asset.security_id,
                        asset.symbol_text,
                        asset.weight_hint,
                        asset.confidence,
                    ),
                )
            conn.commit()
        return hypothesis_id

    def insert_research_artifact(self, spec: ResearchArtifactSpec) -> str:
        artifact_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_artifacts (
                    id,
                    session_id,
                    hypothesis_id,
                    artifact_type,
                    title,
                    body_md,
                    code_text,
                    language,
                    is_favorite,
                    created_by_task_id,
                    metadata
                )
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s::uuid, %s::jsonb)
                """,
                (
                    artifact_id,
                    spec.session_id,
                    spec.hypothesis_id,
                    spec.artifact_type,
                    spec.title,
                    spec.body_md,
                    spec.code_text,
                    spec.language,
                    bool(spec.is_favorite),
                    spec.created_by_task_id,
                    json.dumps(spec.metadata or {}),
                ),
            )
            conn.commit()
        return artifact_id

    def insert_research_artifact_run(self, spec: ResearchArtifactRunSpec) -> str:
        run_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_artifact_runs (
                    id,
                    artifact_id,
                    run_status,
                    stdout_text,
                    stderr_text,
                    result_json,
                    output_r2_key
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    run_id,
                    spec.artifact_id,
                    spec.run_status,
                    spec.stdout_text,
                    spec.stderr_text,
                    json.dumps(spec.result_json or {}),
                    spec.output_r2_key,
                ),
            )
            conn.commit()
        return run_id

    def insert_research_hypothesis_outcome(self, spec: ResearchHypothesisOutcomeSpec) -> str:
        outcome_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_hypothesis_outcomes (
                    id,
                    hypothesis_id,
                    checked_at,
                    ret_1d,
                    ret_5d,
                    ret_20d,
                    mfe,
                    mae,
                    outcome_label,
                    summary_md,
                    metadata
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    outcome_id,
                    spec.hypothesis_id,
                    spec.checked_at,
                    spec.ret_1d,
                    spec.ret_5d,
                    spec.ret_20d,
                    spec.mfe,
                    spec.mae,
                    spec.outcome_label,
                    spec.summary_md,
                    json.dumps(spec.metadata or {}),
                ),
            )
            conn.commit()
        return outcome_id

    def append_chat_message(
        self,
        *,
        session_id: str,
        role: str,
        content: str,
        run_id: str | None = None,
        answer_before: str | None = None,
        answer_after: str | None = None,
        change_reason: str | None = None,
    ) -> str:
        message_id = str(uuid4())
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_messages (
                    id,
                    session_id,
                    run_id,
                    role,
                    content,
                    answer_before,
                    answer_after,
                    change_reason
                )
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s)
                """,
                (
                    message_id,
                    session_id,
                    run_id,
                    role,
                    content,
                    answer_before,
                    answer_after,
                    change_reason,
                ),
            )
            conn.commit()
        return message_id

    def fetch_latest_chat_message(self, session_id: str, role: str | None = None) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            if role:
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        session_id::text AS session_id,
                        role,
                        content,
                        answer_before,
                        answer_after,
                        change_reason,
                        created_at
                    FROM chat_messages
                    WHERE session_id = %s::uuid
                      AND role = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (session_id, role),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        session_id::text AS session_id,
                        role,
                        content,
                        answer_before,
                        answer_after,
                        change_reason,
                        created_at
                    FROM chat_messages
                    WHERE session_id = %s::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (session_id,),
                )
            return cur.fetchone()

    def fetch_research_external_inputs(self, session_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    session_id::text AS session_id,
                    message_id::text AS message_id,
                    source_type,
                    source_url,
                    raw_text,
                    extracted_text,
                    quality_grade,
                    extraction_status,
                    user_comment,
                    metadata,
                    created_at
                FROM external_inputs
                WHERE session_id = %s::uuid
                ORDER BY created_at ASC
                """,
                (session_id,),
            )
            return cur.fetchall()

    def update_research_external_input(
        self,
        input_id: str,
        *,
        extracted_text: str | None = None,
        quality_grade: str | None = None,
        extraction_status: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE external_inputs
                SET extracted_text = COALESCE(%s, extracted_text),
                    quality_grade = COALESCE(%s, quality_grade),
                    extraction_status = COALESCE(%s, extraction_status),
                    metadata = CASE
                        WHEN %s::jsonb IS NULL THEN metadata
                        ELSE metadata || %s::jsonb
                    END
                WHERE id = %s::uuid
                """,
                (
                    extracted_text,
                    quality_grade,
                    extraction_status,
                    json.dumps(metadata_patch) if metadata_patch is not None else None,
                    json.dumps(metadata_patch) if metadata_patch is not None else None,
                    input_id,
                ),
            )
            conn.commit()

    def fetch_research_hypotheses_by_ids(self, hypothesis_ids: list[str]) -> list[dict[str, Any]]:
        cleaned = [item for item in hypothesis_ids if item]
        if not cleaned:
            return []
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    h.id::text AS id,
                    h.session_id::text AS session_id,
                    h.external_input_id::text AS external_input_id,
                    h.parent_message_id::text AS parent_message_id,
                    h.stance,
                    h.horizon_days,
                    h.thesis_md,
                    h.falsification_md,
                    h.confidence,
                    h.status,
                    h.is_favorite,
                    h.version,
                    h.metadata,
                    h.created_at
                FROM research_hypotheses h
                WHERE h.id = ANY(%s::uuid[])
                ORDER BY h.created_at ASC
                """,
                (cleaned,),
            )
            return cur.fetchall()

    def fetch_research_hypotheses_for_session(self, session_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    h.id::text AS id,
                    h.session_id::text AS session_id,
                    h.external_input_id::text AS external_input_id,
                    h.parent_message_id::text AS parent_message_id,
                    h.stance,
                    h.horizon_days,
                    h.thesis_md,
                    h.falsification_md,
                    h.confidence,
                    h.status,
                    h.is_favorite,
                    h.version,
                    h.metadata,
                    h.created_at
                FROM research_hypotheses h
                WHERE h.session_id = %s::uuid
                ORDER BY h.created_at ASC
                """,
                (session_id,),
            )
            return cur.fetchall()

    def fetch_research_hypothesis_assets(self, hypothesis_ids: list[str]) -> list[dict[str, Any]]:
        cleaned = [item for item in hypothesis_ids if item]
        if not cleaned:
            return []
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.id::text AS id,
                    a.hypothesis_id::text AS hypothesis_id,
                    a.asset_class,
                    a.symbol_text,
                    a.weight_hint,
                    a.confidence,
                    s.security_id,
                    s.ticker,
                    s.name,
                    s.market
                FROM research_hypothesis_assets a
                LEFT JOIN securities s
                  ON s.id = a.security_id
                WHERE a.hypothesis_id = ANY(%s::uuid[])
                ORDER BY a.created_at ASC
                """,
                (cleaned,),
            )
            return cur.fetchall()

    def update_research_hypothesis(
        self,
        hypothesis_id: str,
        *,
        status: str | None = None,
        thesis_md: str | None = None,
        falsification_md: str | None = None,
        confidence: float | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research_hypotheses
                SET status = COALESCE(%s, status),
                    thesis_md = COALESCE(%s, thesis_md),
                    falsification_md = COALESCE(%s, falsification_md),
                    confidence = COALESCE(%s, confidence),
                    metadata = CASE
                        WHEN %s::jsonb IS NULL THEN metadata
                        ELSE metadata || %s::jsonb
                    END
                WHERE id = %s::uuid
                """,
                (
                    status,
                    thesis_md,
                    falsification_md,
                    confidence,
                    json.dumps(metadata_patch) if metadata_patch is not None else None,
                    json.dumps(metadata_patch) if metadata_patch is not None else None,
                    hypothesis_id,
                ),
            )
            conn.commit()

    def fetch_research_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    session_id::text AS session_id,
                    hypothesis_id::text AS hypothesis_id,
                    artifact_type,
                    title,
                    body_md,
                    code_text,
                    language,
                    is_favorite,
                    created_by_task_id::text AS created_by_task_id,
                    metadata,
                    created_at
                FROM research_artifacts
                WHERE id = %s::uuid
                LIMIT 1
                """,
                (artifact_id,),
            )
            return cur.fetchone()

    def fetch_research_artifacts_for_session(self, session_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    session_id::text AS session_id,
                    hypothesis_id::text AS hypothesis_id,
                    artifact_type,
                    title,
                    body_md,
                    code_text,
                    language,
                    is_favorite,
                    created_by_task_id::text AS created_by_task_id,
                    metadata,
                    created_at
                FROM research_artifacts
                WHERE session_id = %s::uuid
                ORDER BY created_at ASC
                """,
                (session_id,),
            )
            return cur.fetchall()

    def fetch_latest_research_artifact_run(self, artifact_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    artifact_id::text AS artifact_id,
                    run_status,
                    stdout_text,
                    stderr_text,
                    result_json,
                    output_r2_key,
                    metadata,
                    created_at
                FROM research_artifact_runs
                WHERE artifact_id = %s::uuid
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (artifact_id,),
            )
            return cur.fetchone()

    def mark_agent_task(
        self,
        task_id: str,
        status: str,
        result: dict[str, Any] | None = None,
        cost_usd: float | None = None,
        error_text: str | None = None,
    ) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_tasks
                SET status = %s,
                    result = COALESCE(%s::jsonb, result),
                    cost_usd = COALESCE(%s, cost_usd),
                    error_text = COALESCE(%s, error_text),
                    attempt_count = CASE WHEN %s = 'failed' THEN attempt_count + 1 ELSE attempt_count END,
                    started_at = CASE WHEN status = 'queued' AND %s = 'running' THEN NOW() ELSE started_at END,
                    finished_at = CASE WHEN %s IN ('success', 'failed', 'canceled') THEN NOW() ELSE finished_at END
                WHERE id = %s::uuid
                """,
                (
                    status,
                    json.dumps(result) if result is not None else None,
                    cost_usd,
                    error_text,
                    status,
                    status,
                    status,
                    task_id,
                ),
            )
            conn.commit()

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

    def fetch_latest_weekly_candidates(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_run AS (
                    SELECT id
                    FROM runs
                    WHERE run_type = 'weekly'
                      AND status = 'success'
                    ORDER BY finished_at DESC NULLS LAST, started_at DESC
                    LIMIT 1
                )
                SELECT
                    s.security_id,
                    s.market,
                    s.ticker,
                    s.name,
                    sc.combined_score,
                    sc.confidence,
                    sc.missing_ratio,
                    COALESCE((sc.flags->>'edge_score')::double precision, 0.0) AS edge_score,
                    COALESCE(es.primary_source_count, 0) AS primary_source_count,
                    COALESCE(es.has_major_contradiction, FALSE) AS has_major_contradiction
                FROM latest_run lr
                JOIN top50_membership t
                  ON t.run_id = lr.id
                JOIN securities s
                  ON s.id = t.security_id
                LEFT JOIN score_snapshots sc
                  ON sc.run_id = t.run_id
                 AND sc.security_id = t.security_id
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(DISTINCT c.doc_version_id)::int AS primary_source_count,
                        BOOL_OR(
                          e.title ~* '(下方修正|訂正|撤回)'
                          OR COALESCE(e.summary, '') ~* '(下方修正|訂正|撤回)'
                        ) AS has_major_contradiction
                    FROM reports r
                    LEFT JOIN citations c
                      ON c.report_id = r.id
                    LEFT JOIN events e
                      ON e.security_id = r.security_id
                     AND e.event_time >= NOW() - INTERVAL '30 day'
                    WHERE r.security_id = t.security_id
                      AND r.created_at >= NOW() - INTERVAL '30 day'
                ) es ON TRUE
                ORDER BY t.rank ASC
                LIMIT %s
                """,
                (max(1, int(limit)),),
            )
            rows = cur.fetchall()
        return rows

    def fetch_latest_strategy_edge_inputs(
        self,
        asset_scope: str,
        statuses: list[str] | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        status_values = statuses or ["candidate", "approved", "paper", "live"]
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_versions AS (
                    SELECT DISTINCT ON (sv.strategy_id)
                        sv.strategy_id,
                        sv.id::text AS strategy_version_id,
                        sv.version,
                        sv.spec AS strategy_spec
                    FROM strategy_versions sv
                    ORDER BY sv.strategy_id, sv.version DESC
                ),
                latest_eval AS (
                    SELECT DISTINCT ON (se.strategy_version_id)
                        se.strategy_version_id,
                        se.eval_type,
                        nullif(se.metrics->>'sharpe', '')::double precision AS sharpe,
                        nullif(se.metrics->>'max_dd', '')::double precision AS max_dd,
                        nullif(se.metrics->>'cagr', '')::double precision AS cagr,
                        se.metrics AS metrics,
                        se.artifacts AS artifacts,
                        se.created_at
                    FROM strategy_evaluations se
                    ORDER BY se.strategy_version_id, se.created_at DESC
                )
                SELECT
                    s.id::text AS strategy_id,
                    s.name AS strategy_name,
                    s.asset_scope,
                    s.status,
                    lv.strategy_version_id,
                    lv.version,
                    lv.strategy_spec,
                    le.eval_type,
                    le.sharpe,
                    le.max_dd,
                    le.cagr,
                    le.metrics,
                    le.artifacts,
                    le.created_at AS eval_created_at
                FROM strategies s
                JOIN latest_versions lv
                  ON lv.strategy_id = s.id
                LEFT JOIN latest_eval le
                  ON le.strategy_version_id = lv.strategy_version_id::uuid
                WHERE s.asset_scope = %s
                  AND s.status = ANY(%s)
                ORDER BY s.updated_at DESC
                LIMIT %s
                """,
                (asset_scope, status_values, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def fetch_latest_edge_states(
        self,
        market_scope: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_name,
                    strategy_version_id::text AS strategy_version_id,
                    market_scope,
                    symbol,
                    observed_at,
                    edge_score,
                    COALESCE(expected_net_edge, expected_net_edge_bps) AS expected_net_edge,
                    COALESCE(distance_to_entry, distance_to_entry_bps) AS distance_to_entry,
                    expected_net_edge_bps,
                    distance_to_entry_bps,
                    confidence,
                    COALESCE(NULLIF(risk_json, '{}'::jsonb), risk, '{}'::jsonb) AS risk_json,
                    risk,
                    explain,
                    COALESCE(market_regime, market_scope) AS market_regime,
                    meta,
                    created_at
                FROM edge_states
                WHERE (%s::text IS NULL OR market_scope = %s)
                ORDER BY observed_at DESC, edge_score DESC
                LIMIT %s
                """,
                (market_scope, market_scope, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def fetch_latest_edge_state_for_strategy(
        self,
        strategy_version_id: str,
        at_or_before: datetime | None = None,
    ) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_name,
                    strategy_version_id::text AS strategy_version_id,
                    market_scope,
                    symbol,
                    observed_at,
                    edge_score,
                    COALESCE(expected_net_edge, expected_net_edge_bps) AS expected_net_edge,
                    COALESCE(distance_to_entry, distance_to_entry_bps) AS distance_to_entry,
                    expected_net_edge_bps,
                    distance_to_entry_bps,
                    confidence,
                    COALESCE(NULLIF(risk_json, '{}'::jsonb), risk, '{}'::jsonb) AS risk_json,
                    risk,
                    explain,
                    COALESCE(market_regime, market_scope) AS market_regime,
                    meta,
                    created_at
                FROM edge_states
                WHERE strategy_version_id = %s::uuid
                  AND (%s::timestamptz IS NULL OR observed_at <= %s)
                ORDER BY observed_at DESC, edge_score DESC
                LIMIT 1
                """,
                (strategy_version_id, at_or_before, at_or_before),
            )
            row = cur.fetchone()
        return row

    def fetch_edge_states_for_period(
        self,
        strategy_version_id: str,
        start_at: datetime,
        end_at: datetime,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    strategy_name,
                    strategy_version_id::text AS strategy_version_id,
                    market_scope,
                    symbol,
                    observed_at,
                    edge_score,
                    COALESCE(expected_net_edge, expected_net_edge_bps) AS expected_net_edge,
                    COALESCE(distance_to_entry, distance_to_entry_bps) AS distance_to_entry,
                    expected_net_edge_bps,
                    distance_to_entry_bps,
                    confidence,
                    COALESCE(NULLIF(risk_json, '{}'::jsonb), risk, '{}'::jsonb) AS risk_json,
                    risk,
                    explain,
                    COALESCE(market_regime, market_scope) AS market_regime,
                    meta,
                    created_at
                FROM edge_states
                WHERE strategy_version_id = %s::uuid
                  AND observed_at >= %s
                  AND observed_at <= %s
                ORDER BY observed_at ASC, edge_score DESC
                LIMIT %s
                """,
                (strategy_version_id, start_at, end_at, max(1, int(limit))),
            )
            rows = cur.fetchall()
        return rows

    def fetch_queued_agent_tasks(
        self,
        limit: int = 20,
        task_types: list[str] | None = None,
        assigned_role: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    session_id::text AS session_id,
                    parent_task_id::text AS parent_task_id,
                    task_type,
                    priority,
                    status,
                    payload,
                    result,
                    cost_usd,
                    attempt_count,
                    max_attempts,
                    available_at,
                    lease_owner,
                    lease_expires_at,
                    dedupe_key,
                    error_text,
                    assigned_role,
                    assigned_node,
                    started_at,
                    finished_at,
                    created_at
                FROM agent_tasks
                WHERE status = 'queued'
                  AND available_at <= NOW()
                  AND (%s::text[] IS NULL OR task_type = ANY(%s))
                  AND (%s::text IS NULL OR assigned_role = %s OR assigned_role IS NULL)
                ORDER BY priority ASC, created_at ASC
                LIMIT %s
                """,
                (
                    task_types,
                    task_types,
                    assigned_role,
                    assigned_role,
                    max(1, int(limit)),
                ),
            )
            rows = cur.fetchall()
        return rows

    def latest_weekly_run_id(self) -> str | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text
                FROM runs
                WHERE run_type = 'weekly' AND status = 'success'
                ORDER BY finished_at DESC NULLS LAST, started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        return row["id"] if row else None

    def latest_daily_run_id(self) -> str | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text
                FROM runs
                WHERE run_type = 'daily' AND status = 'success'
                ORDER BY finished_at DESC NULLS LAST, started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        return row["id"] if row else None

    def has_run_for_date(
        self,
        run_type: str,
        target_date: date,
        tz_name: str = "Asia/Tokyo",
        statuses: tuple[str, ...] = ("running", "success"),
    ) -> bool:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM runs
                    WHERE run_type = %s
                      AND status = ANY(%s)
                      AND (started_at AT TIME ZONE %s)::date = %s
                ) AS exists_flag
                """,
                (run_type, list(statuses), tz_name, target_date),
            )
            row = cur.fetchone()
        return bool(row["exists_flag"])
