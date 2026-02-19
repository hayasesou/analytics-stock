from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime
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
    EventItem,
    FillRecord,
    FundamentalSnapshot,
    OrderIntent,
    OrderRecord,
    PortfolioSpec,
    PositionRecord,
    ReportItem,
    RiskSnapshot,
    Security,
    StrategyEvaluation,
    StrategySpec,
    StrategyVersionSpec,
)


def _chunks(seq: list[Any], size: int = 1000) -> Iterator[list[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


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
                              status = EXCLUDED.status,
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
    ) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_tasks (task_type, priority, status, payload)
                VALUES (%s, %s, 'queued', %s::jsonb)
                RETURNING id::text
                """,
                (task_type, int(priority), json.dumps(payload)),
            )
            task_id = cur.fetchone()["id"]
            conn.commit()
        return task_id

    def mark_agent_task(
        self,
        task_id: str,
        status: str,
        result: dict[str, Any] | None = None,
        cost_usd: float | None = None,
    ) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_tasks
                SET status = %s,
                    result = COALESCE(%s::jsonb, result),
                    cost_usd = COALESCE(%s, cost_usd),
                    started_at = CASE WHEN status = 'queued' AND %s = 'running' THEN NOW() ELSE started_at END,
                    finished_at = CASE WHEN %s IN ('success', 'failed', 'canceled') THEN NOW() ELSE finished_at END
                WHERE id = %s::uuid
                """,
                (
                    status,
                    json.dumps(result) if result is not None else None,
                    cost_usd,
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

    def fetch_queued_agent_tasks(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    task_type,
                    priority,
                    status,
                    payload,
                    result,
                    cost_usd,
                    started_at,
                    finished_at,
                    created_at
                FROM agent_tasks
                WHERE status = 'queued'
                ORDER BY priority ASC, created_at ASC
                LIMIT %s
                """,
                (max(1, int(limit)),),
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
