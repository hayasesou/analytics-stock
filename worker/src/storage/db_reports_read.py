from __future__ import annotations

import pandas as pd

from src.types import CitationItem


class NeonRepositoryReportReadMixin:
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
