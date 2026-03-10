from __future__ import annotations

from datetime import date, datetime
from typing import Any


class NeonRepositoryReadQueriesMixin:
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
