from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from src.storage.db_base import _merge_edge_risk_payload, _normalize_edge_risk_payload
from src.types import EdgeState, StrategyEvaluation, StrategyLifecycleReview, StrategySpec, StrategyVersionSpec


class NeonRepositoryStrategyMixin:
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

