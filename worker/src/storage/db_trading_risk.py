from __future__ import annotations

from datetime import datetime
import json
from typing import Any

from src.types import RiskSnapshot, StrategyRiskEvent, StrategyRiskSnapshot


class NeonRepositoryTradingRiskMixin:
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
