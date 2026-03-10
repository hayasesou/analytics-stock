from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd

from src.storage.db_base import _chunks


class NeonRepositoryScoresMixin:
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

