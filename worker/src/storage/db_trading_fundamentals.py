from __future__ import annotations

import json

from src.types import FundamentalSnapshot


class NeonRepositoryTradingFundamentalsMixin:
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
