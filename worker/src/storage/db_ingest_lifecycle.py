from __future__ import annotations

import json
from typing import Any
from uuid import uuid4


class NeonRepositoryIngestLifecycleMixin:
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
