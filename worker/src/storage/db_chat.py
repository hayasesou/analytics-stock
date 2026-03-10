from __future__ import annotations

import json
from typing import Any
from uuid import uuid4


class NeonRepositoryChatMixin:
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

