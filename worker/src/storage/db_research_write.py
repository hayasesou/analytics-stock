from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from src.types import ResearchArtifactRunSpec, ResearchArtifactSpec, ResearchExternalInput, ResearchHypothesisOutcomeSpec, ResearchHypothesisSpec


class NeonRepositoryResearchWriteMixin:
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

