from __future__ import annotations

import json

from src.types import ExperimentSpec, IdeaEvidenceSpec, IdeaSpec, LessonSpec


class NeonRepositoryIdeasMixin:
    def create_idea(self, idea: IdeaSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ideas (
                    source_type, source_url, title, raw_text, status, priority, created_by, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    idea.source_type,
                    idea.source_url,
                    idea.title,
                    idea.raw_text,
                    idea.status,
                    int(idea.priority),
                    idea.created_by,
                    json.dumps(idea.metadata),
                ),
            )
            idea_id = cur.fetchone()["id"]
            conn.commit()
        return idea_id

    def fetch_idea_claim_hashes_by_source_url(
        self,
        *,
        source_type: str,
        source_url: str,
        limit: int = 1000,
    ) -> set[str]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    metadata->>'claim_hash' AS claim_hash
                FROM ideas
                WHERE source_type = %s
                  AND source_url = %s
                  AND metadata ? 'claim_hash'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (source_type, source_url, max(1, int(limit))),
            )
            rows = cur.fetchall()

        output: set[str] = set()
        for row in rows:
            raw_value = (row or {}).get("claim_hash")
            if raw_value is None:
                continue
            value = str(raw_value).strip().lower()
            if value:
                output.add(value)
        return output

    def fetch_idea(self, idea_id: str) -> dict[str, Any] | None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id::text AS id,
                    source_type,
                    source_url,
                    title,
                    raw_text,
                    status,
                    priority,
                    created_by,
                    metadata,
                    created_at,
                    updated_at
                FROM ideas
                WHERE id = %s::uuid
                LIMIT 1
                """,
                (idea_id,),
            )
            row = cur.fetchone()
        return row

    def fetch_research_kanban_counts(self, statuses: list[str] | None = None) -> dict[str, int]:
        lanes = statuses or ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        normalized_lanes = [str(status).strip() for status in lanes if str(status).strip()]
        if not normalized_lanes:
            normalized_lanes = ["new", "analyzing", "rejected", "candidate", "paper", "live"]

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH lane_rows AS (
                    SELECT status AS lane
                    FROM ideas
                    WHERE status = ANY(%s::text[])
                    UNION ALL
                    SELECT status AS lane
                    FROM strategies
                    WHERE status = ANY(%s::text[])
                )
                SELECT lane, COUNT(*)::int AS cnt
                FROM lane_rows
                GROUP BY lane
                """,
                (normalized_lanes, normalized_lanes),
            )
            rows = cur.fetchall()

        output = {lane: 0 for lane in normalized_lanes}
        for row in rows:
            lane = str((row or {}).get("lane", "")).strip()
            if not lane:
                continue
            try:
                output[lane] = int((row or {}).get("cnt", 0) or 0)
            except (TypeError, ValueError):
                output[lane] = 0
        return output

    def fetch_research_kanban_samples(
        self,
        *,
        statuses: list[str] | None = None,
        limit_per_lane: int = 3,
    ) -> dict[str, list[str]]:
        lanes = statuses or ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        normalized_lanes = [str(status).strip() for status in lanes if str(status).strip()]
        if not normalized_lanes:
            normalized_lanes = ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        lane_limit = max(1, int(limit_per_lane))

        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH idea_ranked AS (
                    SELECT
                        status AS lane,
                        title AS item_title,
                        ROW_NUMBER() OVER (PARTITION BY status ORDER BY priority DESC, created_at DESC) AS rn
                    FROM ideas
                    WHERE status = ANY(%s::text[])
                ),
                strategy_ranked AS (
                    SELECT
                        status AS lane,
                        name AS item_title,
                        ROW_NUMBER() OVER (PARTITION BY status ORDER BY updated_at DESC, created_at DESC) AS rn
                    FROM strategies
                    WHERE status = ANY(%s::text[])
                ),
                merged AS (
                    SELECT lane, item_title, rn FROM idea_ranked
                    UNION ALL
                    SELECT lane, item_title, rn FROM strategy_ranked
                )
                SELECT lane, item_title
                FROM merged
                WHERE rn <= %s
                ORDER BY lane, rn
                """,
                (normalized_lanes, normalized_lanes, lane_limit),
            )
            rows = cur.fetchall()

        output = {lane: [] for lane in normalized_lanes}
        for row in rows:
            lane = str((row or {}).get("lane", "")).strip()
            if not lane:
                continue
            title = str((row or {}).get("item_title", "")).strip()
            if not title:
                continue
            if lane not in output:
                output[lane] = []
            output[lane].append(title)
        return output

    def update_idea_status(self, idea_id: str, status: str) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ideas
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                (status, idea_id),
            )
            conn.commit()

    def insert_idea_evidence(self, evidence: IdeaEvidenceSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO idea_evidence (
                    idea_id, doc_version_id, excerpt, locator
                )
                VALUES (%s::uuid, %s::uuid, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    evidence.idea_id,
                    evidence.doc_version_id,
                    evidence.excerpt,
                    json.dumps(evidence.locator),
                ),
            )
            idea_evidence_id = cur.fetchone()["id"]
            conn.commit()
        return idea_evidence_id

    def create_experiment(self, experiment: ExperimentSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO experiments (
                    idea_id, strategy_version_id, hypothesis, eval_status, metrics, artifacts
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s::jsonb, %s::jsonb)
                RETURNING id::text
                """,
                (
                    experiment.idea_id,
                    experiment.strategy_version_id,
                    experiment.hypothesis,
                    experiment.eval_status,
                    json.dumps(experiment.metrics),
                    json.dumps(experiment.artifacts),
                ),
            )
            experiment_id = cur.fetchone()["id"]
            conn.commit()
        return experiment_id

    def create_lesson(self, lesson: LessonSpec) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lessons (
                    idea_id, experiment_id, lesson_type, summary, reusable_checklist
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (
                    lesson.idea_id,
                    lesson.experiment_id,
                    lesson.lesson_type,
                    lesson.summary,
                    json.dumps(lesson.reusable_checklist),
                ),
            )
            lesson_id = cur.fetchone()["id"]
            conn.commit()
        return lesson_id

