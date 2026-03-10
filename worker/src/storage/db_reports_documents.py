from __future__ import annotations

from datetime import datetime
import hashlib

import psycopg


class NeonRepositoryReportDocumentsMixin:
    def _ensure_document_version(self, cur: psycopg.Cursor, doc_version_id: str, source_url: str | None = None) -> None:
        ext_doc_id = doc_version_id
        src = source_url or "https://example.com/evidence"
        cur.execute(
            """
            INSERT INTO documents (external_doc_id, source_system, source_url, title, published_at)
            VALUES (%s, 'mock', %s, %s, NOW())
            ON CONFLICT (source_system, external_doc_id)
            DO UPDATE SET source_url = EXCLUDED.source_url
            RETURNING id::text
            """,
            (ext_doc_id, src, f"Evidence {ext_doc_id[:8]}"),
        )
        doc_id = cur.fetchone()["id"]

        sha = hashlib.sha256(doc_version_id.encode("utf-8")).hexdigest()
        cur.execute(
            """
            INSERT INTO document_versions (
                id, document_id, retrieved_at, sha256, mime_type, r2_object_key, r2_text_key, page_count
            )
            VALUES (%s::uuid, %s::uuid, NOW(), %s, 'text/plain', %s, %s, 1)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                doc_version_id,
                doc_id,
                sha,
                f"mock/evidence/{doc_version_id}.txt",
                f"mock/evidence/{doc_version_id}.chunk.txt",
            ),
        )

    def upsert_document_with_version(
        self,
        *,
        external_doc_id: str,
        source_system: str,
        source_url: str,
        title: str | None,
        published_at: datetime | None,
        retrieved_at: datetime | None,
        sha256: str,
        mime_type: str,
        r2_object_key: str,
        r2_text_key: str | None = None,
        page_count: int | None = None,
    ) -> str:
        normalized_sha = str(sha256).strip().lower()
        if len(normalized_sha) != 64:
            raise ValueError("sha256 must be a 64-char hex string")

        fetched_at = retrieved_at or datetime.utcnow()
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (
                    external_doc_id, source_system, source_url, title, published_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_system, external_doc_id)
                DO UPDATE SET source_url = EXCLUDED.source_url,
                              title = COALESCE(EXCLUDED.title, documents.title),
                              published_at = COALESCE(EXCLUDED.published_at, documents.published_at)
                RETURNING id::text
                """,
                (external_doc_id, source_system, source_url, title, published_at),
            )
            doc_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO document_versions (
                    document_id, retrieved_at, sha256, mime_type, r2_object_key, r2_text_key, page_count
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, sha256)
                DO UPDATE SET retrieved_at = EXCLUDED.retrieved_at,
                              mime_type = EXCLUDED.mime_type,
                              r2_object_key = EXCLUDED.r2_object_key,
                              r2_text_key = EXCLUDED.r2_text_key,
                              page_count = EXCLUDED.page_count
                RETURNING id::text
                """,
                (
                    doc_id,
                    fetched_at,
                    normalized_sha,
                    mime_type,
                    r2_object_key,
                    r2_text_key,
                    page_count,
                ),
            )
            doc_version_id = cur.fetchone()["id"]
            conn.commit()

        return doc_version_id
