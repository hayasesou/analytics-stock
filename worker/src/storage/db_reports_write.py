from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from src.storage.db_base import _chunks
from src.types import EventItem, ReportItem


class NeonRepositoryReportWriteMixin:
    def insert_events(
        self,
        run_id: str,
        events: list[EventItem],
        security_uuid_map: dict[str, str],
    ) -> None:
        if not events:
            return

        with self._conn() as conn, conn.cursor() as cur:
            for e in events:
                sec_uuid = security_uuid_map.get(e.security_id) if e.security_id else None
                doc_version_id = e.doc_version_id
                if doc_version_id:
                    self._ensure_document_version(cur, doc_version_id, e.source_url)

                cur.execute(
                    """
                    INSERT INTO events (
                        run_id, security_id, event_type, importance, event_time,
                        title, summary, source_url, doc_version_id, metadata
                    )
                    VALUES (
                        %s::uuid, %s::uuid, %s, %s, %s,
                        %s, %s, %s, %s::uuid, %s::jsonb
                    )
                    """,
                    (
                        run_id,
                        sec_uuid,
                        e.event_type,
                        e.importance,
                        e.event_time,
                        e.title,
                        e.summary,
                        e.source_url,
                        doc_version_id,
                        json.dumps(e.metadata),
                    ),
                )
            conn.commit()

    def insert_report(self, run_id: str, report: ReportItem, security_uuid_map: dict[str, str]) -> str:
        sec_uuid = security_uuid_map.get(report.security_id) if report.security_id else None
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reports (
                    run_id, security_id, report_type, title, body_md,
                    conclusion, falsification_conditions, confidence
                )
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
                RETURNING id::text
                """,
                (
                    run_id,
                    sec_uuid,
                    report.report_type,
                    report.title,
                    report.body_md,
                    report.conclusion,
                    report.falsification_conditions,
                    report.confidence,
                ),
            )
            report_id = cur.fetchone()["id"]

            for claim in report.claims:
                cur.execute(
                    """
                    INSERT INTO report_claims (report_id, claim_id, claim_text, claim_type, status)
                    VALUES (%s::uuid, %s, %s, 'important', %s)
                    ON CONFLICT (report_id, claim_id)
                    DO UPDATE SET claim_text = EXCLUDED.claim_text,
                                  status = EXCLUDED.status
                    """,
                    (report_id, claim["claim_id"], claim["claim_text"], claim.get("status", "supported")),
                )

            for citation in report.citations:
                self._ensure_document_version(cur, citation.doc_version_id)
                cur.execute(
                    """
                    INSERT INTO citations (
                        report_id, claim_id, doc_version_id, page_ref, quote_text, locator
                    )
                    VALUES (%s::uuid, %s, %s::uuid, %s, %s, %s::jsonb)
                    """,
                    (
                        report_id,
                        citation.claim_id,
                        citation.doc_version_id,
                        citation.page_ref,
                        citation.quote_text,
                        json.dumps({"source": "worker"}),
                    ),
                )

            conn.commit()
        return report_id

    def insert_reports_bulk(
        self,
        run_id: str,
        reports: list[ReportItem],
        security_uuid_map: dict[str, str],
        batch_size: int = 10,
    ) -> list[str]:
        if not reports:
            return []

        report_ids: list[str] = []
        with self._conn() as conn, conn.cursor() as cur:
            for batch in _chunks(reports, size=batch_size):
                current_ids = [str(uuid4()) for _ in batch]
                report_rows: list[tuple[Any, ...]] = []
                claim_rows: list[tuple[Any, ...]] = []
                citation_rows: list[tuple[Any, ...]] = []
                doc_sources: dict[str, str | None] = {}

                for report_id, report in zip(current_ids, batch, strict=True):
                    sec_uuid = security_uuid_map.get(report.security_id) if report.security_id else None
                    report_rows.append(
                        (
                            report_id,
                            run_id,
                            sec_uuid,
                            report.report_type,
                            report.title,
                            report.body_md,
                            report.conclusion,
                            report.falsification_conditions,
                            report.confidence,
                        )
                    )

                    for claim in report.claims:
                        claim_rows.append(
                            (
                                report_id,
                                claim["claim_id"],
                                claim["claim_text"],
                                claim.get("status", "supported"),
                            )
                        )

                    for citation in report.citations:
                        doc_sources.setdefault(citation.doc_version_id, None)
                        citation_rows.append(
                            (
                                report_id,
                                citation.claim_id,
                                citation.doc_version_id,
                                citation.page_ref,
                                citation.quote_text,
                                json.dumps({"source": "worker"}),
                            )
                        )

                for doc_version_id, source_url in doc_sources.items():
                    self._ensure_document_version(cur, doc_version_id, source_url)

                cur.executemany(
                    """
                    INSERT INTO reports (
                        id, run_id, security_id, report_type, title, body_md,
                        conclusion, falsification_conditions, confidence
                    )
                    VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
                    """,
                    report_rows,
                )

                if claim_rows:
                    cur.executemany(
                        """
                        INSERT INTO report_claims (report_id, claim_id, claim_text, claim_type, status)
                        VALUES (%s::uuid, %s, %s, 'important', %s)
                        ON CONFLICT (report_id, claim_id)
                        DO UPDATE SET claim_text = EXCLUDED.claim_text,
                                      status = EXCLUDED.status
                        """,
                        claim_rows,
                    )

                if citation_rows:
                    cur.executemany(
                        """
                        INSERT INTO citations (
                            report_id, claim_id, doc_version_id, page_ref, quote_text, locator
                        )
                        VALUES (%s::uuid, %s, %s::uuid, %s, %s, %s::jsonb)
                        """,
                        citation_rows,
                    )

                conn.commit()
                report_ids.extend(current_ids)

        return report_ids
