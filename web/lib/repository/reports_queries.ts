import { getSql } from "@/lib/db";
import type { CitationRecord, ReportRecord } from "@/lib/types";
import { decodeSecurityId, isLegacyMockSecurity } from "./shared";

export async function fetchReportsBySecurity(securityId: string): Promise<ReportRecord[]> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId);
  const rows = await sql<
    {
      id: string;
      security_id: string | null;
      market: "JP" | "US";
      name: string;
      report_type: string;
      title: string;
      body_md: string;
      conclusion: string | null;
      falsification_conditions: string | null;
      confidence: string | null;
      created_at: string;
    }[]
  >`
    select
      r.id::text,
      s.security_id,
      s.market,
      coalesce(s.name, s.security_id) as name,
      r.report_type,
      r.title,
      r.body_md,
      r.conclusion,
      r.falsification_conditions,
      r.confidence,
      r.created_at::text
    from reports r
    left join securities s on s.id = r.security_id
    where s.security_id = ${normalizedSecurityId}
    order by r.created_at desc
  `;

  return rows
    .filter((row) => !isLegacyMockSecurity({ market: row.market, name: row.name }))
    .map((row) => ({
      id: row.id,
      securityId: row.security_id,
      reportType: row.report_type,
      title: row.title,
      bodyMd: row.body_md,
      conclusion: row.conclusion,
      falsificationConditions: row.falsification_conditions,
      confidence: row.confidence,
      createdAt: row.created_at
    }));
}

export async function fetchWeeklySummary(): Promise<ReportRecord | null> {
  const sql = getSql();
  const row = await sql<
    {
      id: string;
      report_type: string;
      title: string;
      body_md: string;
      conclusion: string | null;
      falsification_conditions: string | null;
      confidence: string | null;
      created_at: string;
    }[]
  >`
    select
      r.id::text,
      r.report_type,
      r.title,
      r.body_md,
      r.conclusion,
      r.falsification_conditions,
      r.confidence,
      r.created_at::text
    from reports r
    join runs ru on ru.id = r.run_id
    where r.report_type = 'weekly_summary'
      and ru.run_type = 'weekly'
      and ru.status = 'success'
    order by r.created_at desc
    limit 1
  `;

  if (!row[0]) {
    return null;
  }

  return {
    id: row[0].id,
    securityId: null,
    reportType: row[0].report_type,
    title: row[0].title,
    bodyMd: row[0].body_md,
    conclusion: row[0].conclusion,
    falsificationConditions: row[0].falsification_conditions,
    confidence: row[0].confidence,
    createdAt: row[0].created_at
  };
}

export async function fetchCitations(reportId: string): Promise<CitationRecord[]> {
  const sql = getSql();
  const rows = await sql<
    {
      claim_id: string;
      doc_version_id: string;
      page_ref: string | null;
      quote_text: string;
    }[]
  >`
    select
      claim_id,
      doc_version_id::text,
      page_ref,
      quote_text
    from citations
    where report_id = ${reportId}::uuid
    order by claim_id asc
  `;

  return rows.map((row) => ({
    claimId: row.claim_id,
    docVersionId: row.doc_version_id,
    pageRef: row.page_ref,
    quoteText: row.quote_text
  }));
}
