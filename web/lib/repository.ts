import { randomUUID } from "node:crypto";

import { getSql } from "@/lib/db";
import {
  BacktestMetric,
  BacktestPoint,
  CitationRecord,
  EventRecord,
  ReportRecord,
  Top50Row
} from "@/lib/types";

export async function latestRunId(runType: "weekly" | "daily"): Promise<string | null> {
  const sql = getSql();
  const rows = await sql<{ id: string }[]>`
    select id::text as id
    from runs
    where run_type = ${runType} and status = 'success'
    order by finished_at desc nulls last, started_at desc
    limit 1
  `;
  return rows[0]?.id ?? null;
}

export async function fetchTop50(runId?: string): Promise<Top50Row[]> {
  const sql = getSql();
  const targetRun = runId ?? (await latestRunId("weekly"));
  if (!targetRun) {
    return [];
  }

  const rows = await sql<
    {
      rank: number;
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
      sector: string | null;
      combined_score: number;
      confidence: "High" | "Medium" | "Low";
      is_signal: boolean;
      entry_allowed: boolean;
      valid_until: string | null;
    }[]
  >`
    select
      t.rank,
      s.security_id,
      s.market,
      s.ticker,
      s.name,
      s.sector,
      sc.combined_score,
      sc.confidence,
      coalesce(sig.is_signal, false) as is_signal,
      coalesce(sig.entry_allowed, false) as entry_allowed,
      sig.valid_until::text
    from top50_membership t
    join securities s on s.id = t.security_id
    left join score_snapshots sc on sc.run_id = t.run_id and sc.security_id = s.id
    left join signals sig on sig.run_id = t.run_id and sig.security_id = s.id
    where t.run_id = ${targetRun}::uuid
    order by t.rank asc
  `;

  return rows.map((r) => ({
    rank: r.rank,
    securityId: r.security_id,
    market: r.market,
    ticker: r.ticker,
    name: r.name,
    sector: r.sector,
    score: Number(r.combined_score ?? 0),
    confidence: r.confidence,
    isSignal: r.is_signal,
    entryAllowed: r.entry_allowed,
    validUntil: r.valid_until
  }));
}

export async function fetchReportsBySecurity(securityId: string): Promise<ReportRecord[]> {
  const sql = getSql();
  const rows = await sql<
    {
      id: string;
      security_id: string | null;
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
      r.report_type,
      r.title,
      r.body_md,
      r.conclusion,
      r.falsification_conditions,
      r.confidence,
      r.created_at::text
    from reports r
    left join securities s on s.id = r.security_id
    where s.security_id = ${securityId}
    order by r.created_at desc
  `;

  return rows.map((r) => ({
    id: r.id,
    securityId: r.security_id,
    reportType: r.report_type,
    title: r.title,
    bodyMd: r.body_md,
    conclusion: r.conclusion,
    falsificationConditions: r.falsification_conditions,
    confidence: r.confidence,
    createdAt: r.created_at
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

  return rows.map((r) => ({
    claimId: r.claim_id,
    docVersionId: r.doc_version_id,
    pageRef: r.page_ref,
    quoteText: r.quote_text
  }));
}

export async function fetchDailyEvents(): Promise<EventRecord[]> {
  const sql = getSql();
  const runId = await latestRunId("daily");
  if (!runId) {
    return [];
  }

  const rows = await sql<
    {
      id: string;
      importance: "high" | "medium" | "low";
      event_type: string;
      event_time: string;
      title: string;
      summary: string;
      source_url: string | null;
    }[]
  >`
    select
      id::text,
      importance,
      event_type,
      event_time::text,
      title,
      summary,
      source_url
    from events
    where run_id = ${runId}::uuid
    order by event_time desc
    limit 250
  `;

  return rows.map((r) => ({
    id: r.id,
    importance: r.importance,
    eventType: r.event_type,
    eventTime: r.event_time,
    title: r.title,
    summary: r.summary,
    sourceUrl: r.source_url
  }));
}

export async function fetchBacktestData(): Promise<{
  metrics: BacktestMetric[];
  curve: BacktestPoint[];
}> {
  const sql = getSql();
  const runId = await latestRunId("weekly");
  if (!runId) {
    return { metrics: [], curve: [] };
  }

  const backtest = await sql<{ id: string }[]>`
    select id::text as id
    from backtest_runs
    where run_id = ${runId}::uuid
    order by created_at desc
    limit 1
  `;
  if (!backtest[0]) {
    return { metrics: [], curve: [] };
  }

  const metricsRows = await sql<
    {
      cost_profile: "zero" | "standard" | "strict";
      cagr: number;
      max_dd: number;
      sharpe: number;
      sortino: number;
      volatility: number;
      win_rate: number;
      avg_win: number;
      avg_loss: number;
      alpha_simple: number;
      information_ratio_simple: number;
    }[]
  >`
    select
      cost_profile,
      cagr,
      max_dd,
      sharpe,
      sortino,
      volatility,
      win_rate,
      avg_win,
      avg_loss,
      alpha_simple,
      information_ratio_simple
    from backtest_metrics
    where backtest_run_id = ${backtest[0].id}::uuid and market_scope = 'MIXED'
    order by cost_profile asc
  `;

  const curveRows = await sql<
    {
      cost_profile: string;
      trade_date: string;
      equity: number;
      benchmark_equity: number | null;
    }[]
  >`
    select
      cost_profile,
      trade_date::text,
      equity,
      benchmark_equity
    from backtest_equity_curve
    where backtest_run_id = ${backtest[0].id}::uuid
    order by trade_date asc
  `;

  return {
    metrics: metricsRows.map((r) => ({
      costProfile: r.cost_profile,
      cagr: Number(r.cagr ?? 0),
      maxDd: Number(r.max_dd ?? 0),
      sharpe: Number(r.sharpe ?? 0),
      sortino: Number(r.sortino ?? 0),
      volatility: Number(r.volatility ?? 0),
      winRate: Number(r.win_rate ?? 0),
      avgWin: Number(r.avg_win ?? 0),
      avgLoss: Number(r.avg_loss ?? 0),
      alphaSimple: Number(r.alpha_simple ?? 0),
      informationRatioSimple: Number(r.information_ratio_simple ?? 0)
    })),
    curve: curveRows.map((r) => ({
      costProfile: r.cost_profile,
      tradeDate: r.trade_date,
      equity: Number(r.equity ?? 0),
      benchmarkEquity: r.benchmark_equity == null ? null : Number(r.benchmark_equity)
    }))
  };
}

export async function createChatSessionIfNeeded(sessionId?: string): Promise<string> {
  const sql = getSql();
  if (sessionId) {
    return sessionId;
  }
  const id = randomUUID();
  await sql`
    insert into chat_sessions (id, title)
    values (${id}::uuid, ${"Q&A Session"})
  `;
  return id;
}

export async function appendChatMessage(input: {
  sessionId: string;
  runId?: string | null;
  role: "user" | "assistant" | "system";
  content: string;
  answerBefore?: string | null;
  answerAfter?: string | null;
  changeReason?: string | null;
}): Promise<string> {
  const sql = getSql();
  const id = randomUUID();
  if (input.runId) {
    await sql`
      insert into chat_messages (
        id, session_id, run_id, role, content, answer_before, answer_after, change_reason
      ) values (
        ${id}::uuid,
        ${input.sessionId}::uuid,
        ${input.runId}::uuid,
        ${input.role},
        ${input.content},
        ${input.answerBefore ?? null},
        ${input.answerAfter ?? null},
        ${input.changeReason ?? null}
      )
    `;
  } else {
    await sql`
      insert into chat_messages (
        id, session_id, role, content, answer_before, answer_after, change_reason
      ) values (
        ${id}::uuid,
        ${input.sessionId}::uuid,
        ${input.role},
        ${input.content},
        ${input.answerBefore ?? null},
        ${input.answerAfter ?? null},
        ${input.changeReason ?? null}
      )
    `;
  }
  return id;
}

export async function appendChatCitation(input: {
  messageId: string;
  docVersionId: string;
  pageRef?: string | null;
  quoteText: string;
  claimId?: string | null;
}) {
  const sql = getSql();
  await sql`
    insert into chat_message_citations (
      message_id, doc_version_id, page_ref, quote_text, claim_id
    ) values (
      ${input.messageId}::uuid,
      ${input.docVersionId}::uuid,
      ${input.pageRef ?? null},
      ${input.quoteText},
      ${input.claimId ?? null}
    )
  `;
}

export async function fetchLatestAssistantAnswer(sessionId: string): Promise<string | null> {
  const sql = getSql();
  const row = await sql<{ content: string }[]>`
    select content
    from chat_messages
    where session_id = ${sessionId}::uuid and role = 'assistant'
    order by created_at desc
    limit 1
  `;
  return row[0]?.content ?? null;
}

export async function searchEvidenceFromReports(query: string): Promise<{
  report: ReportRecord;
  citations: CitationRecord[];
}[]> {
  const sql = getSql();
  const q = `%${query}%`;
  const reports = await sql<
    {
      id: string;
      security_id: string | null;
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
      r.report_type,
      r.title,
      r.body_md,
      r.conclusion,
      r.falsification_conditions,
      r.confidence,
      r.created_at::text
    from reports r
    left join securities s on s.id = r.security_id
    where r.body_md ilike ${q}
       or r.title ilike ${q}
       or r.conclusion ilike ${q}
    order by r.created_at desc
    limit 5
  `;

  const out: {
    report: ReportRecord;
    citations: CitationRecord[];
  }[] = [];

  for (const r of reports) {
    const citations = await fetchCitations(r.id);
    out.push({
      report: {
        id: r.id,
        securityId: r.security_id,
        reportType: r.report_type,
        title: r.title,
        bodyMd: r.body_md,
        conclusion: r.conclusion,
        falsificationConditions: r.falsification_conditions,
        confidence: r.confidence,
        createdAt: r.created_at
      },
      citations
    });
  }

  return out;
}
