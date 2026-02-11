import { randomUUID } from "node:crypto";

import { getSql } from "@/lib/db";
import {
  BacktestMetric,
  BacktestPoint,
  CitationRecord,
  EventRecord,
  ReportRecord,
  SecurityTimelineData,
  Top50Row,
  WeeklyActionData
} from "@/lib/types";

function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function clampLookbackDays(days: number): number {
  if (!Number.isFinite(days)) {
    return 180;
  }
  return Math.min(3650, Math.max(1, Math.trunc(days)));
}

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
      prev_rank: number | null;
      rank_delta: number | null;
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
      sector: string | null;
      quality: number | null;
      growth: number | null;
      value: number | null;
      momentum: number | null;
      catalyst: number | null;
      combined_score: number;
      score_delta: number | null;
      edge_score: number | null;
      missing_ratio: number | null;
      liquidity_flag: boolean | null;
      selection_reason: string | null;
      confidence: "High" | "Medium" | "Low";
      is_signal: boolean;
      entry_allowed: boolean;
      signal_reason: string | null;
      valid_until: string | null;
    }[]
  >`
    with target_run as (
      select id, coalesce(finished_at, started_at) as ordering_time
      from runs
      where id = ${targetRun}::uuid
      limit 1
    ),
    prev_run as (
      select r.id
      from runs r
      join target_run t on true
      where r.run_type = 'weekly'
        and r.status = 'success'
        and r.id <> t.id
        and coalesce(r.finished_at, r.started_at) < t.ordering_time
      order by coalesce(r.finished_at, r.started_at) desc, r.started_at desc
      limit 1
    )
    select
      t.rank,
      prev_t.rank as prev_rank,
      (prev_t.rank - t.rank) as rank_delta,
      s.security_id,
      s.market,
      s.ticker,
      s.name,
      s.sector,
      sc.quality,
      sc.growth,
      sc.value,
      sc.momentum,
      sc.catalyst,
      sc.combined_score,
      (sc.combined_score - prev_sc.combined_score) as score_delta,
      nullif(sc.flags->>'edge_score', '')::double precision as edge_score,
      sc.missing_ratio,
      sc.liquidity_flag,
      t.reason as selection_reason,
      sc.confidence,
      coalesce(sig.is_signal, false) as is_signal,
      coalesce(sig.entry_allowed, false) as entry_allowed,
      sig.reason as signal_reason,
      sig.valid_until::text
    from top50_membership t
    join securities s on s.id = t.security_id
    left join score_snapshots sc on sc.run_id = t.run_id and sc.security_id = s.id
    left join signals sig on sig.run_id = t.run_id and sig.security_id = s.id
    left join prev_run pr on true
    left join top50_membership prev_t on prev_t.run_id = pr.id and prev_t.security_id = s.id
    left join score_snapshots prev_sc on prev_sc.run_id = pr.id and prev_sc.security_id = s.id
    where t.run_id = ${targetRun}::uuid
    order by t.rank asc
  `;

  return rows.map((r) => ({
    rank: r.rank,
    rankPrev: r.prev_rank,
    rankDelta: r.rank_delta,
    securityId: r.security_id,
    market: r.market,
    ticker: r.ticker,
    name: r.name,
    sector: r.sector,
    score: Number(r.combined_score ?? 0),
    scoreDelta: r.score_delta == null ? null : Number(r.score_delta),
    edgeScore: Number(r.edge_score ?? 0),
    quality: Number(r.quality ?? 0),
    growth: Number(r.growth ?? 0),
    value: Number(r.value ?? 0),
    momentum: Number(r.momentum ?? 0),
    catalyst: Number(r.catalyst ?? 0),
    missingRatio: Number(r.missing_ratio ?? 0),
    liquidityFlag: Boolean(r.liquidity_flag),
    selectionReason: r.selection_reason,
    signalReason: r.signal_reason,
    confidence: r.confidence,
    isSignal: r.is_signal,
    entryAllowed: r.entry_allowed,
    validUntil: r.valid_until
  }));
}

export async function fetchSecurityTimeline(
  securityId: string,
  days = 180
): Promise<SecurityTimelineData | null> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId);
  const lookbackDays = clampLookbackDays(days);

  const securityRows = await sql<{ id: string }[]>`
    select id::text as id
    from securities
    where security_id = ${normalizedSecurityId}
    limit 1
  `;
  const security = securityRows[0];
  if (!security) {
    return null;
  }

  const [priceRows, signalRows, eventRows] = await Promise.all([
    sql<
      {
        trade_date: string;
        close_raw: number;
      }[]
    >`
      select
        trade_date::text as trade_date,
        close_raw
      from prices_daily
      where security_id = ${security.id}::uuid
        and trade_date >= current_date - (${lookbackDays}::int * interval '1 day')
      order by trade_date asc
    `,
    sql<
      {
        as_of_date: string;
        is_signal: boolean;
        entry_allowed: boolean;
        reason: string | null;
        rank: number | null;
        confidence: "High" | "Medium" | "Low" | null;
        valid_until: string | null;
      }[]
    >`
      select
        as_of_date::text as as_of_date,
        is_signal,
        entry_allowed,
        reason,
        rank,
        confidence,
        valid_until::text as valid_until
      from signals
      where security_id = ${security.id}::uuid
        and as_of_date >= current_date - (${lookbackDays}::int * interval '1 day')
      order by as_of_date asc
    `,
    sql<
      {
        event_time: string;
        event_date: string;
        title: string;
        summary: string;
        importance: "high" | "medium" | "low";
        event_type: string;
        source_url: string | null;
      }[]
    >`
      select
        event_time::text as event_time,
        (event_time at time zone 'UTC')::date::text as event_date,
        title,
        summary,
        importance,
        event_type,
        source_url
      from events
      where security_id = ${security.id}::uuid
        and event_time >= now() - (${lookbackDays}::int * interval '1 day')
      order by event_time asc
    `
  ]);

  return {
    securityId: normalizedSecurityId,
    days: lookbackDays,
    prices: priceRows.map((r) => ({
      date: r.trade_date,
      close: Number(r.close_raw ?? 0)
    })),
    signals: signalRows.map((r) => ({
      date: r.as_of_date,
      isSignal: Boolean(r.is_signal),
      entryAllowed: Boolean(r.entry_allowed),
      reason: r.reason,
      rank: r.rank,
      confidence: r.confidence,
      validUntil: r.valid_until
    })),
    events: eventRows.map((r) => ({
      date: r.event_date,
      eventTime: r.event_time,
      title: r.title,
      summary: r.summary,
      importance: r.importance,
      eventType: r.event_type,
      sourceUrl: r.source_url
    }))
  };
}

export async function fetchReportsBySecurity(securityId: string): Promise<ReportRecord[]> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId);
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
    where s.security_id = ${normalizedSecurityId}
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

export async function fetchWeeklyActionData(): Promise<WeeklyActionData> {
  const sql = getSql();
  const runRows = await sql<{ id: string }[]>`
    select id::text as id
    from runs
    where run_type = 'weekly' and status = 'success'
    order by finished_at desc nulls last, started_at desc
    limit 2
  `;

  const latestRunId = runRows[0]?.id ?? null;
  const previousRunId = runRows[1]?.id ?? null;

  if (!latestRunId) {
    return {
      latestRunId: null,
      previousRunId: null,
      highConfidenceTop10: [],
      liquidityChanges: [],
      strictMetric: null,
      signalDiagnostics: []
    };
  }

  const topRows = await sql<
    {
      rank: number;
      security_id: string;
      ticker: string;
      name: string;
      market: "JP" | "US";
    }[]
  >`
    select
      t.rank,
      s.security_id,
      s.ticker,
      s.name,
      s.market
    from top50_membership t
    join score_snapshots sc on sc.run_id = t.run_id and sc.security_id = t.security_id
    join securities s on s.id = t.security_id
    where t.run_id = ${latestRunId}::uuid
      and t.rank <= 10
      and sc.confidence = 'High'
    order by t.rank asc
  `;

  let liquidityRows: {
    security_id: string;
    ticker: string;
    name: string;
    market: "JP" | "US";
    previous_liquidity_flag: boolean;
    current_liquidity_flag: boolean;
  }[] = [];
  if (previousRunId) {
    liquidityRows = await sql<
      {
        security_id: string;
        ticker: string;
        name: string;
        market: "JP" | "US";
        previous_liquidity_flag: boolean;
        current_liquidity_flag: boolean;
      }[]
    >`
      select
        s.security_id,
        s.ticker,
        s.name,
        s.market,
        prev.liquidity_flag as previous_liquidity_flag,
        curr.liquidity_flag as current_liquidity_flag
      from score_snapshots curr
      join score_snapshots prev
        on prev.security_id = curr.security_id
       and prev.run_id = ${previousRunId}::uuid
      join securities s on s.id = curr.security_id
      where curr.run_id = ${latestRunId}::uuid
        and curr.liquidity_flag is distinct from prev.liquidity_flag
      order by s.market asc, s.security_id asc
      limit 100
    `;
  }

  const strictRows = await sql<
    {
      cagr: number | null;
      max_dd: number | null;
      sharpe: number | null;
    }[]
  >`
    select
      m.cagr,
      m.max_dd,
      m.sharpe
    from backtest_runs br
    join backtest_metrics m on m.backtest_run_id = br.id
    where br.run_id = ${latestRunId}::uuid
      and m.cost_profile = 'strict'
      and m.market_scope = 'MIXED'
    order by br.created_at desc
    limit 1
  `;

  const diagnosticRows = await sql<
    {
      horizon_days: number;
      hit_rate: number;
      median_return: number | null;
      p10_return: number | null;
      p90_return: number | null;
      sample_size: number;
    }[]
  >`
    select
      horizon_days,
      hit_rate,
      median_return,
      p10_return,
      p90_return,
      sample_size
    from signal_diagnostics_weekly
    where run_id = ${latestRunId}::uuid
    order by horizon_days asc
  `;

  return {
    latestRunId,
    previousRunId,
    highConfidenceTop10: topRows.map((r) => ({
      rank: r.rank,
      securityId: r.security_id,
      ticker: r.ticker,
      name: r.name,
      market: r.market
    })),
    liquidityChanges: liquidityRows.map((r) => ({
      securityId: r.security_id,
      ticker: r.ticker,
      name: r.name,
      market: r.market,
      previousLiquidityFlag: r.previous_liquidity_flag,
      currentLiquidityFlag: r.current_liquidity_flag
    })),
    strictMetric: strictRows[0]
      ? {
          cagr: Number(strictRows[0].cagr ?? 0),
          maxDd: Number(strictRows[0].max_dd ?? 0),
          sharpe: Number(strictRows[0].sharpe ?? 0)
        }
      : null,
    signalDiagnostics: diagnosticRows.map((r) => ({
      horizonDays: ([5, 20, 60].includes(r.horizon_days) ? r.horizon_days : 5) as 5 | 20 | 60,
      hitRate: Number(r.hit_rate ?? 0),
      medianReturn: r.median_return == null ? null : Number(r.median_return),
      p10Return: r.p10_return == null ? null : Number(r.p10_return),
      p90Return: r.p90_return == null ? null : Number(r.p90_return),
      sampleSize: Number(r.sample_size ?? 0)
    }))
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

  const rawCurve = curveRows.map((r) => ({
    costProfile: r.cost_profile,
    tradeDate: r.trade_date,
    equity: Number(r.equity ?? 0),
    benchmarkEquity: r.benchmark_equity == null ? null : Number(r.benchmark_equity)
  }));

  const peakByCost = new Map<string, number>();
  const curve = rawCurve.map((p) => {
    const prevPeak = peakByCost.get(p.costProfile) ?? p.equity;
    const peak = Math.max(prevPeak, p.equity);
    peakByCost.set(p.costProfile, peak);
    const drawdown = peak > 0 ? p.equity / peak - 1 : 0;
    return {
      ...p,
      drawdown
    };
  });

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
    curve
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

export async function searchEvidenceFromReports(
  query: string,
  options?: { securityId?: string | null; periodDays?: number | null }
): Promise<{
  report: ReportRecord;
  citations: CitationRecord[];
}[]> {
  const sql = getSql();
  const q = `%${query}%`;
  const securityId = options?.securityId?.trim() ? decodeSecurityId(options.securityId.trim()) : null;
  const periodDays = options?.periodDays && Number.isFinite(options.periodDays)
    ? Math.max(1, Math.trunc(options.periodDays))
    : null;
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
    where (
      r.body_md ilike ${q}
      or r.title ilike ${q}
      or r.conclusion ilike ${q}
    )
      and (${securityId}::text is null or s.security_id = ${securityId})
      and (${periodDays}::int is null or r.created_at >= now() - (${periodDays}::int * interval '1 day'))
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
