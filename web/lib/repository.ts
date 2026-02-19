import { randomUUID } from "node:crypto";

import { getSql } from "@/lib/db";
import type {
  BacktestData,
  BacktestMeta,
  BacktestMetric,
  BacktestPoint,
  BacktestReasonCode,
  BacktestRunOption,
  CitationRecord,
  ExecutionOrderIntent,
  ExecutionRiskSnapshot,
  EventRecord,
  ResearchAgentTask,
  ResearchFundamentalSnapshot,
  ResearchStrategy,
  ReportRecord,
  SecurityIdentity,
  SecurityTimelineData,
  Top50Row,
  WeeklyActionData
} from "@/lib/types";

function isUndefinedRelationError(error: unknown, relation: string): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { code?: string; message?: string };
  if (candidate.code === "42P01") {
    return true;
  }
  return typeof candidate.message === "string" && candidate.message.includes(`relation "${relation}" does not exist`);
}

function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function clampLookbackDays(days: number): number {
  if (!Number.isFinite(days)) {
    return 180;
  }
  return Math.min(3650, Math.max(1, Math.trunc(days)));
}

function clampLookupLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 5;
  }
  return Math.min(20, Math.max(1, Math.trunc(limit)));
}

function clampExecutionLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 50;
  }
  return Math.min(200, Math.max(1, Math.trunc(limit)));
}

function clampBacktestRunLimit(limit: number): number {
  if (!Number.isFinite(limit)) {
    return 20;
  }
  return Math.min(200, Math.max(20, Math.trunc(limit)));
}

function mapSecurityIdentityRow(row: {
  security_id: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
}): SecurityIdentity {
  return {
    securityId: row.security_id,
    market: row.market,
    ticker: row.ticker,
    name: row.name
  };
}

function isLegacyMockSecurity(row: { market: "JP" | "US"; name: string }): boolean {
  const name = (row.name ?? "").trim();
  if (row.market === "JP") {
    return /^JP Corp \d{4}$/.test(name);
  }
  return /^US Holdings \d+$/.test(name);
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function asBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string");
}

function extractFoldValidationSummary(
  metricsValue: unknown,
  artifactsValue: unknown
): Pick<
  ResearchStrategy,
  | "validationPassed"
  | "validationFoldCount"
  | "validationPrimaryProfile"
  | "foldSharpeFirst"
  | "foldSharpeLast"
  | "foldSharpeDelta"
  | "foldSharpeMin"
  | "foldSharpeMax"
  | "validationFailReasons"
  | "validationFolds"
  | "validationGates"
> {
  const metrics = asRecord(metricsValue);
  const artifacts = asRecord(artifactsValue);
  const validation = asRecord(artifacts?.validation);
  const gate = asRecord(validation?.gate);
  const summary = asRecord(validation?.summary);
  const policy = asRecord(validation?.policy);
  const gates = asRecord(policy?.gates);

  const primaryProfile =
    asString(metrics?.validation_primary_profile)
    ?? asString(gate?.primary_cost_profile)
    ?? "standard";

  const primarySummary = summary ? asRecord(summary[primaryProfile]) : null;
  const profileFoldCount = asNumber(primarySummary?.fold_count);
  const foldCountFromMetrics = asNumber(metrics?.validation_fold_count);

  const foldsRaw = Array.isArray(validation?.folds) ? validation.folds : [];
  const normalizedFolds: ResearchStrategy["validationFolds"] = [];
  const sharpeSeries: number[] = [];

  for (const foldRaw of foldsRaw) {
    const fold = asRecord(foldRaw);
    if (!fold) {
      continue;
    }
    const profiles = asRecord(fold.profiles);
    const normalizedProfiles: Record<string, { sharpe: number | null; cagr: number | null; maxDd: number | null; tradeCount: number | null }> = {};
    if (profiles) {
      for (const [profileName, rawProfileMetrics] of Object.entries(profiles)) {
        const profileMetrics = asRecord(rawProfileMetrics);
        if (!profileMetrics) {
          continue;
        }
        normalizedProfiles[profileName] = {
          sharpe: asNumber(profileMetrics.sharpe),
          cagr: asNumber(profileMetrics.cagr),
          maxDd: asNumber(profileMetrics.max_dd),
          tradeCount: asNumber(profileMetrics.trade_count)
        };
      }
    }

    const skipped = asBoolean(fold.skipped) ?? false;
    const primarySharpe = normalizedProfiles[primaryProfile]?.sharpe ?? null;
    if (!skipped && primarySharpe != null) {
      sharpeSeries.push(primarySharpe);
    }

    normalizedFolds.push({
      fold: asNumber(fold.fold) ?? normalizedFolds.length,
      trainStart: asString(fold.train_start) ?? "",
      trainEnd: asString(fold.train_end) ?? "",
      testStart: asString(fold.test_start) ?? "",
      testEnd: asString(fold.test_end) ?? "",
      signalCount: asNumber(fold.signal_count) ?? 0,
      momentumThreshold: asNumber(fold.momentum_threshold),
      skipped,
      skipReason: asString(fold.skip_reason),
      profiles: normalizedProfiles
    });
  }

  const foldSharpeFirst = sharpeSeries.length > 0 ? sharpeSeries[0] : null;
  const foldSharpeLast = sharpeSeries.length > 0 ? sharpeSeries[sharpeSeries.length - 1] : null;
  const foldSharpeMin = sharpeSeries.length > 0 ? Math.min(...sharpeSeries) : null;
  const foldSharpeMax = sharpeSeries.length > 0 ? Math.max(...sharpeSeries) : null;
  const foldSharpeDelta = (foldSharpeFirst != null && foldSharpeLast != null)
    ? foldSharpeLast - foldSharpeFirst
    : null;

  const failReasons = asStringArray(metrics?.validation_fail_reasons);
  const gateReasons = asStringArray(gate?.reasons);
  const validationGates = gates
    ? {
        minFoldCount: asNumber(gates.min_fold_count),
        minTradesPerFold: asNumber(gates.min_trades_per_fold),
        minSharpe: asNumber(gates.min_sharpe),
        minCagr: asNumber(gates.min_cagr),
        minMaxDd: asNumber(gates.min_max_dd)
      }
    : null;

  return {
    validationPassed: asBoolean(metrics?.validation_passed) ?? asBoolean(gate?.passed),
    validationFoldCount: foldCountFromMetrics ?? profileFoldCount ?? (sharpeSeries.length || null),
    validationPrimaryProfile: primaryProfile,
    foldSharpeFirst,
    foldSharpeLast,
    foldSharpeDelta,
    foldSharpeMin,
    foldSharpeMax,
    validationFailReasons: failReasons.length > 0 ? failReasons : gateReasons,
    validationFolds: normalizedFolds,
    validationGates
  };
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

export async function fetchSecurityIdentity(securityId: string): Promise<SecurityIdentity | null> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId).trim();
  if (!normalizedSecurityId) {
    return null;
  }

  const rows = await sql<
    {
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
    }[]
  >`
    select
      security_id,
      market,
      ticker,
      coalesce(name, security_id) as name
    from securities
    where security_id = ${normalizedSecurityId}
    limit 1
  `;
  const row = rows[0];
  if (!row || isLegacyMockSecurity({ market: row.market, name: row.name })) {
    return null;
  }
  return mapSecurityIdentityRow(row);
}

export async function resolveSecurityQuery(query: string, limit = 5): Promise<SecurityIdentity[]> {
  const sql = getSql();
  const normalizedQuery = decodeSecurityId(query).trim();
  if (!normalizedQuery) {
    return [];
  }

  const queryLike = `%${normalizedQuery}%`;
  const cappedLimit = clampLookupLimit(limit);
  const rows = await sql<
    {
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
      rank_bucket: number;
    }[]
  >`
    with candidates as (
      select
        s.security_id,
        s.market,
        s.ticker,
        coalesce(s.name, s.security_id) as name,
        case
          when upper(s.security_id) = upper(${normalizedQuery}) then 0
          when upper(s.ticker) = upper(${normalizedQuery}) then 1
          when upper(coalesce(s.name, '')) = upper(${normalizedQuery}) then 2
          when upper(s.security_id) like upper(${queryLike}) then 3
          when upper(s.ticker) like upper(${queryLike}) then 4
          when upper(coalesce(s.name, '')) like upper(${queryLike}) then 5
          else 99
        end as rank_bucket
      from securities s
      where
        upper(s.security_id) = upper(${normalizedQuery})
        or upper(s.ticker) = upper(${normalizedQuery})
        or upper(coalesce(s.name, '')) = upper(${normalizedQuery})
        or upper(s.security_id) like upper(${queryLike})
        or upper(s.ticker) like upper(${queryLike})
        or upper(coalesce(s.name, '')) like upper(${queryLike})
    )
    select
      security_id,
      market,
      ticker,
      name,
      rank_bucket
    from candidates
    where rank_bucket < 99
    order by rank_bucket asc, security_id asc
    limit ${cappedLimit}
  `;

  return rows
    .filter((r) => !isLegacyMockSecurity({ market: r.market, name: r.name }))
    .map((r) => mapSecurityIdentityRow(r));
}

export async function fetchSecurityTimeline(
  securityId: string,
  days = 180
): Promise<SecurityTimelineData | null> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId);
  const lookbackDays = clampLookbackDays(days);

  const securityRows = await sql<
    {
      id: string;
      market: "JP" | "US";
      name: string;
    }[]
  >`
    select
      id::text as id,
      market,
      coalesce(name, security_id) as name
    from securities
    where security_id = ${normalizedSecurityId}
    limit 1
  `;
  const security = securityRows[0];
  if (!security || isLegacyMockSecurity({ market: security.market, name: security.name })) {
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
    .filter((r) => !isLegacyMockSecurity({ market: r.market, name: r.name }))
    .map((r) => ({
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

  let diagnosticRows: {
    horizon_days: number;
    hit_rate: number;
    median_return: number | null;
    p10_return: number | null;
    p90_return: number | null;
    sample_size: number;
  }[] = [];
  try {
    diagnosticRows = await sql<
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
  } catch (error) {
    if (!isUndefinedRelationError(error, "signal_diagnostics_weekly")) {
      throw error;
    }
  }

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

export async function fetchLatestDailyEvents(limit = 10): Promise<EventRecord[]> {
  const sql = getSql();
  const targetLimit = clampLookupLimit(limit);

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
    with ranked as (
      select
        e.id::text as id,
        e.importance,
        e.event_type,
        e.event_time::text as event_time,
        e.title,
        e.summary,
        e.source_url,
        row_number() over (
          partition by coalesce(e.source_url, e.title), e.event_type, e.event_time
          order by e.created_at desc, e.id desc
        ) as rn
      from events e
      join runs r on r.id = e.run_id
      where r.run_type = 'daily'
        and r.status = 'success'
        and e.summary not ilike 'Mock event generated for baseline operation.%'
    )
    select
      id,
      importance,
      event_type,
      event_time,
      title,
      summary,
      source_url
    from ranked
    where rn = 1
    order by event_time desc
    limit ${targetLimit}
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

export async function fetchBacktestData(input?: {
  runId?: string | null;
  fallbackMode?: "none" | "latest_with_backtest";
}): Promise<BacktestData> {
  const sql = getSql();
  const requestedRunIdRaw = input?.runId?.trim() || null;
  const requestedRunId = requestedRunIdRaw && isUuid(requestedRunIdRaw) ? requestedRunIdRaw : null;
  const fallbackMode = input?.fallbackMode === "latest_with_backtest" ? "latest_with_backtest" : "none";

  type RunMetaRow = {
    run_id: string;
    status: string;
    started_at: string;
    finished_at: string | null;
    signals: number | null;
    backtest_profiles: number | null;
  };
  type LatestWithBacktestRow = RunMetaRow & {
    backtest_run_id: string;
  };

  const latestWeeklyRunId = await latestRunId("weekly");
  const latestWithBacktestRows = await sql<LatestWithBacktestRow[]>`
    select
      r.id::text as run_id,
      br.id::text as backtest_run_id,
      r.status,
      r.started_at::text as started_at,
      r.finished_at::text as finished_at,
      nullif(r.metadata->>'signals', '')::int as signals,
      nullif(r.metadata->>'backtest_profiles', '')::int as backtest_profiles
    from backtest_runs br
    join runs r on r.id = br.run_id
    where r.run_type = 'weekly'
      and r.status = 'success'
    order by coalesce(r.finished_at, r.started_at) desc, br.created_at desc
    limit 1
  `;
  const latestWithBacktest = latestWithBacktestRows[0] ?? null;
  const latestWithBacktestRunId = latestWithBacktest?.run_id ?? null;

  const buildMeta = (
    partial?: Partial<BacktestMeta>
  ): BacktestMeta => ({
    requestedRunId: requestedRunIdRaw,
    resolvedRunId: null,
    latestWeeklyRunId,
    latestWithBacktestRunId,
    resolvedSource: "none",
    reasonCode: "no_weekly_run",
    resolvedRunStatus: null,
    resolvedRunStartedAt: null,
    resolvedRunFinishedAt: null,
    resolvedRunSignals: null,
    resolvedRunBacktestProfiles: null,
    ...partial
  });

  const emptyResponse = (meta: BacktestMeta): BacktestData => ({
    metrics: [],
    curve: [],
    meta
  });

  const runMetaById = async (runId: string): Promise<RunMetaRow | null> => {
    const rows = await sql<RunMetaRow[]>`
      select
        r.id::text as run_id,
        r.status,
        r.started_at::text as started_at,
        r.finished_at::text as finished_at,
        nullif(r.metadata->>'signals', '')::int as signals,
        nullif(r.metadata->>'backtest_profiles', '')::int as backtest_profiles
      from runs r
      where r.id = ${runId}::uuid
        and r.run_type = 'weekly'
      limit 1
    `;
    return rows[0] ?? null;
  };

  const latestWeeklyMeta = latestWeeklyRunId ? await runMetaById(latestWeeklyRunId) : null;
  if (!latestWeeklyMeta && !requestedRunId) {
    return emptyResponse(buildMeta({ reasonCode: "no_weekly_run" }));
  }

  let targetRun: RunMetaRow | null = null;
  let resolvedSource: BacktestMeta["resolvedSource"] = "none";
  let fallbackReason: BacktestReasonCode | null = null;

  if (requestedRunIdRaw && !requestedRunId) {
    if (fallbackMode === "latest_with_backtest" && latestWithBacktest) {
      targetRun = latestWithBacktest;
      resolvedSource = "latest_with_backtest";
      fallbackReason = "requested_run_not_found";
    } else {
      return emptyResponse(buildMeta({ reasonCode: "requested_run_not_found" }));
    }
  } else if (requestedRunId) {
    const requestedMeta = await runMetaById(requestedRunId);
    if (!requestedMeta) {
      if (fallbackMode === "latest_with_backtest" && latestWithBacktest) {
        targetRun = latestWithBacktest;
        resolvedSource = "latest_with_backtest";
        fallbackReason = "requested_run_not_found";
      } else {
        return emptyResponse(buildMeta({ reasonCode: "requested_run_not_found" }));
      }
    } else {
      targetRun = requestedMeta;
      resolvedSource = "requested";
    }
  } else {
    targetRun = latestWeeklyMeta;
    resolvedSource = "latest_weekly";
  }

  if (!targetRun) {
    return emptyResponse(buildMeta({ reasonCode: "no_weekly_run" }));
  }

  const backtestRows = await sql<{ id: string }[]>`
    select id::text as id
    from backtest_runs
    where run_id = ${targetRun.run_id}::uuid
    order by created_at desc
    limit 1
  `;
  let resolvedBacktestRunId = backtestRows[0]?.id ?? null;
  if (!resolvedBacktestRunId && fallbackMode === "latest_with_backtest" && latestWithBacktest) {
    const latestHasDifferentRun = latestWithBacktest.run_id !== targetRun.run_id;
    if (latestHasDifferentRun) {
      fallbackReason = requestedRunId ? "requested_run_has_no_backtest" : "latest_weekly_has_no_backtest";
      targetRun = latestWithBacktest;
      resolvedSource = "latest_with_backtest";
      resolvedBacktestRunId = latestWithBacktest.backtest_run_id;
    }
  }

  if (!resolvedBacktestRunId) {
    const reason: BacktestReasonCode = requestedRunId
      ? "requested_run_has_no_backtest"
      : "latest_weekly_has_no_backtest";
    return emptyResponse(
      buildMeta({
        resolvedRunId: targetRun.run_id,
        resolvedSource,
        reasonCode: reason,
        resolvedRunStatus: targetRun.status,
        resolvedRunStartedAt: targetRun.started_at,
        resolvedRunFinishedAt: targetRun.finished_at,
        resolvedRunSignals: targetRun.signals,
        resolvedRunBacktestProfiles: targetRun.backtest_profiles
      })
    );
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
    where backtest_run_id = ${resolvedBacktestRunId}::uuid and market_scope = 'MIXED'
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
    where backtest_run_id = ${resolvedBacktestRunId}::uuid
    order by trade_date asc
  `;
  const tradeCountRows = await sql<{ trade_count: number }[]>`
    select count(*)::int as trade_count
    from backtest_trades
    where backtest_run_id = ${resolvedBacktestRunId}::uuid
  `;
  const tradeCount = Number(tradeCountRows[0]?.trade_count ?? 0);

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

  let reasonCode: BacktestReasonCode = fallbackReason ?? "ok";
  if (metricsRows.length === 0) {
    reasonCode = "no_metrics";
  } else if (curve.length === 0) {
    reasonCode = "no_curve";
  } else if (tradeCount === 0) {
    reasonCode = "no_signals";
  }

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
    curve,
    meta: buildMeta({
      resolvedRunId: targetRun.run_id,
      resolvedSource,
      reasonCode,
      resolvedRunStatus: targetRun.status,
      resolvedRunStartedAt: targetRun.started_at,
      resolvedRunFinishedAt: targetRun.finished_at,
      resolvedRunSignals: targetRun.signals,
      resolvedRunBacktestProfiles: targetRun.backtest_profiles
    })
  };
}

export async function fetchBacktestRunOptions(input?: {
  limit?: number | null;
}): Promise<BacktestRunOption[]> {
  const sql = getSql();
  const limit = clampBacktestRunLimit(input?.limit ?? 20);
  const rows = await sql<
    {
      run_id: string;
      status: string;
      started_at: string;
      finished_at: string | null;
      signals: number | null;
      backtest_profiles: number | null;
      has_backtest_run: boolean;
    }[]
  >`
    select
      r.id::text as run_id,
      r.status,
      r.started_at::text as started_at,
      r.finished_at::text as finished_at,
      nullif(r.metadata->>'signals', '')::int as signals,
      nullif(r.metadata->>'backtest_profiles', '')::int as backtest_profiles,
      (br.id is not null) as has_backtest_run
    from runs r
    left join lateral (
      select id
      from backtest_runs
      where run_id = r.id
      order by created_at desc
      limit 1
    ) br on true
    where r.run_type = 'weekly'
    order by coalesce(r.finished_at, r.started_at) desc
    limit ${limit}
  `;

  return rows.map((r) => ({
    runId: r.run_id,
    status: r.status,
    startedAt: r.started_at,
    finishedAt: r.finished_at,
    signals: r.signals == null ? null : Number(r.signals),
    backtestProfiles: r.backtest_profiles == null ? null : Number(r.backtest_profiles),
    hasBacktestRun: Boolean(r.has_backtest_run)
  }));
}

export async function fetchExecutionOrderIntents(input?: {
  status?: ExecutionOrderIntent["status"] | null;
  portfolioName?: string | null;
  limit?: number | null;
}): Promise<ExecutionOrderIntent[]> {
  const sql = getSql();
  const status = input?.status ?? null;
  const portfolioName = input?.portfolioName?.trim() || null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        intent_id: string;
        portfolio_id: string;
        portfolio_name: string;
        strategy_version_id: string | null;
        as_of: string;
        status: ExecutionOrderIntent["status"];
        reason: string | null;
        risk_checks: Record<string, unknown> | null;
        target_positions: unknown[] | null;
        created_at: string;
        approved_at: string | null;
        approved_by: string | null;
      }[]
    >`
      select
        oi.id::text as intent_id,
        oi.portfolio_id::text as portfolio_id,
        p.name as portfolio_name,
        oi.strategy_version_id::text as strategy_version_id,
        oi.as_of::text as as_of,
        oi.status,
        oi.reason,
        oi.risk_checks,
        oi.target_positions,
        oi.created_at::text as created_at,
        oi.approved_at::text as approved_at,
        oi.approved_by
      from order_intents oi
      join portfolios p on p.id = oi.portfolio_id
      where (${status}::text is null or oi.status = ${status})
        and (${portfolioName}::text is null or p.name = ${portfolioName})
      order by oi.created_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      intentId: r.intent_id,
      portfolioId: r.portfolio_id,
      portfolioName: r.portfolio_name,
      strategyVersionId: r.strategy_version_id,
      asOf: r.as_of,
      status: r.status,
      reason: r.reason,
      riskChecks: r.risk_checks ?? {},
      targetPositions: Array.isArray(r.target_positions) ? r.target_positions : [],
      createdAt: r.created_at,
      approvedAt: r.approved_at,
      approvedBy: r.approved_by
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "order_intents") ||
      isUndefinedRelationError(error, "portfolios")
    ) {
      return [];
    }
    throw error;
  }
}

export async function fetchExecutionRiskSnapshots(input?: {
  portfolioName?: string | null;
  limit?: number | null;
}): Promise<ExecutionRiskSnapshot[]> {
  const sql = getSql();
  const portfolioName = input?.portfolioName?.trim() || null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        portfolio_id: string;
        portfolio_name: string;
        as_of: string;
        equity: number;
        drawdown: number;
        sharpe_20d: number | null;
        gross_exposure: number | null;
        net_exposure: number | null;
        state: ExecutionRiskSnapshot["state"];
        triggers: Record<string, unknown> | null;
        created_at: string;
      }[]
    >`
      select
        rs.portfolio_id::text as portfolio_id,
        p.name as portfolio_name,
        rs.as_of::text as as_of,
        rs.equity,
        rs.drawdown,
        rs.sharpe_20d,
        rs.gross_exposure,
        rs.net_exposure,
        rs.state,
        rs.triggers,
        rs.created_at::text as created_at
      from risk_snapshots rs
      join portfolios p on p.id = rs.portfolio_id
      where (${portfolioName}::text is null or p.name = ${portfolioName})
      order by rs.as_of desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      portfolioId: r.portfolio_id,
      portfolioName: r.portfolio_name,
      asOf: r.as_of,
      equity: Number(r.equity ?? 0),
      drawdown: Number(r.drawdown ?? 0),
      sharpe20d: r.sharpe_20d == null ? null : Number(r.sharpe_20d),
      grossExposure: r.gross_exposure == null ? null : Number(r.gross_exposure),
      netExposure: r.net_exposure == null ? null : Number(r.net_exposure),
      state: r.state,
      triggers: r.triggers ?? {},
      createdAt: r.created_at
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "risk_snapshots") ||
      isUndefinedRelationError(error, "portfolios")
    ) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchStrategies(input?: {
  status?: ResearchStrategy["status"] | null;
  limit?: number | null;
}): Promise<ResearchStrategy[]> {
  const sql = getSql();
  const status = input?.status ?? null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        strategy_id: string;
        strategy_name: string;
        asset_scope: ResearchStrategy["assetScope"];
        status: ResearchStrategy["status"];
        updated_at: string;
        version_id: string | null;
        version: number | null;
        eval_type: ResearchStrategy["evalType"];
        sharpe: number | null;
        max_dd: number | null;
        cagr: number | null;
        eval_run_id: string | null;
        eval_metrics: Record<string, unknown> | null;
        eval_artifacts: Record<string, unknown> | null;
      }[]
    >`
      with latest_versions as (
        select distinct on (sv.strategy_id)
          sv.strategy_id,
          sv.id::text as version_id,
          sv.version
        from strategy_versions sv
        order by sv.strategy_id, sv.version desc
      ),
      latest_eval as (
        select distinct on (se.strategy_version_id)
          se.strategy_version_id,
          se.eval_type,
          nullif(se.metrics->>'sharpe', '')::double precision as sharpe,
          nullif(se.metrics->>'max_dd', '')::double precision as max_dd,
          nullif(se.metrics->>'cagr', '')::double precision as cagr,
          nullif(se.artifacts->>'run_id', '')::text as eval_run_id,
          se.metrics as eval_metrics,
          se.artifacts as eval_artifacts
        from strategy_evaluations se
        order by se.strategy_version_id, se.created_at desc
      )
      select
        s.id::text as strategy_id,
        s.name as strategy_name,
        s.asset_scope,
        s.status,
        s.updated_at::text as updated_at,
        lv.version_id,
        lv.version,
        le.eval_type,
        le.sharpe,
        le.max_dd,
        le.cagr,
        le.eval_run_id,
        le.eval_metrics,
        le.eval_artifacts
      from strategies s
      left join latest_versions lv on lv.strategy_id = s.id
      left join latest_eval le on le.strategy_version_id = lv.version_id::uuid
      where (${status}::text is null or s.status = ${status})
      order by s.updated_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      ...extractFoldValidationSummary(r.eval_metrics, r.eval_artifacts),
      strategyId: r.strategy_id,
      strategyName: r.strategy_name,
      assetScope: r.asset_scope,
      status: r.status,
      updatedAt: r.updated_at,
      versionId: r.version_id,
      version: r.version,
      evalRunId: r.eval_run_id,
      evalType: r.eval_type,
      sharpe: r.sharpe == null ? null : Number(r.sharpe),
      maxDd: r.max_dd == null ? null : Number(r.max_dd),
      cagr: r.cagr == null ? null : Number(r.cagr)
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "strategies") ||
      isUndefinedRelationError(error, "strategy_versions") ||
      isUndefinedRelationError(error, "strategy_evaluations")
    ) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchFundamentalSnapshots(input?: {
  rating?: ResearchFundamentalSnapshot["rating"] | null;
  limit?: number | null;
}): Promise<ResearchFundamentalSnapshot[]> {
  const sql = getSql();
  const rating = input?.rating ?? null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        security_id: string;
        ticker: string;
        name: string;
        market: "JP" | "US";
        source: string;
        as_of_date: string;
        rating: "A" | "B" | "C";
        confidence: "High" | "Medium" | "Low" | null;
        summary: string;
        created_at: string;
      }[]
    >`
      select
        s.security_id,
        s.ticker,
        coalesce(s.name, s.security_id) as name,
        s.market,
        fs.source,
        fs.as_of_date::text as as_of_date,
        fs.rating,
        fs.confidence,
        fs.summary,
        fs.created_at::text as created_at
      from fundamental_snapshots fs
      join securities s on s.id = fs.security_id
      where (${rating}::text is null or fs.rating = ${rating})
      order by fs.as_of_date desc, fs.created_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      securityId: r.security_id,
      ticker: r.ticker,
      name: r.name,
      market: r.market,
      source: r.source,
      asOfDate: r.as_of_date,
      rating: r.rating,
      confidence: r.confidence,
      summary: r.summary,
      createdAt: r.created_at
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "fundamental_snapshots") ||
      isUndefinedRelationError(error, "securities")
    ) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchAgentTasks(input?: {
  status?: ResearchAgentTask["status"] | null;
  limit?: number | null;
}): Promise<ResearchAgentTask[]> {
  const sql = getSql();
  const status = input?.status ?? null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        id: string;
        task_type: string;
        priority: number;
        status: ResearchAgentTask["status"];
        payload: Record<string, unknown> | null;
        cost_usd: number | null;
        created_at: string;
        started_at: string | null;
        finished_at: string | null;
      }[]
    >`
      select
        id::text,
        task_type,
        priority,
        status,
        payload,
        cost_usd,
        created_at::text as created_at,
        started_at::text as started_at,
        finished_at::text as finished_at
      from agent_tasks
      where (${status}::text is null or status = ${status})
      order by created_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      id: r.id,
      taskType: r.task_type,
      priority: Number(r.priority ?? 0),
      status: r.status,
      strategyName: typeof r.payload?.strategy_name === "string" ? r.payload.strategy_name : null,
      securityId: typeof r.payload?.security_id === "string" ? r.payload.security_id : null,
      costUsd: r.cost_usd == null ? null : Number(r.cost_usd),
      createdAt: r.created_at,
      startedAt: r.started_at,
      finishedAt: r.finished_at
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "agent_tasks")) {
      return [];
    }
    throw error;
  }
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
