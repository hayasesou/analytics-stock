import { getSql } from "@/lib/db";
import type { EventRecord, WeeklyActionData } from "@/lib/types";
import { latestRunId } from "./core";
import { clampLookupLimit, isUndefinedRelationError } from "./shared";

export async function fetchWeeklyActionData(): Promise<WeeklyActionData> {
  const sql = getSql();
  const runRows = await sql<{ id: string }[]>`
    select id::text as id
    from runs
    where run_type = 'weekly' and status = 'success'
    order by finished_at desc nulls last, started_at desc
    limit 2
  `;

  const latestWeeklyRunId = runRows[0]?.id ?? null;
  const previousRunId = runRows[1]?.id ?? null;

  if (!latestWeeklyRunId) {
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
    where t.run_id = ${latestWeeklyRunId}::uuid
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
      where curr.run_id = ${latestWeeklyRunId}::uuid
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
    where br.run_id = ${latestWeeklyRunId}::uuid
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
      where run_id = ${latestWeeklyRunId}::uuid
      order by horizon_days asc
    `;
  } catch (error) {
    if (!isUndefinedRelationError(error, "signal_diagnostics_weekly")) {
      throw error;
    }
  }

  return {
    latestRunId: latestWeeklyRunId,
    previousRunId,
    highConfidenceTop10: topRows.map((row) => ({
      rank: row.rank,
      securityId: row.security_id,
      ticker: row.ticker,
      name: row.name,
      market: row.market
    })),
    liquidityChanges: liquidityRows.map((row) => ({
      securityId: row.security_id,
      ticker: row.ticker,
      name: row.name,
      market: row.market,
      previousLiquidityFlag: row.previous_liquidity_flag,
      currentLiquidityFlag: row.current_liquidity_flag
    })),
    strictMetric: strictRows[0]
      ? {
          cagr: Number(strictRows[0].cagr ?? 0),
          maxDd: Number(strictRows[0].max_dd ?? 0),
          sharpe: Number(strictRows[0].sharpe ?? 0)
        }
      : null,
    signalDiagnostics: diagnosticRows.map((row) => ({
      horizonDays: ([5, 20, 60].includes(row.horizon_days) ? row.horizon_days : 5) as 5 | 20 | 60,
      hitRate: Number(row.hit_rate ?? 0),
      medianReturn: row.median_return == null ? null : Number(row.median_return),
      p10Return: row.p10_return == null ? null : Number(row.p10_return),
      p90Return: row.p90_return == null ? null : Number(row.p90_return),
      sampleSize: Number(row.sample_size ?? 0)
    }))
  };
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

  return rows.map((row) => ({
    id: row.id,
    importance: row.importance,
    eventType: row.event_type,
    eventTime: row.event_time,
    title: row.title,
    summary: row.summary,
    sourceUrl: row.source_url
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

  return rows.map((row) => ({
    id: row.id,
    importance: row.importance,
    eventType: row.event_type,
    eventTime: row.event_time,
    title: row.title,
    summary: row.summary,
    sourceUrl: row.source_url
  }));
}
