import { getSql } from "@/lib/db";
import type { BacktestData, BacktestMeta, BacktestMetric, BacktestPoint, BacktestReasonCode, BacktestRunOption } from "@/lib/types";
import { latestRunId } from "./core";
import { clampBacktestRunLimit, clampLookbackDays, isUndefinedRelationError, isUuid } from "./shared";

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
