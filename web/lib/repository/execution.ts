import { getSql } from "@/lib/db";
import type { EdgeStateRow, EdgeTrendPoint, ExecutionOrderIntent, ExecutionRiskSnapshot } from "@/lib/types";
import { clampEdgeLimit, clampExecutionLimit, clampLookbackDays, isUndefinedRelationError } from "./shared";

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

export async function fetchEdgeStates(input?: {
  marketScope?: EdgeStateRow["marketScope"] | null;
  strategyName?: string | null;
  symbol?: string | null;
  limit?: number | null;
}): Promise<EdgeStateRow[]> {
  const sql = getSql();
  const marketScope = input?.marketScope ?? null;
  const strategyName = input?.strategyName?.trim() || null;
  const symbol = input?.symbol?.trim() || null;
  const limit = clampEdgeLimit(input?.limit ?? 120);

  try {
    const rows = await sql<
      {
        strategy_name: string;
        strategy_version_id: string | null;
        strategy_status: EdgeStateRow["strategyStatus"];
        market_scope: EdgeStateRow["marketScope"];
        symbol: string;
        observed_at: string;
        edge_score: number;
        expected_net_edge_bps: number | null;
        distance_to_entry_bps: number | null;
        confidence: number | null;
        market_regime: string | null;
        explain: string | null;
        risk_state: EdgeStateRow["riskState"];
        risk_drawdown: number | null;
        risk_sharpe_20d: number | null;
        cooldown_until: string | null;
        meta: Record<string, unknown> | null;
      }[]
    >`
      select
        es.strategy_name,
        es.strategy_version_id::text as strategy_version_id,
        s.status as strategy_status,
        es.market_scope,
        es.symbol,
        es.observed_at::text as observed_at,
        es.edge_score,
        coalesce(es.expected_net_edge, es.expected_net_edge_bps) as expected_net_edge_bps,
        coalesce(es.distance_to_entry, es.distance_to_entry_bps) as distance_to_entry_bps,
        es.confidence,
        coalesce(es.market_regime, es.market_scope) as market_regime,
        es.explain,
        rs.state as risk_state,
        rs.drawdown as risk_drawdown,
        rs.sharpe_20d as risk_sharpe_20d,
        rs.cooldown_until::text as cooldown_until,
        coalesce(es.meta, '{}'::jsonb) as meta
      from edge_states es
      left join strategy_versions sv on sv.id = es.strategy_version_id
      left join strategies s on s.id = sv.strategy_id
      left join lateral (
        select
          srs.state,
          srs.drawdown,
          srs.sharpe_20d,
          srs.cooldown_until
        from strategy_risk_snapshots srs
        where srs.strategy_version_id = es.strategy_version_id
        order by srs.as_of desc, srs.created_at desc
        limit 1
      ) rs on true
      where (${marketScope}::text is null or es.market_scope = ${marketScope})
        and (${strategyName}::text is null or es.strategy_name = ${strategyName})
        and (${symbol}::text is null or es.symbol = ${symbol})
      order by es.observed_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      strategyName: r.strategy_name,
      strategyVersionId: r.strategy_version_id,
      strategyStatus: r.strategy_status,
      marketScope: r.market_scope,
      symbol: r.symbol,
      observedAt: r.observed_at,
      edgeScore: Number(r.edge_score ?? 0),
      expectedNetEdgeBps: r.expected_net_edge_bps == null ? null : Number(r.expected_net_edge_bps),
      distanceToEntryBps: r.distance_to_entry_bps == null ? null : Number(r.distance_to_entry_bps),
      confidence: r.confidence == null ? null : Number(r.confidence),
      marketRegime: r.market_regime,
      explain: r.explain,
      riskState: r.risk_state,
      riskDrawdown: r.risk_drawdown == null ? null : Number(r.risk_drawdown),
      riskSharpe20d: r.risk_sharpe_20d == null ? null : Number(r.risk_sharpe_20d),
      cooldownUntil: r.cooldown_until,
      meta: r.meta ?? {}
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "edge_states") ||
      isUndefinedRelationError(error, "strategy_versions") ||
      isUndefinedRelationError(error, "strategies") ||
      isUndefinedRelationError(error, "strategy_risk_snapshots")
    ) {
      return [];
    }
    throw error;
  }
}

export async function fetchEdgeTrend(input: {
  strategyName: string;
  symbol?: string | null;
  marketScope?: EdgeStateRow["marketScope"] | null;
  limit?: number | null;
}): Promise<EdgeTrendPoint[]> {
  const sql = getSql();
  const strategyName = input.strategyName.trim();
  if (!strategyName) {
    return [];
  }
  const symbol = input.symbol?.trim() || null;
  const marketScope = input.marketScope ?? null;
  const limit = clampEdgeLimit(input.limit ?? 120);

  try {
    const rows = await sql<
      {
        strategy_name: string;
        strategy_version_id: string | null;
        symbol: string;
        observed_at: string;
        edge_score: number;
        expected_net_edge_bps: number | null;
        distance_to_entry_bps: number | null;
        confidence: number | null;
        risk_state: EdgeTrendPoint["riskState"];
      }[]
    >`
      select
        es.strategy_name,
        es.strategy_version_id::text as strategy_version_id,
        es.symbol,
        es.observed_at::text as observed_at,
        es.edge_score,
        coalesce(es.expected_net_edge, es.expected_net_edge_bps) as expected_net_edge_bps,
        coalesce(es.distance_to_entry, es.distance_to_entry_bps) as distance_to_entry_bps,
        es.confidence,
        rs.state as risk_state
      from edge_states es
      left join lateral (
        select state
        from strategy_risk_snapshots srs
        where srs.strategy_version_id = es.strategy_version_id
        order by srs.as_of desc, srs.created_at desc
        limit 1
      ) rs on true
      where es.strategy_name = ${strategyName}
        and (${marketScope}::text is null or es.market_scope = ${marketScope})
        and (${symbol}::text is null or es.symbol = ${symbol})
      order by es.observed_at desc
      limit ${limit}
    `;

    return rows.map((r) => ({
      strategyName: r.strategy_name,
      strategyVersionId: r.strategy_version_id,
      symbol: r.symbol,
      observedAt: r.observed_at,
      edgeScore: Number(r.edge_score ?? 0),
      expectedNetEdgeBps: r.expected_net_edge_bps == null ? null : Number(r.expected_net_edge_bps),
      distanceToEntryBps: r.distance_to_entry_bps == null ? null : Number(r.distance_to_entry_bps),
      confidence: r.confidence == null ? null : Number(r.confidence),
      riskState: r.risk_state
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "edge_states") ||
      isUndefinedRelationError(error, "strategy_risk_snapshots")
    ) {
      return [];
    }
    throw error;
  }
}

