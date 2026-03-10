import { getSql } from "@/lib/db";
import type { ResearchAgentTask, ResearchFundamentalSnapshot, ResearchLifecycleReview, ResearchStrategy } from "@/lib/types";
import { asBoolean, asNumber, asRecord, clampExecutionLimit, extractFoldValidationSummary, isUndefinedRelationError } from "./shared";

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
        live_candidate: boolean;
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
        paper_metrics: Record<string, unknown> | null;
        last_lifecycle_action: string | null;
        last_lifecycle_reason: string | null;
        last_lifecycle_by: string | null;
        last_lifecycle_at: string | null;
        last_lifecycle_recheck_after: string | null;
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
      ),
      latest_paper_eval as (
        select distinct on (se.strategy_version_id)
          se.strategy_version_id,
          se.metrics as paper_metrics
        from strategy_evaluations se
        where se.eval_type = 'paper'
        order by se.strategy_version_id, se.created_at desc
      ),
      latest_lifecycle as (
        select distinct on (slr.strategy_id)
          slr.strategy_id,
          slr.action as last_lifecycle_action,
          slr.reason as last_lifecycle_reason,
          slr.acted_by as last_lifecycle_by,
          slr.acted_at::text as last_lifecycle_at,
          slr.recheck_after::text as last_lifecycle_recheck_after
        from strategy_lifecycle_reviews slr
        order by slr.strategy_id, slr.acted_at desc, slr.created_at desc
      )
      select
        s.id::text as strategy_id,
        s.name as strategy_name,
        s.asset_scope,
        s.status,
        s.live_candidate,
        s.updated_at::text as updated_at,
        lv.version_id,
        lv.version,
        le.eval_type,
        le.sharpe,
        le.max_dd,
        le.cagr,
        le.eval_run_id,
        le.eval_metrics,
        le.eval_artifacts,
        pe.paper_metrics,
        ll.last_lifecycle_action,
        ll.last_lifecycle_reason,
        ll.last_lifecycle_by,
        ll.last_lifecycle_at,
        ll.last_lifecycle_recheck_after
      from strategies s
      left join latest_versions lv on lv.strategy_id = s.id
      left join latest_eval le on le.strategy_version_id = lv.version_id::uuid
      left join latest_paper_eval pe on pe.strategy_version_id = lv.version_id::uuid
      left join latest_lifecycle ll on ll.strategy_id = s.id
      where (${status}::text is null or s.status = ${status})
      order by s.updated_at desc
      limit ${limit}
    `;

    return rows.map((row) => {
      const paperMetrics = asRecord(row.paper_metrics);
      return {
        ...extractFoldValidationSummary(row.eval_metrics, row.eval_artifacts),
        strategyId: row.strategy_id,
        strategyName: row.strategy_name,
        assetScope: row.asset_scope,
        status: row.status,
        liveCandidate: Boolean(row.live_candidate),
        updatedAt: row.updated_at,
        versionId: row.version_id,
        version: row.version,
        evalRunId: row.eval_run_id,
        evalType: row.eval_type,
        sharpe: row.sharpe == null ? null : Number(row.sharpe),
        maxDd: row.max_dd == null ? null : Number(row.max_dd),
        cagr: row.cagr == null ? null : Number(row.cagr),
        paperDays: asNumber(paperMetrics?.paper_days),
        paperRoundTrips: asNumber(paperMetrics?.round_trips),
        paperSharpe20d: asNumber(paperMetrics?.sharpe_20d),
        paperMaxDrawdown: asNumber(paperMetrics?.max_drawdown),
        paperGateDaysOk: asBoolean(paperMetrics?.days_ok),
        paperGateRoundTripsOk: asBoolean(paperMetrics?.round_trips_ok),
        paperGateRiskOk: asBoolean(paperMetrics?.risk_ok),
        lastLifecycleAction: row.last_lifecycle_action,
        lastLifecycleReason: row.last_lifecycle_reason,
        lastLifecycleBy: row.last_lifecycle_by,
        lastLifecycleAt: row.last_lifecycle_at,
        lastLifecycleRecheckAfter: row.last_lifecycle_recheck_after
      };
    });
  } catch (error) {
    if (
      isUndefinedRelationError(error, "strategies") ||
      isUndefinedRelationError(error, "strategy_versions") ||
      isUndefinedRelationError(error, "strategy_evaluations") ||
      isUndefinedRelationError(error, "strategy_lifecycle_reviews")
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

    return rows.map((row) => ({
      securityId: row.security_id,
      ticker: row.ticker,
      name: row.name,
      market: row.market,
      source: row.source,
      asOfDate: row.as_of_date,
      rating: row.rating,
      confidence: row.confidence,
      summary: row.summary,
      createdAt: row.created_at
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

    return rows.map((row) => ({
      id: row.id,
      taskType: row.task_type,
      priority: Number(row.priority ?? 0),
      status: row.status,
      strategyName: typeof row.payload?.strategy_name === "string" ? row.payload.strategy_name : null,
      securityId: typeof row.payload?.security_id === "string" ? row.payload.security_id : null,
      costUsd: row.cost_usd == null ? null : Number(row.cost_usd),
      createdAt: row.created_at,
      startedAt: row.started_at,
      finishedAt: row.finished_at
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "agent_tasks")) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchLifecycleReviews(input?: {
  strategyId?: string | null;
  limit?: number | null;
}): Promise<ResearchLifecycleReview[]> {
  const sql = getSql();
  const strategyId = input?.strategyId?.trim() || null;
  const limit = clampExecutionLimit(input?.limit ?? 50);

  try {
    const rows = await sql<
      {
        id: string;
        strategy_id: string;
        strategy_name: string;
        strategy_version_id: string | null;
        action: string;
        from_status: string;
        to_status: string;
        live_candidate: boolean;
        reason: string | null;
        recheck_condition: string | null;
        recheck_after: string | null;
        acted_by: string;
        acted_at: string;
      }[]
    >`
      select
        slr.id::text as id,
        slr.strategy_id::text as strategy_id,
        s.name as strategy_name,
        slr.strategy_version_id::text as strategy_version_id,
        slr.action,
        slr.from_status,
        slr.to_status,
        slr.live_candidate,
        slr.reason,
        slr.recheck_condition,
        slr.recheck_after::text as recheck_after,
        slr.acted_by,
        slr.acted_at::text as acted_at
      from strategy_lifecycle_reviews slr
      join strategies s on s.id = slr.strategy_id
      where (${strategyId}::uuid is null or slr.strategy_id = ${strategyId}::uuid)
      order by slr.acted_at desc, slr.created_at desc
      limit ${limit}
    `;
    return rows.map((row) => ({
      id: row.id,
      strategyId: row.strategy_id,
      strategyName: row.strategy_name,
      strategyVersionId: row.strategy_version_id,
      action: row.action,
      fromStatus: row.from_status,
      toStatus: row.to_status,
      liveCandidate: Boolean(row.live_candidate),
      reason: row.reason,
      recheckCondition: row.recheck_condition,
      recheckAfter: row.recheck_after,
      actedBy: row.acted_by,
      actedAt: row.acted_at
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "strategy_lifecycle_reviews") ||
      isUndefinedRelationError(error, "strategies")
    ) {
      return [];
    }
    throw error;
  }
}
