import { getSql } from "@/lib/db";
import type { ResearchArtifactRecord, ResearchArtifactRunStatus, ResearchHypothesisOutcomeRecord, ResearchHypothesisRecord, ResearchInputRecord } from "@/lib/types";
import { asObjectRecord, isUndefinedRelationError } from "./shared";

export async function fetchResearchInputs(options?: {
  sessionId?: string | null;
  limit?: number;
}): Promise<ResearchInputRecord[]> {
  const sql = getSql();
  const limit = Math.min(200, Math.max(1, Math.trunc(options?.limit ?? 50)));
  try {
    const rows = await sql<
      {
        id: string;
        session_id: string;
        message_id: string | null;
        source_type: ResearchInputRecord["sourceType"];
        source_url: string | null;
        raw_text: string | null;
        extracted_text: string | null;
        quality_grade: ResearchInputRecord["qualityGrade"];
        extraction_status: ResearchInputRecord["extractionStatus"];
        user_comment: string | null;
        metadata: unknown;
        created_at: string;
      }[]
    >`
      select
        id::text,
        session_id::text,
        message_id::text,
        source_type,
        source_url,
        raw_text,
        extracted_text,
        quality_grade,
        extraction_status,
        user_comment,
        metadata,
        created_at::text
      from external_inputs
      where (${options?.sessionId ?? null}::uuid is null or session_id = ${options?.sessionId ?? null}::uuid)
      order by created_at desc
      limit ${limit}
    `;
    return rows.map((row) => ({
      id: row.id,
      sessionId: row.session_id,
      messageId: row.message_id,
      sourceType: row.source_type,
      sourceUrl: row.source_url,
      rawText: row.raw_text,
      extractedText: row.extracted_text,
      qualityGrade: row.quality_grade,
      extractionStatus: row.extraction_status,
      userComment: row.user_comment,
      metadata: asObjectRecord(row.metadata),
      createdAt: row.created_at
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "external_inputs")) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchHypotheses(options?: {
  sessionId?: string | null;
  limit?: number;
}): Promise<ResearchHypothesisRecord[]> {
  const sql = getSql();
  const limit = Math.min(200, Math.max(1, Math.trunc(options?.limit ?? 50)));
  try {
    const rows = await sql<
      {
        id: string;
        session_id: string;
        external_input_id: string | null;
        parent_message_id: string | null;
        stance: ResearchHypothesisRecord["stance"];
        horizon_days: number;
        thesis_md: string;
        falsification_md: string;
        confidence: number | null;
        status: ResearchHypothesisRecord["status"];
        is_favorite: boolean;
        version: number;
        metadata: unknown;
        created_at: string;
      }[]
    >`
      select
        id::text,
        session_id::text,
        external_input_id::text,
        parent_message_id::text,
        stance,
        horizon_days,
        thesis_md,
        falsification_md,
        confidence::float8 as confidence,
        status,
        is_favorite,
        version,
        metadata,
        created_at::text
      from research_hypotheses
      where (${options?.sessionId ?? null}::uuid is null or session_id = ${options?.sessionId ?? null}::uuid)
      order by created_at desc
      limit ${limit}
    `;
    const hypothesisIds = rows.map((row) => row.id);
    const assets = hypothesisIds.length > 0
      ? await sql<
          {
            id: string;
            hypothesis_id: string;
            asset_class: "JP_EQ" | "US_EQ" | "CRYPTO";
            security_id: string | null;
            symbol_text: string | null;
            ticker: string | null;
            name: string | null;
            market: "JP" | "US" | null;
            weight_hint: number | null;
            confidence: number | null;
          }[]
        >`
          select
            a.id::text,
            a.hypothesis_id::text,
            a.asset_class,
            s.security_id,
            a.symbol_text,
            s.ticker,
            s.name,
            s.market,
            a.weight_hint::float8 as weight_hint,
            a.confidence::float8 as confidence
          from research_hypothesis_assets a
          left join securities s on s.id = a.security_id
          where a.hypothesis_id = any(${hypothesisIds}::uuid[])
          order by a.created_at asc
        `
      : [];
    const assetsByHypothesis = new Map<string, ResearchHypothesisRecord["assets"]>();
    for (const asset of assets) {
      const bucket = assetsByHypothesis.get(asset.hypothesis_id) ?? [];
      bucket.push({
        id: asset.id,
        assetClass: asset.asset_class,
        securityId: asset.security_id,
        symbolText: asset.symbol_text,
        ticker: asset.ticker,
        name: asset.name,
        market: asset.market,
        weightHint: asset.weight_hint,
        confidence: asset.confidence
      });
      assetsByHypothesis.set(asset.hypothesis_id, bucket);
    }
    return rows.map((row) => ({
      id: row.id,
      sessionId: row.session_id,
      externalInputId: row.external_input_id,
      parentMessageId: row.parent_message_id,
      stance: row.stance,
      horizonDays: row.horizon_days,
      thesisMd: row.thesis_md,
      falsificationMd: row.falsification_md,
      confidence: row.confidence,
      status: row.status,
      isFavorite: row.is_favorite,
      version: row.version,
      metadata: asObjectRecord(row.metadata),
      createdAt: row.created_at,
      assets: assetsByHypothesis.get(row.id) ?? []
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "research_hypotheses")) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchArtifacts(options?: {
  sessionId?: string | null;
  limit?: number;
}): Promise<ResearchArtifactRecord[]> {
  const sql = getSql();
  const limit = Math.min(200, Math.max(1, Math.trunc(options?.limit ?? 50)));
  try {
    const rows = await sql<
      {
        id: string;
        session_id: string;
        hypothesis_id: string | null;
        artifact_type: ResearchArtifactRecord["artifactType"];
        title: string;
        body_md: string | null;
        code_text: string | null;
        language: string | null;
        is_favorite: boolean;
        created_by_task_id: string | null;
        metadata: unknown;
        created_at: string;
        run_id: string | null;
        run_status: ResearchArtifactRunStatus | null;
        stdout_text: string | null;
        stderr_text: string | null;
        result_json: unknown;
        output_r2_key: string | null;
        run_created_at: string | null;
      }[]
    >`
      select
        a.id::text,
        a.session_id::text,
        a.hypothesis_id::text,
        a.artifact_type,
        a.title,
        a.body_md,
        a.code_text,
        a.language,
        a.is_favorite,
        a.created_by_task_id::text,
        a.metadata,
        a.created_at::text,
        ar.id::text as run_id,
        ar.run_status,
        ar.stdout_text,
        ar.stderr_text,
        ar.result_json,
        ar.output_r2_key,
        ar.created_at::text as run_created_at
      from research_artifacts a
      left join lateral (
        select *
        from research_artifact_runs ar
        where ar.artifact_id = a.id
        order by ar.created_at desc
        limit 1
      ) ar on true
      where (${options?.sessionId ?? null}::uuid is null or a.session_id = ${options?.sessionId ?? null}::uuid)
      order by a.created_at desc
      limit ${limit}
    `;
    return rows.map((row) => ({
      id: row.id,
      sessionId: row.session_id,
      hypothesisId: row.hypothesis_id,
      artifactType: row.artifact_type,
      title: row.title,
      bodyMd: row.body_md,
      codeText: row.code_text,
      language: row.language,
      isFavorite: row.is_favorite,
      createdByTaskId: row.created_by_task_id,
      metadata: asObjectRecord(row.metadata),
      createdAt: row.created_at,
      latestRun: row.run_id ? {
        id: row.run_id,
        runStatus: row.run_status ?? "pending",
        stdoutText: row.stdout_text,
        stderrText: row.stderr_text,
        resultJson: asObjectRecord(row.result_json),
        outputR2Key: row.output_r2_key,
        createdAt: row.run_created_at ?? row.created_at
      } : null
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "research_artifacts")) {
      return [];
    }
    throw error;
  }
}

export async function fetchResearchValidation(options?: {
  sessionId?: string | null;
  limit?: number;
}): Promise<ResearchHypothesisOutcomeRecord[]> {
  const sql = getSql();
  const limit = Math.min(200, Math.max(1, Math.trunc(options?.limit ?? 50)));
  try {
    const rows = await sql<
      {
        id: string;
        hypothesis_id: string;
        checked_at: string;
        ret_1d: number | null;
        ret_5d: number | null;
        ret_20d: number | null;
        mfe: number | null;
        mae: number | null;
        outcome_label: ResearchHypothesisOutcomeRecord["outcomeLabel"];
        summary_md: string | null;
        metadata: unknown;
        session_id: string | null;
        stance: ResearchHypothesisRecord["stance"] | null;
        horizon_days: number | null;
        thesis_md: string | null;
        confidence: number | null;
        status: ResearchHypothesisRecord["status"] | null;
      }[]
    >`
      select
        o.id::text,
        o.hypothesis_id::text,
        o.checked_at::text,
        o.ret_1d::float8 as ret_1d,
        o.ret_5d::float8 as ret_5d,
        o.ret_20d::float8 as ret_20d,
        o.mfe::float8 as mfe,
        o.mae::float8 as mae,
        o.outcome_label,
        o.summary_md,
        o.metadata,
        h.session_id::text,
        h.stance,
        h.horizon_days,
        h.thesis_md,
        h.confidence::float8 as confidence,
        h.status
      from research_hypothesis_outcomes o
      left join research_hypotheses h on h.id = o.hypothesis_id
      where (${options?.sessionId ?? null}::uuid is null or h.session_id = ${options?.sessionId ?? null}::uuid)
      order by o.checked_at desc
      limit ${limit}
    `;
    return rows.map((row) => ({
      id: row.id,
      hypothesisId: row.hypothesis_id,
      checkedAt: row.checked_at,
      ret1d: row.ret_1d,
      ret5d: row.ret_5d,
      ret20d: row.ret_20d,
      mfe: row.mfe,
      mae: row.mae,
      outcomeLabel: row.outcome_label,
      summaryMd: row.summary_md,
      metadata: asObjectRecord(row.metadata),
      hypothesis: row.session_id && row.stance && row.horizon_days && row.thesis_md ? {
        sessionId: row.session_id,
        stance: row.stance,
        horizonDays: row.horizon_days,
        thesisMd: row.thesis_md,
        confidence: row.confidence,
        status: row.status ?? "draft"
      } : null
    }));
  } catch (error) {
    if (isUndefinedRelationError(error, "research_hypothesis_outcomes")) {
      return [];
    }
    throw error;
  }
}
