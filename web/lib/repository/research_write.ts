import { getSql } from "@/lib/db";
import { randomUUID } from "node:crypto";
import type { CitationRecord, ReportRecord, ResearchArtifactRecord, ResearchHypothesisRecord, ResearchInputRecord } from "@/lib/types";
import { fetchCitations } from "./reports";
import { asRecord, decodeSecurityId, isUndefinedRelationError } from "./shared";

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

function asObjectRecord(value: unknown): Record<string, unknown> {
  return asRecord(value) ?? {};
}

export async function createResearchExternalInput(input: {
  sessionId: string;
  messageId?: string | null;
  sourceType: ResearchInputRecord["sourceType"];
  sourceUrl?: string | null;
  rawText?: string | null;
  extractedText?: string | null;
  qualityGrade?: ResearchInputRecord["qualityGrade"];
  extractionStatus?: ResearchInputRecord["extractionStatus"];
  userComment?: string | null;
  metadata?: Record<string, unknown>;
}): Promise<string> {
  const sql = getSql();
  const id = randomUUID();
  await sql`
    insert into external_inputs (
      id,
      session_id,
      message_id,
      source_type,
      source_url,
      raw_text,
      extracted_text,
      quality_grade,
      extraction_status,
      user_comment,
      metadata
    ) values (
      ${id}::uuid,
      ${input.sessionId}::uuid,
      ${input.messageId ?? null}::uuid,
      ${input.sourceType},
      ${input.sourceUrl ?? null},
      ${input.rawText ?? null},
      ${input.extractedText ?? null},
      ${input.qualityGrade ?? null},
      ${input.extractionStatus ?? "queued"},
      ${input.userComment ?? null},
      ${JSON.stringify(input.metadata ?? {})}::jsonb
    )
  `;
  return id;
}

export async function createResearchHypothesis(input: {
  sessionId: string;
  externalInputId?: string | null;
  parentMessageId?: string | null;
  stance: ResearchHypothesisRecord["stance"];
  horizonDays: number;
  thesisMd: string;
  falsificationMd: string;
  confidence?: number | null;
  status?: ResearchHypothesisRecord["status"];
  isFavorite?: boolean;
  metadata?: Record<string, unknown>;
  assets?: Array<{
    assetClass: "JP_EQ" | "US_EQ" | "CRYPTO";
    securityId?: string | null;
    symbolText?: string | null;
    weightHint?: number | null;
    confidence?: number | null;
  }>;
}): Promise<string> {
  const sql = getSql();
  const id = randomUUID();
  await sql.begin(async (trx) => {
    await trx`
      insert into research_hypotheses (
        id,
        session_id,
        external_input_id,
        parent_message_id,
        stance,
        horizon_days,
        thesis_md,
        falsification_md,
        confidence,
        status,
        is_favorite,
        metadata
      ) values (
        ${id}::uuid,
        ${input.sessionId}::uuid,
        ${input.externalInputId ?? null}::uuid,
        ${input.parentMessageId ?? null}::uuid,
        ${input.stance},
        ${input.horizonDays},
        ${input.thesisMd},
        ${input.falsificationMd},
        ${input.confidence ?? null},
        ${input.status ?? "draft"},
        ${input.isFavorite ?? false},
        ${JSON.stringify(input.metadata ?? {})}::jsonb
      )
    `;
    for (const asset of input.assets ?? []) {
      await trx`
        insert into research_hypothesis_assets (
          id,
          hypothesis_id,
          asset_class,
          security_id,
          symbol_text,
          weight_hint,
          confidence
        ) values (
          ${randomUUID()}::uuid,
          ${id}::uuid,
          ${asset.assetClass},
          ${asset.securityId ?? null}::uuid,
          ${asset.symbolText ?? null},
          ${asset.weightHint ?? null},
          ${asset.confidence ?? null}
        )
      `;
    }
  });
  return id;
}

export async function createResearchArtifact(input: {
  sessionId: string;
  hypothesisId?: string | null;
  artifactType: ResearchArtifactRecord["artifactType"];
  title: string;
  bodyMd?: string | null;
  codeText?: string | null;
  language?: string | null;
  isFavorite?: boolean;
  createdByTaskId?: string | null;
  metadata?: Record<string, unknown>;
}): Promise<string> {
  const sql = getSql();
  const id = randomUUID();
  await sql`
    insert into research_artifacts (
      id,
      session_id,
      hypothesis_id,
      artifact_type,
      title,
      body_md,
      code_text,
      language,
      is_favorite,
      created_by_task_id,
      metadata
    ) values (
      ${id}::uuid,
      ${input.sessionId}::uuid,
      ${input.hypothesisId ?? null}::uuid,
      ${input.artifactType},
      ${input.title},
      ${input.bodyMd ?? null},
      ${input.codeText ?? null},
      ${input.language ?? null},
      ${input.isFavorite ?? false},
      ${input.createdByTaskId ?? null}::uuid,
      ${JSON.stringify(input.metadata ?? {})}::jsonb
    )
  `;
  return id;
}

export async function enqueueResearchAgentTask(input: {
  sessionId: string;
  taskType: string;
  payload: Record<string, unknown>;
  priority?: number;
  parentTaskId?: string | null;
  assignedRole?: string | null;
  dedupeKey?: string | null;
}): Promise<string> {
  const sql = getSql();
  const id = randomUUID();
  await sql`
    insert into agent_tasks (
      id,
      session_id,
      parent_task_id,
      task_type,
      priority,
      status,
      payload,
      dedupe_key,
      assigned_role
    ) values (
      ${id}::uuid,
      ${input.sessionId}::uuid,
      ${input.parentTaskId ?? null}::uuid,
      ${input.taskType},
      ${input.priority ?? 100},
      'queued',
      ${JSON.stringify(input.payload)}::jsonb,
      ${input.dedupeKey ?? null},
      ${input.assignedRole ?? null}
    )
  `;
  return id;
}
