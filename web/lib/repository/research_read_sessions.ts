import { getSql } from "@/lib/db";
import type { ResearchChatMessageRecord, ResearchChatSessionDetail, ResearchSessionListItem } from "@/lib/types";
import { fetchResearchArtifacts, fetchResearchHypotheses, fetchResearchInputs } from "./research_read_records";

export async function fetchResearchSessionMessages(sessionId: string): Promise<ResearchChatMessageRecord[]> {
  const sql = getSql();
  const rows = await sql<
    {
      id: string;
      session_id: string;
      role: ResearchChatMessageRecord["role"];
      content: string;
      answer_before: string | null;
      answer_after: string | null;
      change_reason: string | null;
      created_at: string;
    }[]
  >`
    select
      id::text,
      session_id::text,
      role,
      content,
      answer_before,
      answer_after,
      change_reason,
      created_at::text
    from chat_messages
    where session_id = ${sessionId}::uuid
    order by created_at asc
  `;
  return rows.map((row) => ({
    id: row.id,
    sessionId: row.session_id,
    role: row.role,
    content: row.content,
    answerBefore: row.answer_before,
    answerAfter: row.answer_after,
    changeReason: row.change_reason,
    createdAt: row.created_at
  }));
}

export async function fetchResearchSessionDetail(sessionId: string): Promise<ResearchChatSessionDetail | null> {
  const sql = getSql();
  const sessionRows = await sql<{ id: string; title: string | null }[]>`
    select id::text, title
    from chat_sessions
    where id = ${sessionId}::uuid
    limit 1
  `;
  const session = sessionRows[0];
  if (!session) {
    return null;
  }
  const [messages, inputs, hypotheses, artifacts] = await Promise.all([
    fetchResearchSessionMessages(sessionId),
    fetchResearchInputs({ sessionId, limit: 100 }),
    fetchResearchHypotheses({ sessionId, limit: 100 }),
    fetchResearchArtifacts({ sessionId, limit: 100 })
  ]);
  return {
    sessionId: session.id,
    title: session.title,
    messages,
    inputs,
    hypotheses,
    artifacts
  };
}

export async function fetchResearchSessions(limit: number = 50): Promise<ResearchSessionListItem[]> {
  const sql = getSql();
  const normalizedLimit = Math.min(200, Math.max(1, Math.trunc(limit)));
  const rows = await sql<
    {
      session_id: string;
      title: string | null;
      created_at: string;
      message_count: number;
      input_count: number;
      hypothesis_count: number;
      latest_assistant_message: string | null;
    }[]
  >`
    select
      s.id::text as session_id,
      s.title,
      s.created_at::text,
      (select count(*)::int from chat_messages m where m.session_id = s.id) as message_count,
      (select count(*)::int from external_inputs e where e.session_id = s.id) as input_count,
      (select count(*)::int from research_hypotheses h where h.session_id = s.id) as hypothesis_count,
      (
        select m.content
        from chat_messages m
        where m.session_id = s.id
          and m.role = 'assistant'
        order by m.created_at desc
        limit 1
      ) as latest_assistant_message
    from chat_sessions s
    where exists (
      select 1
      from external_inputs e
      where e.session_id = s.id
    )
    order by s.created_at desc
    limit ${normalizedLimit}
  `;
  return rows.map((row) => ({
    sessionId: row.session_id,
    title: row.title,
    createdAt: row.created_at,
    messageCount: row.message_count,
    inputCount: row.input_count,
    hypothesisCount: row.hypothesis_count,
    latestAssistantMessage: row.latest_assistant_message
  }));
}
