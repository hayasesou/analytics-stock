export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import {
  appendChatMessage,
  createChatSessionIfNeeded,
  createResearchArtifact,
  createResearchExternalInput,
  createResearchHypothesis,
  enqueueResearchAgentTask,
  fetchLatestAssistantAnswer,
  fetchResearchSessionDetail,
  fetchSecurityIdentity
} from "@/lib/repository";

const SECURITY_ID_RE = /^(JP:\d{4}|US:(?:\d+|[A-Z][A-Z0-9.-]{0,6}))$/;
const URL_RE = /(https?:\/\/[^\s]+)/gi;

function extractUrls(text: string): string[] {
  return Array.from(new Set(text.match(URL_RE) ?? [])).slice(0, 5);
}

function buildInitialHypotheses(input: {
  question: string;
  urls: string[];
  securityId: string | null;
  securityName: string | null;
}) {
  const target = input.securityId ?? input.securityName ?? "関連銘柄";
  const basis = input.urls.length > 0
    ? `URL ${input.urls.length} 件に含まれる材料`
    : "ユーザーの自然文入力";
  return [
    {
      stance: "watch" as const,
      horizonDays: 5,
      thesisMd: `${target} は短期的に再評価余地があります。まずは ${basis} から需給・ガイダンス・新規材料を確認する段階です。`,
      falsificationMd: `${basis} を確認しても業績、需給、規制、資金流入の裏付けが得られない場合は撤回します。`,
      confidence: 0.46
    },
    {
      stance: "bullish" as const,
      horizonDays: 20,
      thesisMd: `${target} に対して追加検証でポジティブ仮説が成立する可能性があります。特に収益成長、価格モメンタム、テーマ性の整合を確認します。`,
      falsificationMd: `決算・開示・価格推移のいずれかが悪化し、仮説の前提が崩れた場合は bullish 仮説を失効させます。`,
      confidence: 0.38
    }
  ];
}

function buildStructuredAnswer(input: {
  question: string;
  securityLabel: string | null;
  urls: string[];
  hypotheses: ReturnType<typeof buildInitialHypotheses>;
}): string {
  const lead = input.hypotheses[0];
  const urlLine = input.urls.length > 0 ? `URL ${input.urls.length} 件を session に登録しました。` : "テキスト入力を session に登録しました。";
  return [
    "## 結論",
    `${input.securityLabel ?? "対象未確定"} について、現時点では watch を主仮説として保存し、追加検証で bullish へ昇格できるかを見る方針です。`,
    "",
    "## 根拠",
    `1. ${urlLine}`,
    `2. 初期仮説として ${lead.horizonDays} 日 horizon の ${lead.stance} 仮説を生成しました。`,
    "3. 後続の research / critic / quant / code / portfolio タスクをキュー投入しました。",
    "",
    "## 反証",
    lead.falsificationMd,
    "",
    "## 次アクション",
    "1. 取り込んだ URL の抽出本文を確認し、対象銘柄・テーマの解像度を上げる。",
    "2. 仮説一覧で stance / falsification を確認し、必要なら artifact の SQL/Python を実行する。",
    "3. 5営業日後に validation を確認し、watch から validate へ進める。"
  ].join("\n");
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const question = String(body?.question ?? "").trim();
    if (!question) {
      return NextResponse.json({ error: "question is required" }, { status: 400 });
    }

    const securityIdRaw = body?.securityId == null ? null : String(body.securityId).trim();
    const securityId = securityIdRaw ? securityIdRaw : null;
    if (securityId && !SECURITY_ID_RE.test(securityId)) {
      return NextResponse.json(
        { error: "securityId must be JP:#### or US:<ticker>" },
        { status: 400 }
      );
    }

    const resolvedSecurity = securityId ? await fetchSecurityIdentity(securityId) : null;
    if (securityId && !resolvedSecurity) {
      return NextResponse.json({ error: `securityId not found: ${securityId}` }, { status: 404 });
    }

    const sessionId = await createChatSessionIfNeeded(body?.sessionId);
    const previous = await fetchLatestAssistantAnswer(sessionId);
    const userMessageId = await appendChatMessage({
      sessionId,
      role: "user",
      content: question
    });

    const urls = extractUrls(question);
    const textWithoutUrls = question.replace(URL_RE, " ").replace(/\s+/g, " ").trim();
    const externalInputIds: string[] = [];

    for (const url of urls) {
      externalInputIds.push(await createResearchExternalInput({
        sessionId,
        messageId: userMessageId,
        sourceType: "web_url",
        sourceUrl: url,
        rawText: question,
        extractedText: null,
        extractionStatus: "queued",
        metadata: {
          requestedBy: "web",
          trace: "research-chat"
        }
      }));
    }

    if (textWithoutUrls) {
      externalInputIds.push(await createResearchExternalInput({
        sessionId,
        messageId: userMessageId,
        sourceType: "text",
        rawText: textWithoutUrls,
        extractedText: textWithoutUrls,
        qualityGrade: "B",
        extractionStatus: "success",
        metadata: {
          requestedBy: "web"
        }
      }));
    }

    const initialHypotheses = buildInitialHypotheses({
      question,
      urls,
      securityId: resolvedSecurity?.securityId ?? securityId,
      securityName: resolvedSecurity?.name ?? null
    });

    const hypothesisIds: string[] = [];
    for (const hypothesis of initialHypotheses) {
      const hypothesisId = await createResearchHypothesis({
        sessionId,
        externalInputId: externalInputIds[0] ?? null,
        parentMessageId: userMessageId,
        stance: hypothesis.stance,
        horizonDays: hypothesis.horizonDays,
        thesisMd: hypothesis.thesisMd,
        falsificationMd: hypothesis.falsificationMd,
        confidence: hypothesis.confidence,
        status: "draft",
        metadata: {
          source: "research-chat-bootstrap",
          question
        },
        assets: resolvedSecurity ? [{
          assetClass: resolvedSecurity.market === "JP" ? "JP_EQ" : "US_EQ",
          symbolText: resolvedSecurity.securityId,
          weightHint: hypothesis.stance === "bullish" ? 0.2 : 0.0,
          confidence: hypothesis.confidence
        }] : []
      });
      hypothesisIds.push(hypothesisId);
    }

    const noteArtifactId = await createResearchArtifact({
      sessionId,
      hypothesisId: hypothesisIds[0] ?? null,
      artifactType: "note",
      title: "Initial Research Session Note",
      bodyMd: question,
      metadata: {
        source: "research-chat-bootstrap"
      }
    });

    await createResearchArtifact({
      sessionId,
      hypothesisId: hypothesisIds[0] ?? null,
      artifactType: "sql",
      title: "Validation SQL Draft",
      language: "sql",
      codeText: "-- TODO: replace with read-only validation query\nselect current_date as as_of_date;",
      metadata: {
        source: "research-chat-bootstrap",
        runnable: false
      }
    });

    const taskSpecs = [
      ["research.extract_input", "research"],
      ["research.generate_hypothesis", "research"],
      ["research.critic_review", "critic"],
      ["research.quant_plan", "quant"],
      ["research.code_generate", "code"],
      ["research.portfolio_build", "portfolio"]
    ] as const;

    for (const [taskType, assignedRole] of taskSpecs) {
      await enqueueResearchAgentTask({
        sessionId,
        taskType,
        assignedRole,
        dedupeKey: `${sessionId}:${taskType}`,
        payload: {
          session_id: sessionId,
          message_id: userMessageId,
          external_input_ids: externalInputIds,
          hypothesis_ids: hypothesisIds,
          artifact_id: noteArtifactId,
          question,
          security_id: resolvedSecurity?.securityId ?? securityId ?? null,
          requested_by: "web"
        }
      });
    }

    const answer = buildStructuredAnswer({
      question,
      securityLabel: resolvedSecurity ? `${resolvedSecurity.securityId} (${resolvedSecurity.ticker} / ${resolvedSecurity.name})` : null,
      urls,
      hypotheses: initialHypotheses
    });

    await appendChatMessage({
      sessionId,
      role: "assistant",
      content: answer,
      answerBefore: previous,
      answerAfter: answer,
      changeReason: "research session bootstrapped with initial hypotheses and queued tasks"
    });

    const session = await fetchResearchSessionDetail(sessionId);

    return NextResponse.json({
      sessionId,
      answerBefore: previous,
      answerAfter: answer,
      resolvedSecurity,
      urls,
      externalInputIds,
      hypothesisIds,
      session
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
