export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import {
  appendChatCitation,
  appendChatMessage,
  createChatSessionIfNeeded,
  fetchLatestAssistantAnswer,
  searchEvidenceFromReports
} from "@/lib/repository";

const SECURITY_ID_RE = /^(JP:\d{4}|US:\d+)$/;

function parsePeriodDays(raw: unknown): number | null {
  if (raw == null || raw === "") {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 3650) {
    return null;
  }
  return parsed;
}

function buildAnswer(
  question: string,
  evidence: Awaited<ReturnType<typeof searchEvidenceFromReports>>,
  options: { securityId?: string | null; periodDays?: number | null }
) {
  if (evidence.length === 0) {
    const conclusion = "既存引用に一致する根拠が不足しているため、現時点の結論は仮説扱いです。";
    const roots = [
      "一致する citation が 0 件でした。",
      "Evidence Vault 内の既存レポートだけでは主張を支持できません。",
      "追加の一次資料（決算短信/10-Q/適時開示）確認が必要です。"
    ];
    const counter = "一次情報で前提と逆の事実が確認された場合、この仮説は無効です。";
    const actions = [
      options.securityId
        ? `${options.securityId} の最新開示を再取得し、数値付き citation を3件以上確保する。`
        : "対象銘柄（例: JP:1301 / US:119）を指定して再質問する。",
      options.periodDays
        ? `過去${options.periodDays}日での根拠に限定して再判定する。`
        : "期間（例: 30日 / 90日）を指定して再質問する。"
    ];

    return {
      answer: [
        "## 結論",
        conclusion,
        "",
        "## 根拠3点",
        `1. ${roots[0]}`,
        `2. ${roots[1]}`,
        `3. ${roots[2]}`,
        "",
        "## 反証条件",
        counter,
        "",
        "## 次アクション",
        `1. ${actions[0]}`,
        `2. ${actions[1]}`
      ].join("\n"),
      citations: [] as Array<{ docVersionId: string; pageRef: string | null; quoteText: string; claimId: string | null }>
    };
  }

  const top = evidence[0];
  const citationPreview = top.citations.slice(0, 3);
  const reasonLines = [
    ...citationPreview.map(
      (c, idx) =>
        `${idx + 1}. [${idx + 1}] ${c.quoteText} (claim=${c.claimId ?? "-"}, doc=${c.docVersionId}, page=${c.pageRef ?? "-"})`
    )
  ];
  while (reasonLines.length < 3) {
    reasonLines.push(`${reasonLines.length + 1}. 補助根拠なし（追加 citation が必要）`);
  }

  const conclusion = top.report.conclusion ?? "監視継続";
  const counter =
    top.report.falsificationConditions ?? "一次情報との不一致、または前提条件の崩れを確認した場合。";
  const action1 = options.securityId
    ? `${options.securityId} の次回開示時に同一フォーマットで再評価する。`
    : "対象銘柄を指定して、結論の対象を明確にする。";
  const action2 = options.periodDays
    ? `過去${options.periodDays}日で citation 更新有無を確認する。`
    : "期間を指定して（例: 30/90日）、根拠の鮮度を再確認する。";

  return {
    answer: [
      `質問: ${question}`,
      "",
      "## 結論",
      conclusion,
      "",
      "## 根拠3点",
      ...reasonLines,
      "",
      "## 反証条件",
      counter,
      "",
      "## 次アクション",
      `1. ${action1}`,
      `2. ${action2}`
    ].join("\n"),
    citations: citationPreview.map((c) => ({
      docVersionId: c.docVersionId,
      pageRef: c.pageRef,
      quoteText: c.quoteText,
      claimId: c.claimId
    }))
  };
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
        { error: "securityId must be JP:#### or US:### format" },
        { status: 400 }
      );
    }
    const periodDays = parsePeriodDays(body?.periodDays);
    if (body?.periodDays != null && periodDays == null) {
      return NextResponse.json(
        { error: "periodDays must be integer between 1 and 3650" },
        { status: 400 }
      );
    }

    const sessionId = await createChatSessionIfNeeded(body?.sessionId);

    await appendChatMessage({
      sessionId,
      role: "user",
      content: question
    });

    const previous = await fetchLatestAssistantAnswer(sessionId);
    const evidence = await searchEvidenceFromReports(question, {
      securityId,
      periodDays
    });
    const built = buildAnswer(question, evidence, {
      securityId,
      periodDays
    });

    const assistantMessageId = await appendChatMessage({
      sessionId,
      role: "assistant",
      content: built.answer,
      answerBefore: previous,
      answerAfter: built.answer,
      changeReason:
        evidence.length === 0
          ? "no supporting citation found under current filters; kept as hypothesis"
          : "structured response generated with mapped citations"
    });

    for (const c of built.citations) {
      await appendChatCitation({
        messageId: assistantMessageId,
        docVersionId: c.docVersionId,
        pageRef: c.pageRef,
        quoteText: c.quoteText,
        claimId: c.claimId
      });
    }

    return NextResponse.json({
      sessionId,
      answerBefore: previous,
      answerAfter: built.answer,
      citations: built.citations
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
