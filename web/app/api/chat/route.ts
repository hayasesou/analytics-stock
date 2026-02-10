export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import {
  appendChatCitation,
  appendChatMessage,
  createChatSessionIfNeeded,
  fetchLatestAssistantAnswer,
  searchEvidenceFromReports
} from "@/lib/repository";

function buildAnswer(question: string, evidence: Awaited<ReturnType<typeof searchEvidenceFromReports>>) {
  if (evidence.length === 0) {
    return {
      answer: [
        "変更前: 既存回答なし",
        "変更後: 現時点では既存のEvidence Vaultから直接根拠を提示できません。",
        "変更理由: 既存レポート/引用で質問語句に一致する一次根拠が不足。追加調査（IR/開示）を実行してください。"
      ].join("\n"),
      citations: [] as Array<{ docVersionId: string; pageRef: string | null; quoteText: string; claimId: string | null }>
    };
  }

  const top = evidence[0];
  const citationPreview = top.citations.slice(0, 3);

  const lines = [
    `質問: ${question}`,
    "",
    "既存根拠（引用）:",
    ...citationPreview.map(
      (c) => `- [${c.claimId}] doc=${c.docVersionId} page=${c.pageRef ?? "-"} quote=${c.quoteText}`
    ),
    "",
    "変更前: 既存回答なし",
    `変更後: ${top.report.title} を根拠に、当面の結論は「${top.report.conclusion ?? "監視継続"}」。`,
    "変更理由: 既存レポートの引用付きClaimを優先し、未引用主張は採用していません。",
    "",
    `反証条件: ${top.report.falsificationConditions ?? "一次情報の更新で矛盾が出た場合に再判定"}`
  ];

  return {
    answer: lines.join("\n"),
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

    const sessionId = await createChatSessionIfNeeded(body?.sessionId);

    await appendChatMessage({
      sessionId,
      role: "user",
      content: question
    });

    const previous = await fetchLatestAssistantAnswer(sessionId);
    const evidence = await searchEvidenceFromReports(question);
    const built = buildAnswer(question, evidence);

    const assistantMessageId = await appendChatMessage({
      sessionId,
      role: "assistant",
      content: built.answer,
      answerBefore: previous,
      answerAfter: built.answer,
      changeReason:
        evidence.length === 0
          ? "no existing citation matched; additional primary-source research is required"
          : "updated with existing cited evidence"
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
