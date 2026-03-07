export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { enqueueResearchAgentTask } from "@/lib/repository";

export async function POST(
  req: NextRequest,
  { params }: { params: { artifactId: string } }
) {
  try {
    const body = await req.json();
    const sessionId = typeof body?.sessionId === "string" ? body.sessionId : "";
    const chartType = typeof body?.chartType === "string" ? body.chartType.trim() : "";
    const instruction = typeof body?.instruction === "string" ? body.instruction.trim() : "";
    if (!sessionId) {
      return NextResponse.json({ error: "sessionId is required" }, { status: 400 });
    }
    const taskId = await enqueueResearchAgentTask({
      sessionId,
      taskType: "research.chart_generate",
      assignedRole: "artifact",
      payload: {
        session_id: sessionId,
        artifact_id: params.artifactId,
        requested_by: "web",
        chart_type: chartType || null,
        chart_instruction: instruction || null,
      },
      dedupeKey: `${sessionId}:chart_generate:${params.artifactId}:${chartType || "-"}:${instruction || "-"}`,
    });
    return NextResponse.json({ taskId });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
