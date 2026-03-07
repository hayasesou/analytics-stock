export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { enqueueResearchAgentTask, fetchResearchSessionDetail } from "@/lib/repository";

export async function GET(
  _req: NextRequest,
  { params }: { params: { sessionId: string } }
) {
  try {
    const session = await fetchResearchSessionDetail(params.sessionId);
    if (!session) {
      return NextResponse.json({ error: "session not found" }, { status: 404 });
    }
    return NextResponse.json(session);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}

export async function POST(
  req: NextRequest,
  { params }: { params: { sessionId: string } }
) {
  try {
    const body = await req.json();
    const action = typeof body?.action === "string" ? body.action : "";
    if (!action) {
      return NextResponse.json({ error: "action is required" }, { status: 400 });
    }

    if (action === "validate") {
      const taskId = await enqueueResearchAgentTask({
        sessionId: params.sessionId,
        taskType: "research.validate_outcome",
        assignedRole: "validation",
        payload: {
          session_id: params.sessionId,
          requested_by: "web"
        }
      });
      return NextResponse.json({ taskId });
    }

    if (action === "summarize") {
      const taskId = await enqueueResearchAgentTask({
        sessionId: params.sessionId,
        taskType: "research.session_summarize",
        assignedRole: "research",
        payload: {
          session_id: params.sessionId,
          requested_by: "web"
        }
      });
      return NextResponse.json({ taskId });
    }

    return NextResponse.json({ error: `unsupported action: ${action}` }, { status: 400 });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
