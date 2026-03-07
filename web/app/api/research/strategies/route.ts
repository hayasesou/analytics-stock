export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { applyResearchStrategyLifecycleAction, fetchResearchStrategies } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const status = req.nextUrl.searchParams.get("status") as
      | "draft"
      | "candidate"
      | "approved"
      | "paper"
      | "live"
      | "paused"
      | "retired"
      | null;
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchResearchStrategies({ status, limit });
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const action = (typeof body?.action === "string" ? body.action : "") as
      | "promote_paper"
      | "approve_live"
      | "reject_live";
    const strategyId = typeof body?.strategyId === "string" ? body.strategyId : "";
    const actedBy = typeof body?.actedBy === "string" ? body.actedBy : "web-ui";
    const reason = typeof body?.reason === "string" ? body.reason : null;
    const recheckCondition = typeof body?.recheckCondition === "string" ? body.recheckCondition : null;
    const recheckAfter = typeof body?.recheckAfter === "string" ? body.recheckAfter : null;

    if (!action || !strategyId) {
      return NextResponse.json(
        { error: "action and strategyId are required" },
        { status: 400 }
      );
    }

    const result = await applyResearchStrategyLifecycleAction({
      strategyId,
      action,
      actedBy,
      reason,
      recheckCondition,
      recheckAfter
    });
    return NextResponse.json({ result });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
