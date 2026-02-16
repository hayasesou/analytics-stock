export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchExecutionOrderIntents } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const statusParam = req.nextUrl.searchParams.get("status");
    const portfolioName = req.nextUrl.searchParams.get("portfolioName");
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchExecutionOrderIntents({
      status: (statusParam as
        | "proposed"
        | "approved"
        | "rejected"
        | "sent"
        | "executing"
        | "done"
        | "failed"
        | "canceled"
        | null) ?? null,
      portfolioName,
      limit
    });
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
