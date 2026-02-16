export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchExecutionRiskSnapshots } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const portfolioName = req.nextUrl.searchParams.get("portfolioName");
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchExecutionRiskSnapshots({
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
