export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchEdgeTrend } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const strategyName = req.nextUrl.searchParams.get("strategyName") || "";
    const marketScope = req.nextUrl.searchParams.get("marketScope") as "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED" | null;
    const symbol = req.nextUrl.searchParams.get("symbol");
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    if (!strategyName.trim()) {
      return NextResponse.json({ error: "strategyName is required" }, { status: 400 });
    }

    const rows = await fetchEdgeTrend({
      strategyName,
      marketScope,
      symbol,
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
