export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchEdgeStates, fetchEdgeTrend } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const marketScope = req.nextUrl.searchParams.get("marketScope") as "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED" | null;
    const strategyName = req.nextUrl.searchParams.get("strategyName");
    const symbol = req.nextUrl.searchParams.get("symbol");
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const trendLimitRaw = req.nextUrl.searchParams.get("trendLimit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const trendLimit = trendLimitRaw ? Number(trendLimitRaw) : undefined;

    const rows = await fetchEdgeStates({
      marketScope,
      strategyName,
      symbol,
      limit
    });
    const selectedStrategyName = (strategyName?.trim() || rows[0]?.strategyName || null);
    const trend = selectedStrategyName
      ? await fetchEdgeTrend({
          strategyName: selectedStrategyName,
          marketScope,
          symbol,
          limit: trendLimit
        })
      : [];
    return NextResponse.json({ rows, trend, selectedStrategyName });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
