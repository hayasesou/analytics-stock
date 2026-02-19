export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";

import { fetchBacktestData } from "@/lib/repository";

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const runId = url.searchParams.get("run_id");
    const fallback = url.searchParams.get("fallback");
    const payload = await fetchBacktestData({
      runId: runId?.trim() || null,
      fallbackMode: fallback === "latest_with_backtest" ? "latest_with_backtest" : "none"
    });
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
