export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchTop50 } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const runId = req.nextUrl.searchParams.get("runId") ?? undefined;
    const market = req.nextUrl.searchParams.get("market");
    const confidence = req.nextUrl.searchParams.get("confidence");
    const signalOnly = req.nextUrl.searchParams.get("signalOnly") === "1";

    let rows = await fetchTop50(runId);

    if (market) {
      rows = rows.filter((r) => r.market === market);
    }
    if (confidence) {
      rows = rows.filter((r) => r.confidence === confidence);
    }
    if (signalOnly) {
      rows = rows.filter((r) => r.isSignal);
    }

    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
