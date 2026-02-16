export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchResearchStrategies } from "@/lib/repository";

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
