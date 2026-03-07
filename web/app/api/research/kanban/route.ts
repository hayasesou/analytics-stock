export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchResearchKanban } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const limitRaw = req.nextUrl.searchParams.get("limitPerLane");
    const limitPerLane = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchResearchKanban({ limitPerLane });
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
