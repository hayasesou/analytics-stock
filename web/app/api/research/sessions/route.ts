export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchResearchSessions } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : 50;
    const rows = await fetchResearchSessions(limit);
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
