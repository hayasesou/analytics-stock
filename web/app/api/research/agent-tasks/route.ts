export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchResearchAgentTasks } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const status = req.nextUrl.searchParams.get("status") as
      | "queued"
      | "running"
      | "success"
      | "failed"
      | "canceled"
      | null;
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchResearchAgentTasks({ status, limit });
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
