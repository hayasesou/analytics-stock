export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchResearchFundamentalSnapshots } from "@/lib/repository";

export async function GET(req: NextRequest) {
  try {
    const rating = req.nextUrl.searchParams.get("rating") as "A" | "B" | "C" | null;
    const limitRaw = req.nextUrl.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : undefined;
    const rows = await fetchResearchFundamentalSnapshots({ rating, limit });
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
