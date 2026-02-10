export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";

import { fetchDailyEvents } from "@/lib/repository";

export async function GET() {
  try {
    const rows = await fetchDailyEvents();
    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
