export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";

import { fetchWeeklySummary } from "@/lib/repository";

export async function GET() {
  try {
    const report = await fetchWeeklySummary();
    return NextResponse.json({ report });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
