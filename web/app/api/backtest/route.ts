export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";

import { fetchBacktestData } from "@/lib/repository";

export async function GET() {
  try {
    const payload = await fetchBacktestData();
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
