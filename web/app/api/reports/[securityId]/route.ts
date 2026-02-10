export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchCitations, fetchReportsBySecurity } from "@/lib/repository";

export async function GET(
  _req: NextRequest,
  { params }: { params: { securityId: string } }
) {
  try {
    const reports = await fetchReportsBySecurity(params.securityId);
    const expanded = await Promise.all(
      reports.map(async (r) => ({
        ...r,
        citations: await fetchCitations(r.id)
      }))
    );

    return NextResponse.json({ securityId: params.securityId, reports: expanded });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
