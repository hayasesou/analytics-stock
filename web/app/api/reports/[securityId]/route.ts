export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchCitations, fetchReportsBySecurity } from "@/lib/repository";

function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

export async function GET(
  _req: NextRequest,
  { params }: { params: { securityId: string } }
) {
  try {
    const securityId = decodeSecurityId(params.securityId);
    const reports = await fetchReportsBySecurity(securityId);
    const expanded = await Promise.all(
      reports.map(async (r) => ({
        ...r,
        citations: await fetchCitations(r.id)
      }))
    );

    return NextResponse.json({ securityId, reports: expanded });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
