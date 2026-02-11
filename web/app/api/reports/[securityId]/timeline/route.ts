export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { fetchSecurityTimeline } from "@/lib/repository";

const SECURITY_ID_RE = /^(JP:\d{4}|US:\d+)$/;

function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function parseDays(raw: string | null): number | null {
  if (raw == null || raw === "") {
    return 180;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 3650) {
    return null;
  }
  return parsed;
}

export async function GET(
  req: NextRequest,
  { params }: { params: { securityId: string } }
) {
  try {
    const securityId = decodeSecurityId(params.securityId);
    if (!SECURITY_ID_RE.test(securityId)) {
      return NextResponse.json(
        { error: "invalid securityId format", securityId },
        { status: 400 }
      );
    }

    const days = parseDays(req.nextUrl.searchParams.get("days"));
    if (days == null) {
      return NextResponse.json(
        { error: "days must be integer between 1 and 3650" },
        { status: 400 }
      );
    }

    const timeline = await fetchSecurityTimeline(securityId, days);
    if (!timeline) {
      return NextResponse.json(
        { error: "security not found", securityId, days },
        { status: 404 }
      );
    }

    return NextResponse.json(timeline);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
