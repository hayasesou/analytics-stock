export const dynamic = "force-dynamic";

import { NextRequest, NextResponse } from "next/server";

import { resolveSecurityQuery } from "@/lib/repository";
import type { SecurityIdentity } from "@/lib/types";

function parseLimit(raw: string | null): number {
  if (!raw) {
    return 8;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return 8;
  }
  return Math.min(parsed, 20);
}

function isExactMatch(candidate: SecurityIdentity, query: string): boolean {
  const q = query.toUpperCase();
  return (
    candidate.securityId.toUpperCase() === q
    || candidate.ticker.toUpperCase() === q
    || candidate.name.toUpperCase() === q
  );
}

export async function GET(req: NextRequest) {
  try {
    const query = String(req.nextUrl.searchParams.get("q") ?? "").trim();
    if (!query) {
      return NextResponse.json({ error: "q is required" }, { status: 400 });
    }

    const limit = parseLimit(req.nextUrl.searchParams.get("limit"));
    const candidates = await resolveSecurityQuery(query, limit);
    const exact = candidates.find((candidate) => isExactMatch(candidate, query)) ?? null;
    const resolved = exact ?? (candidates.length === 1 ? candidates[0] : null);

    return NextResponse.json({
      query,
      normalizedSecurityId: resolved?.securityId ?? null,
      resolved,
      candidates
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 }
    );
  }
}
