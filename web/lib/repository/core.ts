import { getSql } from "@/lib/db";
import type { SecurityIdentity, Top50Row } from "@/lib/types";
import { clampLookupLimit, clampLookbackDays, decodeSecurityId, isLegacyMockSecurity, isUndefinedRelationError, isUuid, mapSecurityIdentityRow } from "./shared";

export async function latestRunId(runType: "weekly" | "daily"): Promise<string | null> {
  const sql = getSql();
  const rows = await sql<{ id: string }[]>`
    select id::text as id
    from runs
    where run_type = ${runType} and status = 'success'
    order by finished_at desc nulls last, started_at desc
    limit 1
  `;
  return rows[0]?.id ?? null;
}

export async function fetchTop50(runId?: string): Promise<Top50Row[]> {
  const sql = getSql();
  const targetRun = runId ?? (await latestRunId("weekly"));
  if (!targetRun) {
    return [];
  }

  const rows = await sql<
    {
      rank: number;
      prev_rank: number | null;
      rank_delta: number | null;
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
      sector: string | null;
      quality: number | null;
      growth: number | null;
      value: number | null;
      momentum: number | null;
      catalyst: number | null;
      combined_score: number;
      score_delta: number | null;
      edge_score: number | null;
      missing_ratio: number | null;
      liquidity_flag: boolean | null;
      selection_reason: string | null;
      confidence: "High" | "Medium" | "Low";
      is_signal: boolean;
      entry_allowed: boolean;
      signal_reason: string | null;
      valid_until: string | null;
    }[]
  >`
    with target_run as (
      select id, coalesce(finished_at, started_at) as ordering_time
      from runs
      where id = ${targetRun}::uuid
      limit 1
    ),
    prev_run as (
      select r.id
      from runs r
      join target_run t on true
      where r.run_type = 'weekly'
        and r.status = 'success'
        and r.id <> t.id
        and coalesce(r.finished_at, r.started_at) < t.ordering_time
      order by coalesce(r.finished_at, r.started_at) desc, r.started_at desc
      limit 1
    )
    select
      t.rank,
      prev_t.rank as prev_rank,
      (prev_t.rank - t.rank) as rank_delta,
      s.security_id,
      s.market,
      s.ticker,
      s.name,
      s.sector,
      sc.quality,
      sc.growth,
      sc.value,
      sc.momentum,
      sc.catalyst,
      sc.combined_score,
      (sc.combined_score - prev_sc.combined_score) as score_delta,
      nullif(sc.flags->>'edge_score', '')::double precision as edge_score,
      sc.missing_ratio,
      sc.liquidity_flag,
      t.reason as selection_reason,
      sc.confidence,
      coalesce(sig.is_signal, false) as is_signal,
      coalesce(sig.entry_allowed, false) as entry_allowed,
      sig.reason as signal_reason,
      sig.valid_until::text
    from top50_membership t
    join securities s on s.id = t.security_id
    left join score_snapshots sc on sc.run_id = t.run_id and sc.security_id = s.id
    left join signals sig on sig.run_id = t.run_id and sig.security_id = s.id
    left join prev_run pr on true
    left join top50_membership prev_t on prev_t.run_id = pr.id and prev_t.security_id = s.id
    left join score_snapshots prev_sc on prev_sc.run_id = pr.id and prev_sc.security_id = s.id
    where t.run_id = ${targetRun}::uuid
    order by t.rank asc
  `;

  return rows.map((r) => ({
    rank: r.rank,
    rankPrev: r.prev_rank,
    rankDelta: r.rank_delta,
    securityId: r.security_id,
    market: r.market,
    ticker: r.ticker,
    name: r.name,
    sector: r.sector,
    score: Number(r.combined_score ?? 0),
    scoreDelta: r.score_delta == null ? null : Number(r.score_delta),
    edgeScore: Number(r.edge_score ?? 0),
    quality: Number(r.quality ?? 0),
    growth: Number(r.growth ?? 0),
    value: Number(r.value ?? 0),
    momentum: Number(r.momentum ?? 0),
    catalyst: Number(r.catalyst ?? 0),
    missingRatio: Number(r.missing_ratio ?? 0),
    liquidityFlag: Boolean(r.liquidity_flag),
    selectionReason: r.selection_reason,
    signalReason: r.signal_reason,
    confidence: r.confidence,
    isSignal: r.is_signal,
    entryAllowed: r.entry_allowed,
    validUntil: r.valid_until
  }));
}

export async function fetchSecurityIdentity(securityId: string): Promise<SecurityIdentity | null> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId).trim();
  if (!normalizedSecurityId) {
    return null;
  }

  const rows = await sql<
    {
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
    }[]
  >`
    select
      security_id,
      market,
      ticker,
      coalesce(name, security_id) as name
    from securities
    where security_id = ${normalizedSecurityId}
    limit 1
  `;
  const row = rows[0];
  if (!row || isLegacyMockSecurity({ market: row.market, name: row.name })) {
    return null;
  }
  return mapSecurityIdentityRow(row);
}

export async function resolveSecurityQuery(query: string, limit = 5): Promise<SecurityIdentity[]> {
  const sql = getSql();
  const normalizedQuery = decodeSecurityId(query).trim();
  if (!normalizedQuery) {
    return [];
  }

  const queryLike = `%${normalizedQuery}%`;
  const cappedLimit = clampLookupLimit(limit);
  const rows = await sql<
    {
      security_id: string;
      market: "JP" | "US";
      ticker: string;
      name: string;
      rank_bucket: number;
    }[]
  >`
    with candidates as (
      select
        s.security_id,
        s.market,
        s.ticker,
        coalesce(s.name, s.security_id) as name,
        case
          when upper(s.security_id) = upper(${normalizedQuery}) then 0
          when upper(s.ticker) = upper(${normalizedQuery}) then 1
          when upper(coalesce(s.name, '')) = upper(${normalizedQuery}) then 2
          when upper(s.security_id) like upper(${queryLike}) then 3
          when upper(s.ticker) like upper(${queryLike}) then 4
          when upper(coalesce(s.name, '')) like upper(${queryLike}) then 5
          else 99
        end as rank_bucket
      from securities s
      where
        upper(s.security_id) = upper(${normalizedQuery})
        or upper(s.ticker) = upper(${normalizedQuery})
        or upper(coalesce(s.name, '')) = upper(${normalizedQuery})
        or upper(s.security_id) like upper(${queryLike})
        or upper(s.ticker) like upper(${queryLike})
        or upper(coalesce(s.name, '')) like upper(${queryLike})
    )
    select
      security_id,
      market,
      ticker,
      name,
      rank_bucket
    from candidates
    where rank_bucket < 99
    order by rank_bucket asc, security_id asc
    limit ${cappedLimit}
  `;

  return rows
    .filter((r) => !isLegacyMockSecurity({ market: r.market, name: r.name }))
    .map((r) => mapSecurityIdentityRow(r));
}

