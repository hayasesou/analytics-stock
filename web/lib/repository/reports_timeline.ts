import { getSql } from "@/lib/db";
import type { SecurityTimelineData } from "@/lib/types";
import { clampLookbackDays, decodeSecurityId, isLegacyMockSecurity } from "./shared";

export async function fetchSecurityTimeline(
  securityId: string,
  days = 180
): Promise<SecurityTimelineData | null> {
  const sql = getSql();
  const normalizedSecurityId = decodeSecurityId(securityId);
  const lookbackDays = clampLookbackDays(days);

  const securityRows = await sql<
    {
      id: string;
      market: "JP" | "US";
      name: string;
    }[]
  >`
    select
      id::text as id,
      market,
      coalesce(name, security_id) as name
    from securities
    where security_id = ${normalizedSecurityId}
    limit 1
  `;
  const security = securityRows[0];
  if (!security || isLegacyMockSecurity({ market: security.market, name: security.name })) {
    return null;
  }

  const [priceRows, signalRows, eventRows] = await Promise.all([
    sql<
      {
        trade_date: string;
        close_raw: number;
      }[]
    >`
      select
        trade_date::text as trade_date,
        close_raw
      from prices_daily
      where security_id = ${security.id}::uuid
        and trade_date >= current_date - (${lookbackDays}::int * interval '1 day')
      order by trade_date asc
    `,
    sql<
      {
        as_of_date: string;
        is_signal: boolean;
        entry_allowed: boolean;
        reason: string | null;
        rank: number | null;
        confidence: "High" | "Medium" | "Low" | null;
        valid_until: string | null;
      }[]
    >`
      select
        as_of_date::text as as_of_date,
        is_signal,
        entry_allowed,
        reason,
        rank,
        confidence,
        valid_until::text as valid_until
      from signals
      where security_id = ${security.id}::uuid
        and as_of_date >= current_date - (${lookbackDays}::int * interval '1 day')
      order by as_of_date asc
    `,
    sql<
      {
        event_time: string;
        event_date: string;
        title: string;
        summary: string;
        importance: "high" | "medium" | "low";
        event_type: string;
        source_url: string | null;
      }[]
    >`
      select
        event_time::text as event_time,
        (event_time at time zone 'UTC')::date::text as event_date,
        title,
        summary,
        importance,
        event_type,
        source_url
      from events
      where security_id = ${security.id}::uuid
        and event_time >= now() - (${lookbackDays}::int * interval '1 day')
      order by event_time asc
    `
  ]);

  return {
    securityId: normalizedSecurityId,
    days: lookbackDays,
    prices: priceRows.map((row) => ({
      date: row.trade_date,
      close: Number(row.close_raw ?? 0)
    })),
    signals: signalRows.map((row) => ({
      date: row.as_of_date,
      isSignal: Boolean(row.is_signal),
      entryAllowed: Boolean(row.entry_allowed),
      reason: row.reason,
      rank: row.rank,
      confidence: row.confidence,
      validUntil: row.valid_until
    })),
    events: eventRows.map((row) => ({
      date: row.event_date,
      eventTime: row.event_time,
      title: row.title,
      summary: row.summary,
      importance: row.importance,
      eventType: row.event_type,
      sourceUrl: row.source_url
    }))
  };
}
