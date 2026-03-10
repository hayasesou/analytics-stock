import { getSql } from "@/lib/db";
import type { ResearchKanbanLane, ResearchKanbanStatus } from "@/lib/types";
import { clampExecutionLimit, isUndefinedRelationError } from "./shared";

export async function fetchResearchKanban(input?: {
  limitPerLane?: number | null;
}): Promise<ResearchKanbanLane[]> {
  const sql = getSql();
  const limitPerLane = clampExecutionLimit(input?.limitPerLane ?? 8);
  const order: ResearchKanbanStatus[] = ["new", "analyzing", "rejected", "candidate", "paper", "live"];

  try {
    const countRows = await sql<
      {
        lane: ResearchKanbanStatus;
        cnt: number;
      }[]
    >`
      with lane_rows as (
        select status as lane
        from ideas
        where status in ('new', 'analyzing', 'rejected')
        union all
        select status as lane
        from strategies
        where status in ('candidate', 'paper', 'live')
      )
      select lane, count(*)::int as cnt
      from lane_rows
      group by lane
    `;

    const itemRows = await sql<
      {
        lane: ResearchKanbanStatus;
        item_type: "idea" | "strategy";
        item_id: string;
        title: string;
        subtitle: string | null;
        updated_at: string;
      }[]
    >`
      with idea_ranked as (
        select
          i.status as lane,
          'idea'::text as item_type,
          i.id::text as item_id,
          i.title as title,
          (i.source_type || coalesce(' @ ' || i.source_url, ''))::text as subtitle,
          i.updated_at::text as updated_at,
          row_number() over (partition by i.status order by i.priority desc, i.updated_at desc) as rn
        from ideas i
        where i.status in ('new', 'analyzing', 'rejected')
      ),
      strategy_ranked as (
        select
          s.status as lane,
          'strategy'::text as item_type,
          s.id::text as item_id,
          s.name as title,
          s.asset_scope::text as subtitle,
          s.updated_at::text as updated_at,
          row_number() over (partition by s.status order by s.updated_at desc, s.created_at desc) as rn
        from strategies s
        where s.status in ('candidate', 'paper', 'live')
      ),
      merged as (
        select lane, item_type, item_id, title, subtitle, updated_at, rn from idea_ranked
        union all
        select lane, item_type, item_id, title, subtitle, updated_at, rn from strategy_ranked
      )
      select lane, item_type, item_id, title, subtitle, updated_at
      from merged
      where rn <= ${limitPerLane}
      order by lane, rn
    `;

    const countByLane: Record<ResearchKanbanStatus, number> = {
      new: 0,
      analyzing: 0,
      rejected: 0,
      candidate: 0,
      paper: 0,
      live: 0
    };
    for (const row of countRows) {
      countByLane[row.lane] = Number(row.cnt ?? 0);
    }

    const itemsByLane: Record<ResearchKanbanStatus, ResearchKanbanLane["items"]> = {
      new: [],
      analyzing: [],
      rejected: [],
      candidate: [],
      paper: [],
      live: []
    };
    for (const row of itemRows) {
      itemsByLane[row.lane].push({
        lane: row.lane,
        itemType: row.item_type,
        id: row.item_id,
        title: row.title,
        subtitle: row.subtitle,
        updatedAt: row.updated_at
      });
    }

    return order.map((lane) => ({
      lane,
      count: countByLane[lane],
      items: itemsByLane[lane]
    }));
  } catch (error) {
    if (
      isUndefinedRelationError(error, "ideas") ||
      isUndefinedRelationError(error, "strategies")
    ) {
      return order.map((lane) => ({ lane, count: 0, items: [] }));
    }
    throw error;
  }
}
