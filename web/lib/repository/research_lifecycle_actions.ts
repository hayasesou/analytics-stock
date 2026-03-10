import { getSql } from "@/lib/db";
import type { ResearchStrategy } from "@/lib/types";

export async function applyResearchStrategyLifecycleAction(input: {
  strategyId: string;
  action: "promote_paper" | "approve_live" | "reject_live";
  actedBy: string;
  reason?: string | null;
  recheckCondition?: string | null;
  recheckAfter?: string | null;
}): Promise<{ strategyId: string; status: ResearchStrategy["status"]; liveCandidate: boolean }> {
  const sql = getSql();
  const strategyId = input.strategyId.trim();
  if (!strategyId) {
    throw new Error("strategyId is required");
  }
  const actedBy = input.actedBy.trim() || "web-ui";
  const reason = input.reason?.trim() || null;
  const recheckCondition = input.recheckCondition?.trim() || null;
  const recheckAfter = input.recheckAfter?.trim() || null;

  const rows = await sql<
    {
      strategy_id: string;
      status: ResearchStrategy["status"];
      live_candidate: boolean;
      strategy_version_id: string | null;
    }[]
  >`
    with latest_versions as (
      select distinct on (sv.strategy_id)
        sv.strategy_id,
        sv.id::text as strategy_version_id
      from strategy_versions sv
      order by sv.strategy_id, sv.version desc
    )
    select
      s.id::text as strategy_id,
      s.status,
      s.live_candidate,
      lv.strategy_version_id
    from strategies s
    left join latest_versions lv on lv.strategy_id = s.id
    where s.id = ${strategyId}::uuid
    limit 1
  `;
  const current = rows[0];
  if (!current) {
    throw new Error("strategy not found");
  }

  let toStatus: ResearchStrategy["status"] = current.status;
  let toLiveCandidate = current.live_candidate;

  if (input.action === "promote_paper") {
    toStatus = "paper";
    toLiveCandidate = false;
  } else if (input.action === "approve_live") {
    if (!current.strategy_version_id) {
      throw new Error("latest strategy version is missing");
    }
    if (!current.live_candidate) {
      throw new Error("strategy is not live_candidate");
    }
    if (!(current.status === "paper" || current.status === "approved")) {
      throw new Error(`cannot approve_live from status=${current.status}`);
    }
    toStatus = "live";
    toLiveCandidate = false;
  } else if (input.action === "reject_live") {
    toStatus = current.status === "approved" ? "paper" : current.status;
    toLiveCandidate = false;
  }

  await sql.begin(async (tx) => {
    await tx`
      update strategies
      set status = ${toStatus},
          live_candidate = ${toLiveCandidate},
          updated_at = now()
      where id = ${strategyId}::uuid
    `;

    if (input.action === "approve_live") {
      await tx`
        update strategy_versions
        set
          is_active = (id = ${current.strategy_version_id}::uuid),
          approved_by = case when id = ${current.strategy_version_id}::uuid then ${actedBy} else approved_by end,
          approved_at = case when id = ${current.strategy_version_id}::uuid then now() else approved_at end
        where strategy_id = ${strategyId}::uuid
      `;
    }

    await tx`
      insert into strategy_lifecycle_reviews (
        strategy_id,
        strategy_version_id,
        action,
        from_status,
        to_status,
        live_candidate,
        reason,
        recheck_condition,
        recheck_after,
        acted_by,
        acted_at,
        metadata
      ) values (
        ${strategyId}::uuid,
        ${current.strategy_version_id}::uuid,
        ${input.action},
        ${current.status},
        ${toStatus},
        ${toLiveCandidate},
        ${reason},
        ${recheckCondition},
        ${recheckAfter}::date,
        ${actedBy},
        now(),
        ${JSON.stringify({
          reason,
          recheckCondition,
          recheckAfter
        })}::jsonb
      )
    `;
  });

  return {
    strategyId,
    status: toStatus,
    liveCandidate: toLiveCandidate
  };
}
