export const dynamic = "force-dynamic";

import Link from "next/link";

import { ExecutionDashboard } from "@/components/ExecutionDashboard";
import { fetchExecutionOrderIntents, fetchExecutionRiskSnapshots } from "@/lib/repository";

type IntentStatus =
  | "proposed"
  | "approved"
  | "rejected"
  | "sent"
  | "executing"
  | "done"
  | "failed"
  | "canceled";

const ALL_STATUSES: IntentStatus[] = [
  "proposed",
  "approved",
  "rejected",
  "sent",
  "executing",
  "done",
  "failed",
  "canceled"
];

function normalizeStatus(raw?: string): IntentStatus | null {
  if (!raw) {
    return null;
  }
  const value = raw as IntentStatus;
  return ALL_STATUSES.includes(value) ? value : null;
}

function normalizeLimit(raw?: string): number {
  if (!raw) {
    return 50;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return 50;
  }
  return Math.min(200, Math.max(1, Math.trunc(value)));
}

export default async function ExecutionPage({
  searchParams
}: {
  searchParams: {
    status?: string;
    portfolioName?: string;
    limit?: string;
  };
}) {
  const status = normalizeStatus(searchParams.status);
  const portfolioName = searchParams.portfolioName?.trim() || null;
  const limit = normalizeLimit(searchParams.limit);
  const [intents, risks] = await Promise.all([
    fetchExecutionOrderIntents({ status, portfolioName, limit }),
    fetchExecutionRiskSnapshots({ portfolioName, limit })
  ]);

  const intentCounts = ALL_STATUSES.map((s) => ({
    status: s,
    count: intents.filter((i) => i.status === s).length
  }));
  const riskStateCounts = {
    normal: risks.filter((r) => r.state === "normal").length,
    risk_alert: risks.filter((r) => r.state === "risk_alert").length,
    halted: risks.filter((r) => r.state === "halted").length
  };

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>執行監視（Intent / Risk）</h1>
        <p className="mono">order_intents, orders, fills, positions, risk_snapshots を監視</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/edge" as any}>
            Edge監視へ
          </Link>
          <span>|</span>
          <Link className="action-link" href="/research">
            研究管理へ
          </Link>
        </div>
        <form className="grid three" style={{ alignItems: "end", marginTop: 10 }}>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="status">Intent Status</label>
            <select id="status" name="status" defaultValue={status ?? ""}>
              <option value="">ALL</option>
              {ALL_STATUSES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="portfolioName">Portfolio</label>
            <input
              id="portfolioName"
              name="portfolioName"
              placeholder="core など"
              defaultValue={portfolioName ?? ""}
            />
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="limit">Limit</label>
            <input id="limit" name="limit" type="number" min={1} max={200} defaultValue={String(limit)} />
          </div>
          <div>
            <button type="submit">適用</button>
          </div>
        </form>
      </div>

      <div className="card">
        <h2>Intent件数</h2>
        <div className="grid three">
          {intentCounts.map((item) => (
            <div className="metric" key={item.status}>
              <span className="k">{item.status}</span>
              <span className="v">{item.count}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="card">
        <h2>Risk状態件数</h2>
        <div className="grid three">
          <div className="metric">
            <span className="k">normal</span>
            <span className="v">{riskStateCounts.normal}</span>
          </div>
          <div className="metric">
            <span className="k">risk_alert</span>
            <span className="v">{riskStateCounts.risk_alert}</span>
          </div>
          <div className="metric">
            <span className="k">halted</span>
            <span className="v">{riskStateCounts.halted}</span>
          </div>
        </div>
      </div>
      <ExecutionDashboard intents={intents} risks={risks} />
    </div>
  );
}
