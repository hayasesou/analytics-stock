export const dynamic = "force-dynamic";

import {
  fetchResearchAgentTasks,
  fetchResearchFundamentalSnapshots,
  fetchResearchStrategies
} from "@/lib/repository";

function fmtNum(v: number | null): string {
  if (v == null || Number.isNaN(v)) {
    return "-";
  }
  return v.toFixed(4);
}

function fmtPct(v: number | null): string {
  if (v == null || Number.isNaN(v)) {
    return "-";
  }
  return `${(v * 100).toFixed(2)}%`;
}

function formatJst(ts: string): string {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return ts;
  }
  return d.toLocaleString("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

function ratingClass(rating: string): "high" | "medium" | "low" {
  if (rating === "A") {
    return "high";
  }
  if (rating === "B") {
    return "medium";
  }
  return "low";
}

export default async function ResearchPage({
  searchParams
}: {
  searchParams: {
    strategyStatus?: string;
    rating?: string;
    taskStatus?: string;
    limit?: string;
  };
}) {
  const limitRaw = Number(searchParams.limit ?? 50);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.trunc(limitRaw))) : 50;
  const strategyStatus = (searchParams.strategyStatus || "") as
    | "draft"
    | "candidate"
    | "approved"
    | "paper"
    | "live"
    | "paused"
    | "retired"
    | "";
  const rating = (searchParams.rating || "") as "A" | "B" | "C" | "";
  const taskStatus = (searchParams.taskStatus || "") as
    | "queued"
    | "running"
    | "success"
    | "failed"
    | "canceled"
    | "";

  const [strategies, fundamentals, agentTasks] = await Promise.all([
    fetchResearchStrategies({ status: strategyStatus || null, limit }),
    fetchResearchFundamentalSnapshots({ rating: rating || null, limit }),
    fetchResearchAgentTasks({ status: taskStatus || null, limit })
  ]);

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>研究管理（LV4 Strategy Factory）</h1>
        <form className="grid three" style={{ alignItems: "end", marginTop: 10 }}>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="strategyStatus">Strategy Status</label>
            <select id="strategyStatus" name="strategyStatus" defaultValue={strategyStatus}>
              <option value="">ALL</option>
              <option value="draft">draft</option>
              <option value="candidate">candidate</option>
              <option value="approved">approved</option>
              <option value="paper">paper</option>
              <option value="live">live</option>
              <option value="paused">paused</option>
              <option value="retired">retired</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="rating">Fundamental Rating</label>
            <select id="rating" name="rating" defaultValue={rating}>
              <option value="">ALL</option>
              <option value="A">A</option>
              <option value="B">B</option>
              <option value="C">C</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="taskStatus">Agent Task Status</label>
            <select id="taskStatus" name="taskStatus" defaultValue={taskStatus}>
              <option value="">ALL</option>
              <option value="queued">queued</option>
              <option value="running">running</option>
              <option value="success">success</option>
              <option value="failed">failed</option>
              <option value="canceled">canceled</option>
            </select>
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
        <h2>戦略一覧</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Scope</th>
                <th>Status</th>
                <th>Version</th>
                <th>EvalType</th>
                <th>Sharpe</th>
                <th>MaxDD</th>
                <th>CAGR</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {strategies.length === 0 ? (
                <tr>
                  <td colSpan={9}>戦略データがありません。</td>
                </tr>
              ) : (
                strategies.map((s) => (
                  <tr key={s.strategyId}>
                    <td className="mono">{s.strategyName}</td>
                    <td>{s.assetScope}</td>
                    <td>{s.status}</td>
                    <td>{s.version ?? "-"}</td>
                    <td>{s.evalType ?? "-"}</td>
                    <td>{fmtNum(s.sharpe)}</td>
                    <td>{fmtPct(s.maxDd)}</td>
                    <td>{fmtPct(s.cagr)}</td>
                    <td>{formatJst(s.updatedAt)} JST</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>ファンダメンタル判断（A/B/C）</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rating</th>
                <th>Security</th>
                <th>Market</th>
                <th>Source</th>
                <th>AsOf</th>
                <th>Confidence</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {fundamentals.length === 0 ? (
                <tr>
                  <td colSpan={7}>ファンダ判断データがありません。</td>
                </tr>
              ) : (
                fundamentals.map((f, idx) => (
                  <tr key={`${f.securityId}-${f.source}-${idx}`}>
                    <td>
                      <span className={`pill ${ratingClass(f.rating)}`}>{f.rating}</span>
                    </td>
                    <td>
                      {f.ticker} / {f.name}
                    </td>
                    <td>{f.market}</td>
                    <td>{f.source}</td>
                    <td>{f.asOfDate}</td>
                    <td>{f.confidence ?? "-"}</td>
                    <td>{f.summary}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>AI Agent Tasks</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>TaskType</th>
                <th>Priority</th>
                <th>Strategy</th>
                <th>Security</th>
                <th>CostUSD</th>
                <th>Created</th>
                <th>Finished</th>
              </tr>
            </thead>
            <tbody>
              {agentTasks.length === 0 ? (
                <tr>
                  <td colSpan={8}>タスクデータがありません。</td>
                </tr>
              ) : (
                agentTasks.map((task) => (
                  <tr key={task.id}>
                    <td>{task.status}</td>
                    <td>{task.taskType}</td>
                    <td>{task.priority}</td>
                    <td className="mono">{task.strategyName ?? "-"}</td>
                    <td>{task.securityId ?? "-"}</td>
                    <td>{task.costUsd == null ? "-" : task.costUsd.toFixed(4)}</td>
                    <td>{formatJst(task.createdAt)} JST</td>
                    <td>{task.finishedAt ? `${formatJst(task.finishedAt)} JST` : "-"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
