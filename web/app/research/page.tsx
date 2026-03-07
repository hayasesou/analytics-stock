export const dynamic = "force-dynamic";

import Link from "next/link";

import {
  fetchResearchAgentTasks,
  fetchResearchFundamentalSnapshots,
  fetchResearchKanban,
  fetchResearchStrategies
} from "@/lib/repository";
import { ResearchFoldValidationPanel } from "@/components/ResearchFoldValidationPanel";
import { StrategyLifecycleActions } from "@/components/StrategyLifecycleActions";
import { TermHelp } from "@/components/TermHelp";

const TERM_HELP = {
  evalType: [
    { label: "定義", text: "戦略評価の方式です。quick は単発、robust はwalk-forward fold検証付きです。" },
    { label: "計算元", text: "strategy_evaluations.eval_type。" },
    { label: "解釈", text: "robust_backtest の方が過学習耐性を見る用途に向きます。" },
    { label: "注意点", text: "過去履歴では quick と robust が混在します。" }
  ],
  validation: [
    { label: "定義", text: "fold検証ゲートの pass/fail です。" },
    { label: "計算元", text: "metrics.validation_passed または artifacts.validation.gate.passed。" },
    { label: "解釈", text: "fail は閾値未達または fold不足を示します。" },
    { label: "注意点", text: "quick_backtest では '-' になります。" }
  ],
  folds: [
    { label: "定義", text: "fold検証で集計対象になった fold 数です。" },
    { label: "計算元", text: "metrics.validation_fold_count / summary.fold_count / sharpe系列長。" },
    { label: "解釈", text: "値が大きいほど期間分割での確認回数が多いです。" },
    { label: "注意点", text: "skip fold があると表の行数と一致しない場合があります。" }
  ],
  foldTrend: [
    { label: "定義", text: "primary profile の Sharpe の先頭→末尾（差分）です。" },
    { label: "計算元", text: "validation.folds[].profiles[primary].sharpe。" },
    { label: "解釈", text: "右肩下がりなら最近foldで劣化している可能性があります。" },
    { label: "注意点", text: "skip fold は計算から除外されます。" }
  ],
  foldRange: [
    { label: "定義", text: "primary profile の Sharpe 最小..最大です。" },
    { label: "計算元", text: "foldSharpeMin..foldSharpeMax。" },
    { label: "解釈", text: "幅が広いほど fold間の安定性が低い可能性があります。" },
    { label: "注意点", text: "値が片側に偏る場合はレジーム依存を疑います。" }
  ],
  sharpe: [
    { label: "定義", text: "単位リスクあたりのリターン指標です。" },
    { label: "計算元", text: "strategy_evaluations.metrics.sharpe。" },
    { label: "解釈", text: "高いほど効率的なリターン傾向です。" },
    { label: "注意点", text: "quick系の値で、fold分解結果とは別です。" }
  ],
  maxDd: [
    { label: "定義", text: "最大ドローダウン（ピーク比の最大下落）です。" },
    { label: "計算元", text: "strategy_evaluations.metrics.max_dd。" },
    { label: "解釈", text: "絶対値が大きいほど下落耐性が弱いです。" },
    { label: "注意点", text: "表示は%ですが内部値は小数です。" }
  ],
  cagr: [
    { label: "定義", text: "年率換算リターンです。" },
    { label: "計算元", text: "strategy_evaluations.metrics.cagr。" },
    { label: "解釈", text: "長期成長力の比較に使います。" },
    { label: "注意点", text: "期間依存が強いので単独評価は危険です。" }
  ],
  evalRun: [
    { label: "定義", text: "その評価を生成した research run ID です。" },
    { label: "計算元", text: "strategy_evaluations.artifacts.run_id。" },
    { label: "解釈", text: "同じrun由来の評価群を追跡できます。" },
    { label: "注意点", text: "古いデータには入っていない場合があります。" }
  ]
} as const;

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

function fmtSigned(v: number | null): string {
  if (v == null || Number.isNaN(v)) {
    return "-";
  }
  const abs = Math.abs(v).toFixed(4);
  if (v > 0) {
    return `+${abs}`;
  }
  if (v < 0) {
    return `-${abs}`;
  }
  return "0.0000";
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

function shortRunId(id: string | null): string {
  if (!id) {
    return "-";
  }
  return `${id.slice(0, 8)}...${id.slice(-4)}`;
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

function fmtBool(v: boolean | null): string {
  if (v == null) {
    return "-";
  }
  return v ? "OK" : "NG";
}

const KANBAN_LABELS = {
  new: "new",
  analyzing: "analyzing",
  rejected: "rejected",
  candidate: "candidate",
  paper: "paper",
  live: "live"
} as const;

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

  const [strategies, fundamentals, agentTasks, kanbanLanes] = await Promise.all([
    fetchResearchStrategies({ status: strategyStatus || null, limit }),
    fetchResearchFundamentalSnapshots({ rating: rating || null, limit }),
    fetchResearchAgentTasks({ status: taskStatus || null, limit }),
    fetchResearchKanban({ limitPerLane: 3 })
  ]);
  const kanbanTotal = kanbanLanes.reduce((acc, lane) => acc + lane.count, 0);

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>研究管理（LV4 Strategy Factory）</h1>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/edge" as any}>
            Edge監視へ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/chat" as any}>
            Research Chatへ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/sessions" as any}>
            Sessionsへ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/inputs" as any}>
            Inputsへ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/hypotheses" as any}>
            Hypothesesへ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/artifacts" as any}>
            Artifactsへ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/validation" as any}>
            Validationへ
          </Link>
          <span>|</span>
          <Link className="action-link" href="/execution">
            執行監視へ
          </Link>
        </div>
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
        <h2>Research Kanban</h2>
        <div className="grid three">
          <div className="metric">
            <span className="k">Total</span>
            <span className="v">{kanbanTotal}</span>
          </div>
          {kanbanLanes.map((lane) => (
            <div className="metric" key={`kanban-metric-${lane.lane}`}>
              <span className="k">{KANBAN_LABELS[lane.lane]}</span>
              <span className="v">{lane.count}</span>
            </div>
          ))}
        </div>
        <div className="table-wrap" style={{ marginTop: 10 }}>
          <table>
            <thead>
              <tr>
                <th>Lane</th>
                <th>Count</th>
                <th>Samples</th>
              </tr>
            </thead>
            <tbody>
              {kanbanLanes.map((lane) => (
                <tr key={`kanban-row-${lane.lane}`}>
                  <td>{KANBAN_LABELS[lane.lane]}</td>
                  <td>{lane.count}</td>
                  <td>
                    {lane.items.length === 0
                      ? "-"
                      : lane.items
                          .map((item) =>
                            item.subtitle ? `${item.title} (${item.subtitle})` : item.title
                          )
                          .join(" / ")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
                <th>LiveCandidate</th>
                <th>Version</th>
                <th>
                  <span className="term-head">
                    EvalType
                    <TermHelp term="EvalType" sections={TERM_HELP.evalType} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    Validation
                    <TermHelp term="Validation" sections={TERM_HELP.validation} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    Folds
                    <TermHelp term="Folds" sections={TERM_HELP.folds} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    Fold Trend
                    <TermHelp term="Fold Trend" sections={TERM_HELP.foldTrend} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    Fold Range
                    <TermHelp term="Fold Range" sections={TERM_HELP.foldRange} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    Sharpe
                    <TermHelp term="Sharpe" sections={TERM_HELP.sharpe} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    MaxDD
                    <TermHelp term="MaxDD" sections={TERM_HELP.maxDd} />
                  </span>
                </th>
                <th>
                  <span className="term-head">
                    CAGR
                    <TermHelp term="CAGR" sections={TERM_HELP.cagr} />
                  </span>
                </th>
                <th>Updated</th>
                <th>
                  <span className="term-head">
                    Eval Run
                    <TermHelp term="Eval Run" sections={TERM_HELP.evalRun} />
                  </span>
                </th>
                <th>Backtest</th>
                <th>Paper Progress</th>
                <th>Lifecycle</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {strategies.length === 0 ? (
                <tr>
                  <td colSpan={19}>戦略データがありません。</td>
                </tr>
              ) : (
                strategies.map((s) => (
                  <tr key={s.strategyId}>
                    <td className="mono">{s.strategyName}</td>
                    <td>{s.assetScope}</td>
                    <td>{s.status}</td>
                    <td>{s.liveCandidate ? "yes" : "no"}</td>
                    <td>{s.version ?? "-"}</td>
                    <td>{s.evalType ?? "-"}</td>
                    <td
                      title={
                        s.validationFailReasons.length > 0
                          ? `fail: ${s.validationFailReasons.join(", ")}`
                          : undefined
                      }
                    >
                      {s.validationPassed == null ? "-" : s.validationPassed ? "pass" : "fail"}
                    </td>
                    <td>{s.validationFoldCount ?? "-"}</td>
                    <td>
                      {s.foldSharpeFirst == null || s.foldSharpeLast == null
                        ? "-"
                        : `${s.foldSharpeFirst.toFixed(4)}→${s.foldSharpeLast.toFixed(4)} (${fmtSigned(s.foldSharpeDelta)})`}
                    </td>
                    <td>
                      {s.foldSharpeMin == null || s.foldSharpeMax == null
                        ? "-"
                        : `${s.foldSharpeMin.toFixed(4)}..${s.foldSharpeMax.toFixed(4)}`}
                    </td>
                    <td>{fmtNum(s.sharpe)}</td>
                    <td>{fmtPct(s.maxDd)}</td>
                    <td>{fmtPct(s.cagr)}</td>
                    <td>{formatJst(s.updatedAt)} JST</td>
                    <td className="mono">{shortRunId(s.evalRunId)}</td>
                    <td>
                      {s.evalRunId ? (
                        <Link className="action-link" href={`/backtest?run_id=${encodeURIComponent(s.evalRunId)}`}>
                          このrunを開く
                        </Link>
                      ) : (
                        "-"
                      )}
                    </td>
                    <td>
                      {s.paperDays == null && s.paperRoundTrips == null
                        ? "-"
                        : `${s.paperDays ?? 0}d / ${s.paperRoundTrips ?? 0}rt (${fmtBool(
                            s.paperGateDaysOk
                          )}/${fmtBool(s.paperGateRoundTripsOk)}/${fmtBool(s.paperGateRiskOk)})`}
                    </td>
                    <td
                      title={
                        s.lastLifecycleReason
                          ? `${s.lastLifecycleAction ?? "-"}: ${s.lastLifecycleReason}`
                          : undefined
                      }
                    >
                      {s.lastLifecycleAction
                        ? `${s.lastLifecycleAction} by ${s.lastLifecycleBy ?? "-"}`
                        : "-"}
                      {s.lastLifecycleAt ? ` @ ${formatJst(s.lastLifecycleAt)} JST` : ""}
                      {s.lastLifecycleRecheckAfter ? ` / recheck ${s.lastLifecycleRecheckAfter}` : ""}
                    </td>
                    <td>
                      <StrategyLifecycleActions
                        strategyId={s.strategyId}
                        status={s.status}
                        liveCandidate={s.liveCandidate}
                      />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>Walk-forward Fold Validation</h2>
        <ResearchFoldValidationPanel strategies={strategies} />
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
