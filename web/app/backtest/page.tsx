export const dynamic = "force-dynamic";

import { BacktestEquityDrawdownChart } from "@/components/BacktestEquityDrawdownChart";
import { MetricCards } from "@/components/MetricCards";
import { fetchBacktestData, fetchBacktestRunOptions } from "@/lib/repository";
import { BacktestRunOption } from "@/lib/types";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isUuid(value: string): boolean {
  return UUID_RE.test(value);
}

function pct(v: number) {
  return `${(v * 100).toFixed(2)}%`;
}

function clampRunLimit(v: number): number {
  if (!Number.isFinite(v)) {
    return 20;
  }
  return Math.min(200, Math.max(20, Math.trunc(v)));
}

function formatJst(ts: string | null): string {
  if (!ts) {
    return "未取得";
  }
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
    return "未取得";
  }
  return `${id.slice(0, 8)}...${id.slice(-4)}`;
}

function statusLabel(status: string | null): string {
  if (!status) {
    return "未取得";
  }
  if (status === "success") {
    return "成功";
  }
  if (status === "failed") {
    return "失敗";
  }
  if (status === "running") {
    return "実行中";
  }
  if (status === "queued") {
    return "待機";
  }
  if (status === "canceled") {
    return "キャンセル";
  }
  return status;
}

function countLabel(value: number | null): string {
  if (value == null) {
    return "未取得";
  }
  return String(value);
}

function backtestAvailabilityLabel(run: BacktestRunOption | null): string {
  if (!run) {
    return "未取得";
  }
  if (run.hasBacktestRun) {
    return "あり";
  }
  return "なし";
}

function runRowHref(args: {
  runId: string | null;
  fallback: "none" | "latest_with_backtest";
  runLimit: number;
}): string {
  const params = new URLSearchParams();
  if (args.runId) {
    params.set("run_id", args.runId);
  }
  params.set("fallback", args.fallback);
  params.set("run_limit", String(args.runLimit));
  return `/backtest?${params.toString()}`;
}

function summaryMessage(input: {
  hasInvalidRunId: boolean;
  hasLatestWeekly: boolean;
  autoSwitchedToBacktestRun: boolean;
  selectedRunByUser: boolean;
  reasonCode: string;
}): string {
  if (!input.hasLatestWeekly) {
    return "まだ実行データがありません。研究管理で週次実行を行った後、もう一度このページを開いてください。";
  }
  if (input.hasInvalidRunId) {
    return "URLの実行ID指定が不正だったため無視し、最新データを基準に表示しています。";
  }
  if (input.autoSwitchedToBacktestRun) {
    return "最新の週次実行に検証結果がないため、結果がある直近の実行を自動で表示しています。";
  }
  if (input.selectedRunByUser && input.reasonCode === "requested_run_has_no_backtest") {
    return "選択した実行には検証結果がないため、結果を表示できません。";
  }
  if (input.reasonCode === "no_signals") {
    return "シグナルが0件のためノートレードです（資産はフラット、ベンチマークのみ変動）。";
  }
  if (input.reasonCode === "no_curve") {
    return "検証の指標はありますが、推移グラフ用データ（資産推移/ドローダウン）が保存されていません。";
  }
  if (input.reasonCode === "no_metrics") {
    return "検証は実行されていますが、指標データが保存されていません。";
  }
  return "検証結果を表示しています。";
}

export default async function BacktestPage({
  searchParams
}: {
  searchParams: {
    run_id?: string;
    fallback?: string;
    run_limit?: string;
  };
}) {
  const runLimit = clampRunLimit(Number(searchParams.run_limit ?? 20));
  const rawRunId = searchParams.run_id?.trim() || null;
  const hasInvalidRunId = Boolean(rawRunId && !isUuid(rawRunId));
  const normalizedRunId = rawRunId && isUuid(rawRunId) ? rawRunId : null;

  const explicitFallback = searchParams.fallback === "none" || searchParams.fallback === "latest_with_backtest"
    ? searchParams.fallback
    : null;
  const fallbackMode = explicitFallback ?? (normalizedRunId ? "none" : "latest_with_backtest");

  const [data, runOptions] = await Promise.all([
    fetchBacktestData({ runId: normalizedRunId, fallbackMode }),
    fetchBacktestRunOptions({ limit: runLimit })
  ]);

  const latestWeekly = runOptions.find((r) => r.runId === data.meta.latestWeeklyRunId) ?? null;
  const resolvedRun = runOptions.find((r) => r.runId === data.meta.resolvedRunId) ?? null;
  const selectedRunByUser = Boolean(normalizedRunId);

  const autoSwitchedToBacktestRun = Boolean(
    !selectedRunByUser
      && data.meta.resolvedSource === "latest_with_backtest"
      && data.meta.latestWeeklyRunId
      && data.meta.resolvedRunId
      && data.meta.latestWeeklyRunId !== data.meta.resolvedRunId
  );

  const canShowMoreRuns = runLimit < 200 && runOptions.length >= runLimit;
  const isNoSignals = data.meta.reasonCode === "no_signals";

  const primaryAction = (() => {
    if (!data.meta.latestWeeklyRunId && !data.meta.latestWithBacktestRunId) {
      return null;
    }

    if (autoSwitchedToBacktestRun && data.meta.latestWeeklyRunId) {
      return {
        label: "最新の週次実行を表示",
        href: runRowHref({
          runId: data.meta.latestWeeklyRunId,
          fallback: "none",
          runLimit
        })
      };
    }

    if (selectedRunByUser) {
      if (data.meta.latestWithBacktestRunId && normalizedRunId !== data.meta.latestWithBacktestRunId) {
        return {
          label: "結果がある最新実行を表示",
          href: runRowHref({
            runId: data.meta.latestWithBacktestRunId,
            fallback: "none",
            runLimit
          })
        };
      }
      if (data.meta.latestWeeklyRunId && normalizedRunId !== data.meta.latestWeeklyRunId) {
        return {
          label: "最新の週次実行へ戻す",
          href: runRowHref({
            runId: null,
            fallback: "latest_with_backtest",
            runLimit
          })
        };
      }
    }

    return null;
  })();

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>バックテスト結果（3コスト）</h1>
        <p className="mono">前提: シグナル翌営業日の寄りで約定 / ATRベースの損切・利確ルール</p>
        <p style={{ marginTop: 8 }}>
          {summaryMessage({
            hasInvalidRunId,
            hasLatestWeekly: Boolean(data.meta.latestWeeklyRunId),
            autoSwitchedToBacktestRun,
            selectedRunByUser,
            reasonCode: data.meta.reasonCode
          })}
        </p>
        {primaryAction ? (
          <div style={{ marginTop: 10 }}>
            <a href={primaryAction.href} className="action-link">
              {primaryAction.label}
            </a>
          </div>
        ) : null}

        <details style={{ marginTop: 10 }}>
          <summary className="mono">技術向け詳細を表示</summary>
          <div className="mini-stack mono" style={{ marginTop: 8 }}>
            <div>requestedRunId: {data.meta.requestedRunId ?? "(なし)"}</div>
            <div>resolvedRunId: {data.meta.resolvedRunId ?? "(なし)"}</div>
            <div>latestWeeklyRunId: {data.meta.latestWeeklyRunId ?? "(なし)"}</div>
            <div>latestWithBacktestRunId: {data.meta.latestWithBacktestRunId ?? "(なし)"}</div>
            <div>resolvedSource: {data.meta.resolvedSource}</div>
            <div>reasonCode: {data.meta.reasonCode}</div>
          </div>
        </details>
      </div>

      <div className="grid two">
        <div className="card">
          <h2>最新の週次実行</h2>
          {latestWeekly ? (
            <div className="mini-stack">
              <div>実行ID: <span className="mono">{shortRunId(latestWeekly.runId)}</span></div>
              <div>実行日時: {formatJst(latestWeekly.finishedAt ?? latestWeekly.startedAt)} JST</div>
              <div>状態: {statusLabel(latestWeekly.status)}</div>
              <div>シグナル件数: {countLabel(latestWeekly.signals)}</div>
              <div>検証結果: {backtestAvailabilityLabel(latestWeekly)}</div>
            </div>
          ) : (
            <p>最新の週次実行はまだありません。</p>
          )}
        </div>

        <div className="card">
          <h2>現在表示している実行</h2>
          {data.meta.resolvedRunId ? (
            <div className="mini-stack">
              <div>実行ID: <span className="mono">{shortRunId(data.meta.resolvedRunId)}</span></div>
              <div>実行日時: {formatJst(data.meta.resolvedRunFinishedAt ?? data.meta.resolvedRunStartedAt)} JST</div>
              <div>状態: {statusLabel(data.meta.resolvedRunStatus)}</div>
              <div>シグナル件数: {countLabel(data.meta.resolvedRunSignals)}</div>
              <div>検証プロファイル数: {countLabel(data.meta.resolvedRunBacktestProfiles)}</div>
            </div>
          ) : (
            <p>現在表示している実行はありません。</p>
          )}
        </div>
      </div>

      <div className="card">
        <h2>表示する実行を選ぶ（直近{runLimit}件）</h2>
        {runOptions.length === 0 ? (
          <p>選択できる実行がありません。</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>表示</th>
                  <th>実行日時</th>
                  <th>状態</th>
                  <th>シグナル</th>
                  <th>バックテスト</th>
                  <th>実行ID</th>
                </tr>
              </thead>
              <tbody>
                {runOptions.map((run) => {
                  const isCurrent = run.runId === data.meta.resolvedRunId;
                  const href = runRowHref({ runId: run.runId, fallback: "none", runLimit });
                  return (
                    <tr key={run.runId} style={isCurrent ? { background: "rgba(15, 127, 109, 0.08)" } : undefined}>
                      <td>
                        {isCurrent ? (
                          <span className="mono">表示中</span>
                        ) : (
                          <a href={href} className="action-link">表示</a>
                        )}
                      </td>
                      <td>{formatJst(run.finishedAt ?? run.startedAt)} JST</td>
                      <td>{statusLabel(run.status)}</td>
                      <td>{countLabel(run.signals)}</td>
                      <td>{backtestAvailabilityLabel(run)}</td>
                      <td className="mono">{shortRunId(run.runId)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {canShowMoreRuns ? (
          <div style={{ marginTop: 10 }}>
            <a
              href={runRowHref({
                runId: normalizedRunId,
                fallback: fallbackMode,
                runLimit: runLimit + 20
              })}
              className="action-link"
            >
              候補をさらに20件表示
            </a>
          </div>
        ) : null}
      </div>

      {isNoSignals ? (
        <div className="card">
          <h2>主要指標の比較（3コスト）</h2>
          <p>この回はシグナル0件のため、成績比較は除外しています。</p>
        </div>
      ) : (
        <MetricCards metrics={data.metrics} />
      )}

      <div className="card">
        <h2>資産推移とドローダウン（Equity / Drawdown）</h2>
        <BacktestEquityDrawdownChart curve={data.curve} />
      </div>

      {!isNoSignals ? (
        <div className="card">
          <h2>主要指標の比較（3コスト）</h2>
          {data.metrics.length === 0 ? <p>検証指標がありません。</p> : null}
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>コスト</th>
                  <th>年率成長率（CAGR）</th>
                  <th>最大下落（MaxDD）</th>
                  <th>Sharpe</th>
                  <th>勝率（WinRate）</th>
                </tr>
              </thead>
              <tbody>
                {data.metrics.map((m) => (
                  <tr key={m.costProfile}>
                    <td>{m.costProfile}</td>
                    <td>{pct(m.cagr)}</td>
                    <td>{pct(m.maxDd)}</td>
                    <td>{m.sharpe.toFixed(2)}</td>
                    <td>{pct(m.winRate)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}

      <div className="card">
        <h2>資産推移データ（最新30件）</h2>
        {data.curve.length === 0 ? <p>時系列データがありません。</p> : null}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>日付</th>
                <th>コスト</th>
                <th>資産（Equity）</th>
                <th>下落率（Drawdown）</th>
                <th>比較指数（Benchmark）</th>
              </tr>
            </thead>
            <tbody>
              {data.curve.slice(-30).map((p, idx) => (
                <tr key={`${p.costProfile}-${p.tradeDate}-${idx}`}>
                  <td>{p.tradeDate}</td>
                  <td>{p.costProfile}</td>
                  <td>{p.equity.toFixed(4)}</td>
                  <td>{pct(p.drawdown)}</td>
                  <td>{p.benchmarkEquity == null ? "-" : p.benchmarkEquity.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
