export const dynamic = "force-dynamic";

import Link from "next/link";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { fetchWeeklyActionData, fetchWeeklySummary } from "@/lib/repository";

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

function pct(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

function pctNullable(v: number | null): string {
  if (v == null) {
    return "-";
  }
  return pct(v);
}

export default async function WeeklySummaryPage() {
  const [report, actionData] = await Promise.all([fetchWeeklySummary(), fetchWeeklyActionData()]);

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>週間サマリ</h1>
        {!report ? (
          <p>週間サマリがまだ生成されていません。</p>
        ) : (
          <div className="grid" style={{ gap: 12 }}>
            <div className="mono">{formatJst(report.createdAt)} JST</div>
            <h2>{report.title}</h2>
            <div className="markdown">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{report.bodyMd}</ReactMarkdown>
            </div>
            <div>
              <strong>結論:</strong> {report.conclusion ?? "-"}
            </div>
            <div>
              <strong>反証条件:</strong> {report.falsificationConditions ?? "-"}
            </div>

            <div className="grid" style={{ gap: 10 }}>
              <h3>今週のアクション</h3>
              <div className="action-grid">
                <article className="action-card">
                  <h4>1. High x Top10 の一次情報更新</h4>
                  <p>
                    該当件数: <strong>{actionData.highConfidenceTop10.length}</strong> / 10
                  </p>
                  {actionData.highConfidenceTop10.length === 0 ? (
                    <p className="action-note">High confidence 条件を満たす Top10 はありません。</p>
                  ) : (
                    <ul className="action-list">
                      {actionData.highConfidenceTop10.slice(0, 5).map((r) => (
                        <li key={r.securityId}>
                          <Link href={`/reports/${encodeURIComponent(r.securityId)}`}>
                            #{r.rank} {r.ticker} / {r.name}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  )}
                  <Link className="action-link" href="/top50?confidence=High&signalOnly=1">
                    Top50で確認
                  </Link>
                </article>

                <article className="action-card">
                  <h4>2. 流動性フラグの変化</h4>
                  {actionData.previousRunId ? (
                    <p>
                      変化銘柄数: <strong>{actionData.liquidityChanges.length}</strong>
                    </p>
                  ) : (
                    <p className="action-note">前週比較データがまだありません。</p>
                  )}
                  <Link className="action-link" href="#liquidity-changes">
                    変化一覧を見る
                  </Link>
                </article>

                <article className="action-card">
                  <h4>3. strict コストのドローダウン</h4>
                  {actionData.strictMetric ? (
                    <div className="grid" style={{ gap: 4 }}>
                      <p>
                        MaxDD: <strong>{pct(actionData.strictMetric.maxDd)}</strong>
                      </p>
                      <p>
                        CAGR: <strong>{pct(actionData.strictMetric.cagr)}</strong>
                      </p>
                      <p>
                        Sharpe: <strong>{actionData.strictMetric.sharpe.toFixed(2)}</strong>
                      </p>
                    </div>
                  ) : (
                    <p className="action-note">strict 指標が未生成です。</p>
                  )}
                  <Link className="action-link" href="/backtest">
                    Backtestを見る
                  </Link>
                </article>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="card" id="liquidity-changes">
        <h2>流動性フラグ変化（前週比）</h2>
        {!actionData.latestRunId ? (
          <p>週次runがまだありません。</p>
        ) : !actionData.previousRunId ? (
          <p>前週runがまだないため比較できません。</p>
        ) : actionData.liquidityChanges.length === 0 ? (
          <p>流動性フラグの変化はありません。</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>銘柄</th>
                  <th>市場</th>
                  <th>前週</th>
                  <th>今週</th>
                </tr>
              </thead>
              <tbody>
                {actionData.liquidityChanges.map((r) => (
                  <tr key={r.securityId}>
                    <td>
                      <Link href={`/reports/${encodeURIComponent(r.securityId)}`}>
                        {r.ticker} / {r.name}
                      </Link>
                    </td>
                    <td>{r.market}</td>
                    <td>{r.previousLiquidityFlag ? "ON" : "OFF"}</td>
                    <td>{r.currentLiquidityFlag ? "ON" : "OFF"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card">
        <h2>Signal Diagnostics（5日/20日/60日）</h2>
        {actionData.signalDiagnostics.length === 0 ? (
          <p>診断値がまだありません。</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Horizon</th>
                  <th>Sample</th>
                  <th>Hit Rate</th>
                  <th>Median</th>
                  <th>P10</th>
                  <th>P90</th>
                </tr>
              </thead>
              <tbody>
                {actionData.signalDiagnostics.map((d) => (
                  <tr key={d.horizonDays}>
                    <td>{d.horizonDays}d</td>
                    <td>{d.sampleSize}</td>
                    <td>{pct(d.hitRate)}</td>
                    <td>{pctNullable(d.medianReturn)}</td>
                    <td>{pctNullable(d.p10Return)}</td>
                    <td>{pctNullable(d.p90Return)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
