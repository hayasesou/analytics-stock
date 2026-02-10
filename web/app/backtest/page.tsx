export const dynamic = "force-dynamic";

import { MetricCards } from "@/components/MetricCards";
import { fetchBacktestData } from "@/lib/repository";

function pct(v: number) {
  return `${(v * 100).toFixed(2)}%`;
}

export default async function BacktestPage() {
  const data = await fetchBacktestData();

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>バックテスト（3コスト）</h1>
        <p className="mono">約定: シグナル翌営業日寄り / ATR損切・利確ルール</p>
      </div>

      <MetricCards metrics={data.metrics} />

      <div className="card">
        <h2>主要比較</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>cost</th>
                <th>CAGR</th>
                <th>MaxDD</th>
                <th>Sharpe</th>
                <th>WinRate</th>
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

      <div className="card">
        <h2>Equity Curve（末尾30点）</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Cost</th>
                <th>Equity</th>
                <th>Benchmark</th>
              </tr>
            </thead>
            <tbody>
              {data.curve.slice(-30).map((p, idx) => (
                <tr key={`${p.costProfile}-${p.tradeDate}-${idx}`}>
                  <td>{p.tradeDate}</td>
                  <td>{p.costProfile}</td>
                  <td>{p.equity.toFixed(4)}</td>
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
