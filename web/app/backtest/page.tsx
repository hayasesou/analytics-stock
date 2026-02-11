export const dynamic = "force-dynamic";

import { BacktestEquityDrawdownChart } from "@/components/BacktestEquityDrawdownChart";
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
        <h2>Equity + Drawdown</h2>
        <BacktestEquityDrawdownChart curve={data.curve} />
      </div>

      <div className="card">
        <h2>主要比較（3コスト）</h2>
        {data.metrics.length === 0 ? <p>バックテスト指標がありません。</p> : null}
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
        <h2>Equity/Drawdown（末尾30点）</h2>
        {data.curve.length === 0 ? <p>時系列データがありません。</p> : null}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Cost</th>
                <th>Equity</th>
                <th>Drawdown</th>
                <th>Benchmark</th>
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
