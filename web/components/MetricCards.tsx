import { BacktestMetric } from "@/lib/types";

function pct(v: number) {
  return `${(v * 100).toFixed(2)}%`;
}

export function MetricCards({ metrics }: { metrics: BacktestMetric[] }) {
  if (metrics.length === 0) {
    return <div className="card">バックテスト結果がありません。</div>;
  }

  return (
    <div className="grid three">
      {metrics.map((m) => (
        <div key={m.costProfile} className="card">
          <h3>{m.costProfile}</h3>
          <div className="grid two">
            <div className="metric">
              <span className="k">CAGR</span>
              <span className="v">{pct(m.cagr)}</span>
            </div>
            <div className="metric">
              <span className="k">MaxDD</span>
              <span className="v">{pct(m.maxDd)}</span>
            </div>
            <div className="metric">
              <span className="k">Sharpe</span>
              <span className="v">{m.sharpe.toFixed(2)}</span>
            </div>
            <div className="metric">
              <span className="k">Sortino</span>
              <span className="v">{m.sortino.toFixed(2)}</span>
            </div>
            <div className="metric">
              <span className="k">Win Rate</span>
              <span className="v">{pct(m.winRate)}</span>
            </div>
            <div className="metric">
              <span className="k">Volatility</span>
              <span className="v">{pct(m.volatility)}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
