export const dynamic = "force-dynamic";

import Link from "next/link";

import { fetchEdgeStates, fetchEdgeTrend } from "@/lib/repository";

type MarketScope = "JP_EQ" | "US_EQ" | "CRYPTO" | "MIXED";

function normalizeMarketScope(raw?: string): MarketScope | null {
  if (!raw) {
    return null;
  }
  const value = raw.trim().toUpperCase();
  if (value === "JP_EQ" || value === "US_EQ" || value === "CRYPTO" || value === "MIXED") {
    return value;
  }
  return null;
}

function normalizeLimit(raw?: string, fallback = 120, min = 1, max = 500): number {
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.trunc(value)));
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

function fmtBps(value: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}bps`;
}

function fmtNum(value: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(2);
}

export default async function EdgePage({
  searchParams
}: {
  searchParams: {
    marketScope?: string;
    strategyName?: string;
    symbol?: string;
    limit?: string;
    trendLimit?: string;
  };
}) {
  const marketScope = normalizeMarketScope(searchParams.marketScope);
  const strategyName = searchParams.strategyName?.trim() || null;
  const symbol = searchParams.symbol?.trim() || null;
  const limit = normalizeLimit(searchParams.limit, 120, 1, 500);
  const trendLimit = normalizeLimit(searchParams.trendLimit, 80, 1, 500);

  const rows = await fetchEdgeStates({
    marketScope,
    strategyName,
    symbol,
    limit
  });
  const selectedStrategyName = strategyName || rows[0]?.strategyName || null;
  const trend = selectedStrategyName
    ? await fetchEdgeTrend({
        strategyName: selectedStrategyName,
        marketScope,
        symbol,
        limit: trendLimit
      })
    : [];

  const avgEdgeScore = rows.length > 0
    ? rows.reduce((acc, row) => acc + row.edgeScore, 0) / rows.length
    : 0;
  const positiveNetEdgeCount = rows.filter((row) => (row.expectedNetEdgeBps ?? -9999) > 0).length;
  const haltedOrCooldownCount = rows.filter((row) => row.riskState === "halted" || row.riskState === "cooldown").length;

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Edge Radar 監視</h1>
        <p className="mono">edge_states と strategy_risk_snapshots を統合表示</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href="/execution">
            執行監視へ
          </Link>
          <span>|</span>
          <Link className="action-link" href="/research">
            研究管理へ
          </Link>
        </div>
        <form className="grid three" style={{ alignItems: "end", marginTop: 10 }}>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="marketScope">Market Scope</label>
            <select id="marketScope" name="marketScope" defaultValue={marketScope ?? ""}>
              <option value="">ALL</option>
              <option value="JP_EQ">JP_EQ</option>
              <option value="US_EQ">US_EQ</option>
              <option value="CRYPTO">CRYPTO</option>
              <option value="MIXED">MIXED</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="strategyName">Strategy Name</label>
            <input
              id="strategyName"
              name="strategyName"
              placeholder="edge-radar-equities / arb-crypto-main"
              defaultValue={strategyName ?? ""}
            />
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="symbol">Symbol</label>
            <input id="symbol" name="symbol" placeholder="JP:7203 / CRYPTO:BTC..." defaultValue={symbol ?? ""} />
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="limit">Rows Limit</label>
            <input id="limit" name="limit" type="number" min={1} max={500} defaultValue={String(limit)} />
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="trendLimit">Trend Limit</label>
            <input id="trendLimit" name="trendLimit" type="number" min={1} max={500} defaultValue={String(trendLimit)} />
          </div>
          <div>
            <button type="submit">適用</button>
          </div>
        </form>
      </div>

      <div className="card">
        <h2>Overview</h2>
        <div className="grid three">
          <div className="metric">
            <span className="k">Rows</span>
            <span className="v">{rows.length}</span>
          </div>
          <div className="metric">
            <span className="k">Avg Edge Score</span>
            <span className="v">{avgEdgeScore.toFixed(1)}</span>
          </div>
          <div className="metric">
            <span className="k">Net Edge &gt; 0</span>
            <span className="v">{positiveNetEdgeCount}</span>
          </div>
          <div className="metric">
            <span className="k">Risk Halt/Cooldown</span>
            <span className="v">{haltedOrCooldownCount}</span>
          </div>
          <div className="metric">
            <span className="k">Selected Strategy</span>
            <span className="v mono" style={{ fontSize: 14 }}>
              {selectedStrategyName ?? "-"}
            </span>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Strategy Trend</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Observed</th>
                <th>Strategy</th>
                <th>Symbol</th>
                <th>Edge Score</th>
                <th>Net Edge</th>
                <th>Dist</th>
                <th>Conf</th>
                <th>Risk State</th>
              </tr>
            </thead>
            <tbody>
              {trend.length === 0 ? (
                <tr>
                  <td colSpan={8}>trend データがありません。</td>
                </tr>
              ) : (
                trend.map((row, idx) => (
                  <tr key={`${row.strategyName}-${row.symbol}-${row.observedAt}-${idx}`}>
                    <td>{formatJst(row.observedAt)} JST</td>
                    <td className="mono">{row.strategyName}</td>
                    <td>{row.symbol}</td>
                    <td>{row.edgeScore.toFixed(2)}</td>
                    <td>{fmtBps(row.expectedNetEdgeBps)}</td>
                    <td>{fmtBps(row.distanceToEntryBps)}</td>
                    <td>{fmtNum(row.confidence)}</td>
                    <td>{row.riskState ?? "-"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>Latest Edge Rows</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Observed</th>
                <th>Strategy</th>
                <th>Status</th>
                <th>Scope</th>
                <th>Symbol</th>
                <th>Edge Score</th>
                <th>Net Edge</th>
                <th>Dist</th>
                <th>Risk</th>
                <th>Explain</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={10}>edge データがありません。</td>
                </tr>
              ) : (
                rows.map((row, idx) => (
                  <tr key={`${row.strategyName}-${row.symbol}-${row.observedAt}-${idx}`}>
                    <td>{formatJst(row.observedAt)} JST</td>
                    <td className="mono">{row.strategyName}</td>
                    <td>{row.strategyStatus ?? "-"}</td>
                    <td>{row.marketScope}</td>
                    <td>{row.symbol}</td>
                    <td>{row.edgeScore.toFixed(2)}</td>
                    <td>{fmtBps(row.expectedNetEdgeBps)}</td>
                    <td>{fmtBps(row.distanceToEntryBps)}</td>
                    <td>{row.riskState ?? "-"}</td>
                    <td title={row.explain ?? undefined}>{row.explain ?? "-"}</td>
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
