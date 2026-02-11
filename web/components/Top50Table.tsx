import Link from "next/link";

import { ConfidencePill } from "@/components/ConfidencePill";
import { Top50Row } from "@/lib/types";

function formatSelectionReason(reason: string | null): string {
  if (reason === "market_minimum") {
    return "市場最低枠で採用";
  }
  if (reason === "score_rank") {
    return "スコア上位で採用";
  }
  return reason ?? "-";
}

function formatSignalReason(row: Top50Row): string {
  if (row.isSignal && row.entryAllowed) {
    return "エントリー候補";
  }
  if (row.isSignal && !row.entryAllowed) {
    return "シグナル点灯だが上限で見送り";
  }
  if (row.signalReason === "confidence_not_high") {
    return "Confidence不足";
  }
  if (row.signalReason === "rank_outside_top10") {
    return "順位条件外";
  }
  if (row.signalReason === "risk_alert_mode_entry_cap") {
    return "リスク警戒モード上限";
  }
  return row.signalReason ?? "シグナル未点灯";
}

function formatSkipReason(row: Top50Row): string {
  if (row.entryAllowed) {
    return "-";
  }
  if (row.liquidityFlag) {
    return "流動性フラグON";
  }
  return formatSignalReason(row);
}

function formatRankDelta(delta: number | null): string {
  if (delta == null) {
    return "-";
  }
  if (delta > 0) {
    return `+${delta}`;
  }
  return `${delta}`;
}

function formatScoreDelta(delta: number | null): string {
  if (delta == null) {
    return "-";
  }
  const text = delta >= 0 ? `+${delta.toFixed(2)}` : delta.toFixed(2);
  return text;
}

export function Top50Table({ rows }: { rows: Top50Row[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Rank</th>
            <th>先週比</th>
            <th>銘柄</th>
            <th>市場</th>
            <th>スコア / Edge</th>
            <th>Style</th>
            <th>採用理由</th>
            <th>見送り理由</th>
            <th>欠損 / 流動性</th>
            <th>Confidence</th>
            <th>Signal</th>
            <th>有効期限</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.securityId}>
              <td>{r.rank}</td>
              <td>
                <div className="mini-stack">
                  <span>前週: {r.rankPrev ?? "-"}</span>
                  <span className={`delta ${r.rankDelta != null && r.rankDelta > 0 ? "up" : r.rankDelta != null && r.rankDelta < 0 ? "down" : ""}`}>
                    ΔRank: {formatRankDelta(r.rankDelta)}
                  </span>
                </div>
              </td>
              <td>
                <Link href={`/reports/${encodeURIComponent(r.securityId)}`}>
                  {r.ticker} / {r.name}
                </Link>
              </td>
              <td>{r.market}</td>
              <td>
                <div className="mini-stack">
                  <span>{r.score.toFixed(2)}</span>
                  <span className="delta">Edge: {r.edgeScore.toFixed(2)}</span>
                  <span className={`delta ${r.scoreDelta != null && r.scoreDelta > 0 ? "up" : r.scoreDelta != null && r.scoreDelta < 0 ? "down" : ""}`}>
                    ΔScore: {formatScoreDelta(r.scoreDelta)}
                  </span>
                </div>
              </td>
              <td className="mono">
                Q {r.quality.toFixed(1)} / G {r.growth.toFixed(1)} / V {r.value.toFixed(1)} / M{" "}
                {r.momentum.toFixed(1)} / C {r.catalyst.toFixed(1)}
              </td>
              <td>{formatSelectionReason(r.selectionReason)}</td>
              <td>{formatSkipReason(r)}</td>
              <td>
                <div className="mini-stack">
                  <span>欠損率: {(r.missingRatio * 100).toFixed(1)}%</span>
                  <span className={`delta ${r.liquidityFlag ? "down" : "up"}`}>流動性: {r.liquidityFlag ? "Flag ON" : "OK"}</span>
                </div>
              </td>
              <td>
                <ConfidencePill confidence={r.confidence} />
              </td>
              <td>{formatSignalReason(r)}</td>
              <td>{r.validUntil ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
