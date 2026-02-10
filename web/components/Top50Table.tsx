import Link from "next/link";

import { ConfidencePill } from "@/components/ConfidencePill";
import { Top50Row } from "@/lib/types";

export function Top50Table({ rows }: { rows: Top50Row[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Rank</th>
            <th>銘柄</th>
            <th>市場</th>
            <th>スコア</th>
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
                <Link href={`/reports/${encodeURIComponent(r.securityId)}`}>
                  {r.ticker} / {r.name}
                </Link>
              </td>
              <td>{r.market}</td>
              <td>{r.score.toFixed(2)}</td>
              <td>
                <ConfidencePill confidence={r.confidence} />
              </td>
              <td>{r.isSignal ? (r.entryAllowed ? "ON (entry)" : "ON") : "OFF"}</td>
              <td>{r.validUntil ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
