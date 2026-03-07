export const dynamic = "force-dynamic";

import Link from "next/link";

import { fetchResearchValidation } from "@/lib/repository";

function fmtPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(2)}%`;
}

export default async function ResearchValidationPage() {
  const rows = await fetchResearchValidation({ limit: 100 });
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Validation</h1>
        <p>仮説ごとの 1 / 5 / 20 日評価です。</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/research/hypotheses" as any}>
            仮説一覧へ
          </Link>
        </div>
      </div>
      <div className="card table-wrap">
        <table>
          <thead>
            <tr>
              <th>Checked</th>
              <th>Label</th>
              <th>Stance</th>
              <th>Horizon</th>
              <th>1d</th>
              <th>5d</th>
              <th>20d</th>
              <th>MFE</th>
              <th>MAE</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                <td>{row.checkedAt}</td>
                <td>{row.outcomeLabel}</td>
                <td>{row.hypothesis?.stance ?? "-"}</td>
                <td>{row.hypothesis?.horizonDays ?? "-"}</td>
                <td>{fmtPct(row.ret1d)}</td>
                <td>{fmtPct(row.ret5d)}</td>
                <td>{fmtPct(row.ret20d)}</td>
                <td>{fmtPct(row.mfe)}</td>
                <td>{fmtPct(row.mae)}</td>
              </tr>
            ))}
            {rows.length === 0 ? (
              <tr>
                <td colSpan={9}>Validation はまだありません。</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}
