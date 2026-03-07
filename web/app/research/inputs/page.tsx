export const dynamic = "force-dynamic";

import Link from "next/link";

import { fetchResearchInputs } from "@/lib/repository";

export default async function ResearchInputsPage() {
  const rows = await fetchResearchInputs({ limit: 100 });
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Inputs</h1>
        <p>Discord / Web / URL / Text 由来の入力一覧です。</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/research/chat" as any}>
            Research Chatへ
          </Link>
        </div>
      </div>
      <div className="card table-wrap">
        <table>
          <thead>
            <tr>
              <th>Created</th>
              <th>Type</th>
              <th>Status</th>
              <th>Quality</th>
              <th>Source</th>
              <th>Session</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                <td>{row.createdAt}</td>
                <td>{row.sourceType}</td>
                <td>{row.extractionStatus}</td>
                <td>{row.qualityGrade ?? "-"}</td>
                <td className="mono">{row.sourceUrl ?? row.rawText ?? "-"}</td>
                <td className="mono">{row.sessionId}</td>
              </tr>
            ))}
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6}>データがありません。</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}
