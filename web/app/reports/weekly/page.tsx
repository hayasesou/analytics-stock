export const dynamic = "force-dynamic";

import { fetchWeeklySummary } from "@/lib/repository";

export default async function WeeklySummaryPage() {
  const report = await fetchWeeklySummary();

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>週間サマリ</h1>
        {!report ? (
          <p>週間サマリがまだ生成されていません。</p>
        ) : (
          <div className="grid" style={{ gap: 10 }}>
            <div className="mono">{report.createdAt}</div>
            <h2>{report.title}</h2>
            <pre>{report.bodyMd}</pre>
            <div>
              <strong>結論:</strong> {report.conclusion ?? "-"}
            </div>
            <div>
              <strong>反証条件:</strong> {report.falsificationConditions ?? "-"}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
