export const dynamic = "force-dynamic";

import { fetchCitations, fetchReportsBySecurity } from "@/lib/repository";

export default async function SecurityReportPage({
  params
}: {
  params: { securityId: string };
}) {
  const reports = await fetchReportsBySecurity(params.securityId);

  const expanded = await Promise.all(
    reports.map(async (r) => ({
      ...r,
      citations: await fetchCitations(r.id)
    }))
  );

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>銘柄詳細: {params.securityId}</h1>
        {expanded.length === 0 ? <p>レポートがありません。</p> : null}
      </div>

      {expanded.map((r) => (
        <section key={r.id} className="card grid">
          <div className="mono">{r.createdAt}</div>
          <h2>
            {r.title} <span className="pill low">{r.reportType}</span>
          </h2>
          <pre>{r.bodyMd}</pre>
          <div>
            <strong>結論:</strong> {r.conclusion ?? "-"}
          </div>
          <div>
            <strong>反証条件:</strong> {r.falsificationConditions ?? "-"}
          </div>
          <div>
            <strong>Evidence</strong>
            {r.citations.length === 0 ? (
              <p>引用なし</p>
            ) : (
              <ul>
                {r.citations.map((c, idx) => (
                  <li key={`${c.docVersionId}-${idx}`}>
                    <span className="mono">{c.claimId}</span> doc={c.docVersionId} page={c.pageRef ?? "-"} quote={c.quoteText}
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>
      ))}
    </div>
  );
}
