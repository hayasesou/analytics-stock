export const dynamic = "force-dynamic";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { SecurityTimelineChart } from "@/components/SecurityTimelineChart";
import { fetchCitations, fetchReportsBySecurity, fetchSecurityTimeline } from "@/lib/repository";
import { ReportRecord } from "@/lib/types";

function decodeSecurityId(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
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

function pickLatestPerType(reports: ReportRecord[]): ReportRecord[] {
  const seen = new Set<string>();
  const picked: ReportRecord[] = [];
  for (const r of reports) {
    if (seen.has(r.reportType)) {
      continue;
    }
    seen.add(r.reportType);
    picked.push(r);
  }
  return picked;
}

function shortDocId(docId: string): string {
  return docId.length > 12 ? `${docId.slice(0, 8)}...${docId.slice(-4)}` : docId;
}

export default async function SecurityReportPage({
  params,
  searchParams
}: {
  params: { securityId: string };
  searchParams: { history?: string; days?: string };
}) {
  const securityId = decodeSecurityId(params.securityId);
  const timelineDaysRaw = Number(searchParams.days ?? 180);
  const timelineDays =
    Number.isInteger(timelineDaysRaw) && timelineDaysRaw > 0 && timelineDaysRaw <= 3650
      ? timelineDaysRaw
      : 180;
  const [reports, timeline] = await Promise.all([
    fetchReportsBySecurity(securityId),
    fetchSecurityTimeline(securityId, timelineDays)
  ]);
  const showHistory = searchParams.history === "1";
  const basePath = `/reports/${encodeURIComponent(securityId)}`;
  const targetReports = showHistory ? reports : pickLatestPerType(reports);

  const expanded = await Promise.all(
    targetReports.map(async (r) => ({
      ...r,
      citations: await fetchCitations(r.id)
    }))
  );

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>銘柄詳細: {securityId}</h1>
        <div className="grid two" style={{ alignItems: "center", gap: 8 }}>
          <p className="mono">
            表示件数: {expanded.length} / 全{reports.length}
          </p>
          <div>
            {showHistory ? (
              <a className="action-link" href={`${basePath}?days=${timelineDays}`}>
                最新のみ表示
              </a>
            ) : (
              <a className="action-link" href={`${basePath}?history=1&days=${timelineDays}`}>
                履歴を表示
              </a>
            )}
          </div>
        </div>
        {expanded.length === 0 ? <p>レポートがありません。</p> : null}
      </div>

      <section className="card grid" style={{ gap: 10 }}>
        <div className="grid two" style={{ alignItems: "center", gap: 8 }}>
          <h2>価格・シグナル・イベント タイムライン</h2>
          <div className="timeline-days">
            {[90, 180, 365].map((days) => {
              const href = `${basePath}?days=${days}${showHistory ? "&history=1" : ""}`;
              return (
                <a
                  key={days}
                  className={`timeline-days-link ${timelineDays === days ? "active" : ""}`}
                  href={href}
                >
                  {days}d
                </a>
              );
            })}
          </div>
        </div>
        {!timeline ? (
          <p>対象銘柄が見つかりません。</p>
        ) : (
          <>
            <p className="mono">
              prices: {timeline.prices.length} / signals: {timeline.signals.length} / events:{" "}
              {timeline.events.length}
            </p>
            <SecurityTimelineChart
              prices={timeline.prices}
              signals={timeline.signals}
              events={timeline.events}
            />
          </>
        )}
      </section>

      {expanded.map((r) => (
        <section key={r.id} className="card grid" style={{ gap: 10 }}>
          <div className="mono">{formatJst(r.createdAt)} JST</div>
          <h2>
            {r.title} <span className="pill low">{r.reportType}</span>
          </h2>
          <div className="markdown">
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                table: ({ children }) => (
                  <div className="table-wrap">
                    <table>{children}</table>
                  </div>
                )
              }}
            >
              {r.bodyMd}
            </ReactMarkdown>
          </div>
          <div>
            <strong>結論:</strong> {r.conclusion ?? "-"}
          </div>
          <div>
            <strong>反証条件:</strong> {r.falsificationConditions ?? "-"}
          </div>
          <details>
            <summary>
              <strong>Evidence</strong> ({r.citations.length})
            </summary>
            {r.citations.length === 0 ? (
              <p>引用なし</p>
            ) : (
              <div className="table-wrap" style={{ marginTop: 8 }}>
                <table>
                  <thead>
                    <tr>
                      <th>Claim</th>
                      <th>Doc</th>
                      <th>Page</th>
                      <th>Quote</th>
                    </tr>
                  </thead>
                  <tbody>
                    {r.citations.map((c, idx) => (
                      <tr key={`${c.docVersionId}-${idx}`}>
                        <td className="mono">{c.claimId}</td>
                        <td className="mono" title={c.docVersionId}>
                          {shortDocId(c.docVersionId)}
                        </td>
                        <td>{c.pageRef ?? "-"}</td>
                        <td style={{ whiteSpace: "normal" }}>{c.quoteText}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </details>
        </section>
      ))}
    </div>
  );
}
