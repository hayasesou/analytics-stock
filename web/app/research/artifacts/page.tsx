export const dynamic = "force-dynamic";

import Link from "next/link";

import { ResearchArtifactChart } from "@/components/ResearchArtifactChart";
import { fetchResearchArtifacts } from "@/lib/repository";

function formatJson(value: Record<string, unknown> | null | undefined): string {
  if (!value || Object.keys(value).length === 0) {
    return "";
  }
  return JSON.stringify(value, null, 2);
}

function compactText(value: string | null | undefined): string | null {
  const text = String(value ?? "").trim();
  return text.length > 0 ? text : null;
}

function readChartSpec(metadata: Record<string, unknown>): {
  title?: string;
  kind?: "line" | "bar" | "scatter" | "area";
  xAxisLabel?: string;
  yAxisLabel?: string;
  series: { name: string; data: [string, number][] }[];
} | null {
  const raw = metadata.chart_spec;
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const chart = raw as Record<string, unknown>;
  const series = Array.isArray(chart.series) ? chart.series : [];
  const normalizedSeries = series
    .map((item) => {
      if (!item || typeof item !== "object") {
        return null;
      }
      const record = item as Record<string, unknown>;
      const rawPoints = Array.isArray(record.data) ? record.data : [];
      const points = rawPoints
        .map((point) => {
          if (!Array.isArray(point) || point.length < 2) {
            return null;
          }
          const x = String(point[0] ?? "");
          const y = Number(point[1]);
          if (!x || Number.isNaN(y)) {
            return null;
          }
          return [x, y] as [string, number];
        })
        .filter((point): point is [string, number] => point !== null);
      if (points.length === 0) {
        return null;
      }
      return {
        name: String(record.name ?? "series"),
        data: points,
      };
    })
    .filter((item): item is { name: string; data: [string, number][] } => item !== null);
  if (normalizedSeries.length === 0) {
    return null;
  }
  return {
    title: typeof chart.title === "string" ? chart.title : undefined,
    kind:
      chart.kind === "line" || chart.kind === "bar" || chart.kind === "scatter" || chart.kind === "area"
        ? chart.kind
        : undefined,
    xAxisLabel: typeof chart.xAxisLabel === "string" ? chart.xAxisLabel : undefined,
    yAxisLabel: typeof chart.yAxisLabel === "string" ? chart.yAxisLabel : undefined,
    series: normalizedSeries,
  };
}

export default async function ResearchArtifactsPage(props: {
  searchParams?: { sessionId?: string };
}) {
  const sessionId = props.searchParams?.sessionId?.trim() || null;
  const rows = await fetchResearchArtifacts({ limit: 100, sessionId });
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Artifacts</h1>
        <p>Notebook 風の artifact 一覧です。</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/research/chat" as any}>
            Research Chatへ
          </Link>
          {sessionId ? (
            <Link className="action-link" href={`/research/chat?sessionId=${encodeURIComponent(sessionId)}` as any}>
              このsessionをChatで開く
            </Link>
          ) : null}
        </div>
        {sessionId ? <p className="mono">filter session={sessionId}</p> : null}
      </div>
      <div className="grid" style={{ gap: 12 }}>
        {rows.map((row) => (
          <div className="card" key={row.id}>
            <div className="mono">
              {row.artifactType} / {row.title} / session={row.sessionId}
            </div>
            {row.artifactType === "chart" ? (() => {
              const chartSpec = readChartSpec(row.metadata);
              if (!chartSpec) {
                return <div className="mono">chart spec unavailable</div>;
              }
              return <ResearchArtifactChart spec={chartSpec} />;
            })() : null}
            {row.bodyMd ? <pre style={{ whiteSpace: "pre-wrap" }}>{row.bodyMd}</pre> : null}
            {row.codeText ? <pre>{row.codeText}</pre> : null}
            {row.latestRun ? (
              <div className="grid" style={{ gap: 8 }}>
                <strong>Latest Run</strong>
                <div className="mono">
                  status={row.latestRun.runStatus} / executed_at={row.latestRun.createdAt}
                </div>
                {compactText(row.latestRun.stdoutText) ? (
                  <div>
                    <strong>stdout</strong>
                    <pre>{compactText(row.latestRun.stdoutText)}</pre>
                  </div>
                ) : null}
                {compactText(row.latestRun.stderrText) ? (
                  <div>
                    <strong>stderr</strong>
                    <pre>{compactText(row.latestRun.stderrText)}</pre>
                  </div>
                ) : null}
                {formatJson(row.latestRun.resultJson) ? (
                  <div>
                    <strong>result_json</strong>
                    <pre>{formatJson(row.latestRun.resultJson)}</pre>
                  </div>
                ) : null}
                {row.latestRun.outputR2Key ? (
                  <div className="mono">output_r2_key={row.latestRun.outputR2Key}</div>
                ) : null}
              </div>
            ) : null}
          </div>
        ))}
        {rows.length === 0 ? <div className="card">Artifacts はまだありません。</div> : null}
      </div>
    </div>
  );
}
