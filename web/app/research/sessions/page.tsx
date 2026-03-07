export const dynamic = "force-dynamic";

import Link from "next/link";

import { fetchResearchSessions } from "@/lib/repository";

export default async function ResearchSessionsPage() {
  const rows = await fetchResearchSessions(100);
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Sessions</h1>
        <p>Discord / Web から作成された research session の一覧です。</p>
      </div>

      <div className="grid" style={{ gap: 12 }}>
        {rows.map((row) => (
          <div className="card" key={row.sessionId}>
            <div className="grid" style={{ gap: 8 }}>
              <div className="mono">{row.sessionId}</div>
              <strong>{row.title ?? "Untitled Session"}</strong>
              <div className="hint-line">
                <span>messages={row.messageCount}</span>
                <span>|</span>
                <span>inputs={row.inputCount}</span>
                <span>|</span>
                <span>hypotheses={row.hypothesisCount}</span>
                <span>|</span>
                <span>{row.createdAt}</span>
              </div>
              {row.latestAssistantMessage ? (
                <p>{row.latestAssistantMessage.slice(0, 220)}</p>
              ) : (
                <p>assistant message はまだありません。</p>
              )}
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <Link className="action-link" href={`/api/research/sessions/${row.sessionId}` as any}>
                  API JSON
                </Link>
                <Link className="action-link" href={`/research/chat?sessionId=${row.sessionId}` as any}>
                  Chatで開く
                </Link>
                <Link className="action-link" href={`/research/hypotheses?sessionId=${row.sessionId}` as any}>
                  Hypotheses
                </Link>
                <Link className="action-link" href={`/research/artifacts?sessionId=${row.sessionId}` as any}>
                  Artifacts
                </Link>
              </div>
            </div>
          </div>
        ))}
        {rows.length === 0 ? <div className="card">session はまだありません。</div> : null}
      </div>
    </div>
  );
}
