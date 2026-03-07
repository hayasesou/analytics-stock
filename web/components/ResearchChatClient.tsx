"use client";

import { useEffect, useState } from "react";

import type { ResearchChatSessionDetail, SecurityIdentity } from "@/lib/types";

type Payload = {
  sessionId: string;
  answerBefore: string | null;
  answerAfter: string;
  resolvedSecurity: SecurityIdentity | null;
  urls: string[];
  session: ResearchChatSessionDetail | null;
};

function formatRole(role: "user" | "assistant" | "system"): string {
  if (role === "assistant") {
    return "Assistant";
  }
  if (role === "system") {
    return "System";
  }
  return "User";
}

export function ResearchChatClient({ initialSessionId }: { initialSessionId?: string }) {
  const [question, setQuestion] = useState("");
  const [securityId, setSecurityId] = useState("");
  const [chartType, setChartType] = useState("auto");
  const [chartInstruction, setChartInstruction] = useState("");
  const [sessionId, setSessionId] = useState<string | undefined>(initialSessionId);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<Payload | null>(null);

  useEffect(() => {
    if (!initialSessionId) {
      return;
    }
    let alive = true;
    void (async () => {
      try {
        const res = await fetch(`/api/research/sessions/${initialSessionId}`, {
          cache: "no-store",
        });
        const json = await res.json();
        if (!res.ok || !alive) {
          return;
        }
        setPayload((prev) => ({
          sessionId: initialSessionId,
          answerBefore: prev?.answerBefore ?? null,
          answerAfter: prev?.answerAfter ?? "",
          resolvedSecurity: prev?.resolvedSecurity ?? null,
          urls: prev?.urls ?? [],
          session: json,
        }));
      } catch {
        // Keep the form usable even if preloading fails.
      }
    })();
    return () => {
      alive = false;
    };
  }, [initialSessionId]);

  const submit = async () => {
    if (!question.trim()) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/research/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          securityId: securityId.trim() || null,
          sessionId
        })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "request failed");
      }
      const next = json as Payload;
      setSessionId(next.sessionId);
      setPayload(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : "unknown error");
    } finally {
      setLoading(false);
    }
  };

  const detail = payload?.session;

  const enqueueSessionAction = async (action: "validate" | "summarize") => {
    if (!sessionId) {
      return;
    }
    setActionLoading(action);
    setError(null);
    try {
      const res = await fetch(`/api/research/sessions/${sessionId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "action failed");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "unknown error");
    } finally {
      setActionLoading(null);
    }
  };

  const enqueueArtifactRun = async (artifactId: string) => {
    if (!sessionId) {
      return;
    }
    setActionLoading(`artifact:${artifactId}`);
    setError(null);
    try {
      const res = await fetch(`/api/research/artifacts/${artifactId}/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sessionId })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "artifact run enqueue failed");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "unknown error");
    } finally {
      setActionLoading(null);
    }
  };

  const enqueueChartGenerate = async (artifactId: string) => {
    if (!sessionId) {
      return;
    }
    setActionLoading(`chart:${artifactId}`);
    setError(null);
    try {
      const res = await fetch(`/api/research/artifacts/${artifactId}/chart`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId,
          chartType,
          instruction: chartInstruction.trim() || null
        })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "chart generate enqueue failed");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "unknown error");
    } finally {
      setActionLoading(null);
    }
  };

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card grid" style={{ gap: 10 }}>
        <h2>Research Chat</h2>
        <label htmlFor="research-question">入力</label>
        <textarea
          id="research-question"
          rows={5}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="URL と自然文をまとめて入力できます。例: https://example.com/news NVDA の供給制約が改善すると見ています。"
        />
        <div className="grid two" style={{ gap: 8 }}>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="research-security-id">銘柄ID（任意）</label>
            <input
              id="research-security-id"
              value={securityId}
              onChange={(e) => setSecurityId(e.target.value)}
              placeholder="例: US:NVDA"
            />
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label>Session</label>
            <div className="mono">{sessionId ?? "-"}</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button disabled={loading} onClick={submit}>
            {loading ? "処理中..." : "送信"}
          </button>
          <button disabled={!sessionId || actionLoading !== null} onClick={() => void enqueueSessionAction("summarize")}>
            {actionLoading === "summarize" ? "enqueue中..." : "Summary Task"}
          </button>
          <button disabled={!sessionId || actionLoading !== null} onClick={() => void enqueueSessionAction("validate")}>
            {actionLoading === "validate" ? "enqueue中..." : "Validation Task"}
          </button>
        </div>
        {error ? <div style={{ color: "#a93131" }}>{error}</div> : null}
      </div>

      {payload ? (
        <div className="grid two" style={{ gap: 12, alignItems: "start" }}>
          <div className="card grid" style={{ gap: 10 }}>
            <h3>Assistant Reply</h3>
            <pre>{payload.answerAfter}</pre>
            {payload.resolvedSecurity ? (
              <div className="mono">
                target: {payload.resolvedSecurity.securityId} / {payload.resolvedSecurity.ticker} / {payload.resolvedSecurity.name}
              </div>
            ) : null}
            {payload.urls.length > 0 ? (
              <div className="grid" style={{ gap: 4 }}>
                <strong>Captured URLs</strong>
                {payload.urls.map((url) => (
                  <span className="mono" key={url}>{url}</span>
                ))}
              </div>
            ) : null}
          </div>

          <div className="grid" style={{ gap: 12 }}>
            <div className="card">
              <h3>Hypotheses</h3>
              {detail?.hypotheses.length ? (
                <div className="grid" style={{ gap: 10 }}>
                  {detail.hypotheses.map((hypothesis) => (
                    <div key={hypothesis.id} style={{ borderTop: "1px solid #ddd", paddingTop: 8 }}>
                      <div className="mono">
                        {hypothesis.stance} / {hypothesis.horizonDays}d / conf={hypothesis.confidence ?? "-"}
                      </div>
                      <p>{hypothesis.thesisMd}</p>
                      <p>反証: {hypothesis.falsificationMd}</p>
                    </div>
                  ))}
                </div>
              ) : (
                <p>仮説はまだありません。</p>
              )}
            </div>

            <div className="card">
              <h3>Artifacts</h3>
              <div className="grid two" style={{ gap: 8, marginBottom: 10 }}>
                <div className="grid" style={{ gap: 6 }}>
                  <label htmlFor="chart-type">Chart Type</label>
                  <select id="chart-type" value={chartType} onChange={(e) => setChartType(e.target.value)}>
                    <option value="auto">auto</option>
                    <option value="price_trend">price_trend</option>
                    <option value="cumulative_return">cumulative_return</option>
                    <option value="relative_return">relative_return</option>
                    <option value="volume">volume</option>
                    <option value="scatter">scatter</option>
                    <option value="bar_compare">bar_compare</option>
                  </select>
                </div>
                <div className="grid" style={{ gap: 6 }}>
                  <label htmlFor="chart-instruction">Chart Instruction</label>
                  <input
                    id="chart-instruction"
                    value={chartInstruction}
                    onChange={(e) => setChartInstruction(e.target.value)}
                    placeholder="例: イベント後累積リターンを優先して"
                  />
                </div>
              </div>
              {detail?.artifacts.length ? (
                <div className="grid" style={{ gap: 10 }}>
                  {detail.artifacts.map((artifact) => (
                    <div key={artifact.id} style={{ borderTop: "1px solid #ddd", paddingTop: 8 }}>
                      <div className="mono">{artifact.artifactType} / {artifact.title}</div>
                      <div style={{ display: "flex", gap: 8, marginBottom: 6 }}>
                        <button
                          type="button"
                          disabled={actionLoading !== null}
                          onClick={() => void enqueueArtifactRun(artifact.id)}
                        >
                          {actionLoading === `artifact:${artifact.id}` ? "enqueue中..." : "Run Task"}
                        </button>
                        {(artifact.artifactType === "sql" || artifact.artifactType === "python") ? (
                          <button
                            type="button"
                            disabled={actionLoading !== null}
                            onClick={() => void enqueueChartGenerate(artifact.id)}
                          >
                            {actionLoading === `chart:${artifact.id}` ? "enqueue中..." : "Chart Task"}
                          </button>
                        ) : null}
                      </div>
                      {artifact.codeText ? <pre>{artifact.codeText}</pre> : null}
                      {artifact.bodyMd ? <p>{artifact.bodyMd}</p> : null}
                    </div>
                  ))}
                </div>
              ) : (
                <p>Artifacts はまだありません。</p>
              )}
            </div>
          </div>
        </div>
      ) : null}

      {detail?.messages.length ? (
        <div className="card">
          <h3>Session Messages</h3>
          <div className="grid" style={{ gap: 8 }}>
            {detail.messages.map((message) => (
              <div key={message.id} style={{ borderTop: "1px solid #ddd", paddingTop: 8 }}>
                <div className="mono">{formatRole(message.role)} / {message.createdAt}</div>
                <pre>{message.content}</pre>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
