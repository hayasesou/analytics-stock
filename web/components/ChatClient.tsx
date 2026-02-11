"use client";

import { useState } from "react";

type AnswerPayload = {
  sessionId: string;
  answerBefore: string | null;
  answerAfter: string;
  citations: Array<{ docVersionId: string; pageRef: string | null; quoteText: string; claimId: string | null }>;
};

export function ChatClient() {
  const [question, setQuestion] = useState("");
  const [securityId, setSecurityId] = useState("");
  const [periodDays, setPeriodDays] = useState("90");
  const [sessionId, setSessionId] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [payload, setPayload] = useState<AnswerPayload | null>(null);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    if (!question.trim()) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          sessionId,
          securityId: securityId.trim() || null,
          periodDays: periodDays.trim() || null
        })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "request failed");
      }
      setSessionId(json.sessionId);
      setPayload(json as AnswerPayload);
    } catch (e) {
      setError(e instanceof Error ? e.message : "unknown error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card grid" style={{ gap: 10 }}>
        <label htmlFor="q">質問</label>
        <textarea
          id="q"
          rows={4}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="例: JP:1301 の最新結論と反証条件を教えて"
        />
        <div className="grid two" style={{ gap: 8 }}>
          <div className="grid" style={{ gap: 4 }}>
            <label htmlFor="securityId">銘柄（任意）</label>
            <input
              id="securityId"
              value={securityId}
              onChange={(e) => setSecurityId(e.target.value)}
              placeholder="例: US:119"
            />
          </div>
          <div className="grid" style={{ gap: 4 }}>
            <label htmlFor="periodDays">期間日数（任意）</label>
            <input
              id="periodDays"
              value={periodDays}
              onChange={(e) => setPeriodDays(e.target.value)}
              placeholder="例: 90"
            />
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button disabled={loading} onClick={submit}>
            {loading ? "処理中..." : "送信"}
          </button>
          {sessionId ? <span className="mono">session: {sessionId}</span> : null}
        </div>
        {error ? <div style={{ color: "#a93131" }}>{error}</div> : null}
      </div>

      {payload ? (
        <div className="card grid">
          <h3>回答（構造化）</h3>
          <pre>{payload.answerAfter}</pre>
          <h4>引用</h4>
          {payload.citations.length === 0 ? (
            <p>引用なし（追加調査が必要）</p>
          ) : (
            <ul>
              {payload.citations.map((c, idx) => (
                <li key={`${c.docVersionId}-${idx}`}>
                  <span className="mono">doc={c.docVersionId}</span> page={c.pageRef ?? "-"} quote={c.quoteText}
                </li>
              ))}
            </ul>
          )}
        </div>
      ) : null}
    </div>
  );
}
