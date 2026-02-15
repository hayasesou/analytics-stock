"use client";

import { useState } from "react";

const SECURITY_ID_RE = /^(JP:\d{4}|US:(?:\d+|[A-Z][A-Z0-9.-]{0,6}))$/;

type SecurityIdentity = {
  securityId: string;
  market: "JP" | "US";
  ticker: string;
  name: string;
};

type SecurityLookupPayload = {
  query: string;
  normalizedSecurityId: string | null;
  resolved: SecurityIdentity | null;
  candidates: SecurityIdentity[];
};

type AnswerPayload = {
  sessionId: string;
  answerBefore: string | null;
  answerAfter: string;
  resolvedSecurity: SecurityIdentity | null;
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
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookup, setLookup] = useState<SecurityLookupPayload | null>(null);
  const [lookupError, setLookupError] = useState<string | null>(null);

  const resolveSecurity = async (raw: string): Promise<SecurityLookupPayload | null> => {
    const query = raw.trim();
    if (!query) {
      setLookup(null);
      setLookupError(null);
      return null;
    }

    setLookupLoading(true);
    setLookupError(null);
    try {
      const res = await fetch(`/api/securities/resolve?q=${encodeURIComponent(query)}&limit=8`, {
        cache: "no-store"
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "resolve failed");
      }
      setLookup(json as SecurityLookupPayload);
      return json as SecurityLookupPayload;
    } catch (e) {
      const message = e instanceof Error ? e.message : "unknown error";
      setLookupError(message);
      setLookup(null);
      return null;
    } finally {
      setLookupLoading(false);
    }
  };

  const submit = async () => {
    if (!question.trim()) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const rawSecurityInput = securityId.trim();
      let normalizedSecurityId: string | null = rawSecurityInput || null;
      if (rawSecurityInput) {
        let currentLookup = lookup;
        if (!currentLookup || currentLookup.query !== rawSecurityInput) {
          currentLookup = await resolveSecurity(rawSecurityInput);
        }
        if (currentLookup?.normalizedSecurityId) {
          normalizedSecurityId = currentLookup.normalizedSecurityId;
        } else if (!SECURITY_ID_RE.test(rawSecurityInput)) {
          throw new Error("銘柄コードを解決できませんでした。JP:1304 または US:AAPL 形式か、変換候補から選択してください。");
        }
      }

      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          sessionId,
          securityId: normalizedSecurityId,
          periodDays: periodDays.trim() || null
        })
      });
      const json = await res.json();
      if (!res.ok) {
        throw new Error(json.error ?? "request failed");
      }
      setSessionId(json.sessionId);
      const responsePayload = json as AnswerPayload;
      setPayload(responsePayload);
      if (responsePayload.resolvedSecurity) {
        const resolved = responsePayload.resolvedSecurity;
        setLookup({
          query: rawSecurityInput,
          normalizedSecurityId: resolved.securityId,
          resolved,
          candidates: [resolved]
        });
      }
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
          placeholder="例: JP:1301 または US:AAPL の最新結論と反証条件を教えて"
        />
        <div className="grid two" style={{ gap: 8 }}>
          <div className="grid" style={{ gap: 4 }}>
            <label htmlFor="securityId">銘柄（任意）</label>
            <input
              id="securityId"
              value={securityId}
              onChange={(e) => {
                const next = e.target.value;
                setSecurityId(next);
                setLookupError(null);
                const nextTrimmed = next.trim();
                setLookup((prev) => (prev && prev.query === nextTrimmed ? prev : null));
              }}
              onBlur={() => {
                void resolveSecurity(securityId);
              }}
              placeholder="例: US:AAPL"
            />
            <div style={{ display: "flex", gap: 8, alignItems: "center", minHeight: 24 }}>
              <button
                type="button"
                disabled={lookupLoading || !securityId.trim()}
                onClick={() => {
                  void resolveSecurity(securityId);
                }}
              >
                {lookupLoading ? "変換中..." : "変換"}
              </button>
              {lookup?.resolved ? (
                <span className="mono">
                  {lookup.resolved.securityId} - {lookup.resolved.ticker} / {lookup.resolved.name} ({lookup.resolved.market})
                </span>
              ) : null}
            </div>
            {lookup && !lookup.resolved && lookup.candidates.length > 0 ? (
              <div className="grid" style={{ gap: 4 }}>
                <span>候補から選択:</span>
                <div className="grid" style={{ gap: 6 }}>
                  {lookup.candidates.slice(0, 5).map((candidate) => (
                    <button
                      key={candidate.securityId}
                      type="button"
                      onClick={() => {
                        setSecurityId(candidate.securityId);
                        setLookup({
                          query: candidate.securityId,
                          normalizedSecurityId: candidate.securityId,
                          resolved: candidate,
                          candidates: [candidate]
                        });
                      }}
                    >
                      {candidate.securityId} - {candidate.ticker} / {candidate.name} ({candidate.market})
                    </button>
                  ))}
                </div>
              </div>
            ) : null}
            {lookup && !lookup.resolved && lookup.candidates.length === 0 && securityId.trim() ? (
              <span>一致する銘柄が見つかりませんでした。</span>
            ) : null}
            {lookupError ? <span style={{ color: "#a93131" }}>{lookupError}</span> : null}
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
          {payload.resolvedSecurity ? (
            <p className="mono">
              対象銘柄: {payload.resolvedSecurity.securityId} - {payload.resolvedSecurity.ticker} / {payload.resolvedSecurity.name} ({payload.resolvedSecurity.market})
            </p>
          ) : null}
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
