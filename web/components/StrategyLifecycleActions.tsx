"use client";

import { useState } from "react";

type Props = {
  strategyId: string;
  status: "draft" | "candidate" | "approved" | "paper" | "live" | "paused" | "retired";
  liveCandidate: boolean;
};

export function StrategyLifecycleActions({ strategyId, status, liveCandidate }: Props) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [recheckCondition, setRecheckCondition] = useState("");
  const [recheckAfter, setRecheckAfter] = useState("");

  async function submitAction(action: "promote_paper" | "approve_live" | "reject_live") {
    if (busy) {
      return;
    }
    if (action === "reject_live" && !reason.trim()) {
      setError("却下理由を入力してください。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const response = await fetch("/api/research/strategies", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          strategyId,
          action,
          actedBy: "web-ui",
          reason: reason.trim() || null,
          recheckCondition: recheckCondition.trim() || null,
          recheckAfter: recheckAfter || null
        })
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(typeof payload?.error === "string" ? payload.error : "request failed");
      }
      window.location.reload();
    } catch (err) {
      setError(err instanceof Error ? err.message : "action failed");
    } finally {
      setBusy(false);
    }
  }

  if (status === "live" || status === "retired" || status === "paused") {
    return <span>-</span>;
  }

  return (
    <div style={{ display: "grid", gap: 6 }}>
      {status === "candidate" ? (
        <button
          type="button"
          onClick={() => submitAction("promote_paper")}
          disabled={busy}
          title="手動で paper 運用に移行"
        >
          Paperへ
        </button>
      ) : null}
      {(status === "paper" || status === "approved") ? (
        <>
          <button
            type="button"
            onClick={() => submitAction("approve_live")}
            disabled={busy || !liveCandidate}
            title={!liveCandidate ? "live_candidate 条件未達です" : "手動承認で live へ移行"}
          >
            Live承認
          </button>
          <input
            type="text"
            placeholder="却下理由（必須）"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            disabled={busy}
          />
          <input
            type="text"
            placeholder="再審条件（任意）"
            value={recheckCondition}
            onChange={(e) => setRecheckCondition(e.target.value)}
            disabled={busy}
          />
          <input
            type="date"
            value={recheckAfter}
            onChange={(e) => setRecheckAfter(e.target.value)}
            disabled={busy}
          />
          <button
            type="button"
            onClick={() => submitAction("reject_live")}
            disabled={busy}
            title="live 候補を却下し paper 継続"
          >
            却下
          </button>
        </>
      ) : null}
      {error ? <span style={{ color: "#b91c1c" }}>{error}</span> : null}
    </div>
  );
}
