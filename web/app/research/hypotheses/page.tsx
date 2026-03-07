export const dynamic = "force-dynamic";

import Link from "next/link";

import { fetchResearchHypotheses } from "@/lib/repository";

export default async function ResearchHypothesesPage() {
  const rows = await fetchResearchHypotheses({ limit: 100 });
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Hypotheses</h1>
        <p>session から生成された仮説一覧です。</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href={"/research/chat" as any}>
            Research Chatへ
          </Link>
        </div>
      </div>
      <div className="grid" style={{ gap: 12 }}>
        {rows.map((row) => (
          <div className="card" key={row.id}>
            <div className="mono">
              {row.stance} / {row.horizonDays}d / {row.status} / conf={row.confidence ?? "-"}
            </div>
            <p>{row.thesisMd}</p>
            <p>反証: {row.falsificationMd}</p>
            {row.assets.length > 0 ? (
              <div className="grid" style={{ gap: 4 }}>
                <strong>Assets</strong>
                {row.assets.map((asset) => (
                  <span className="mono" key={asset.id}>
                    {asset.assetClass} / {asset.symbolText ?? asset.securityId ?? "-"} / weight={asset.weightHint ?? "-"}
                  </span>
                ))}
              </div>
            ) : null}
          </div>
        ))}
        {rows.length === 0 ? <div className="card">仮説はまだありません。</div> : null}
      </div>
    </div>
  );
}
