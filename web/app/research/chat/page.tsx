import Link from "next/link";

import { ResearchChatClient } from "@/components/ResearchChatClient";

export default function ResearchChatPage({
  searchParams,
}: {
  searchParams?: { sessionId?: string };
}) {
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>Research Chat</h1>
        <p>自然文と複数 URL から研究 session を作成し、仮説、artifact、後続 task を一括で起票します。</p>
        <div className="hint-line" style={{ marginTop: 6 }}>
          <Link className="action-link" href="/research">
            研究管理へ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/sessions" as any}>
            Session一覧へ
          </Link>
          <span>|</span>
          <Link className="action-link" href={"/research/hypotheses" as any}>
            仮説一覧へ
          </Link>
        </div>
      </div>
      <ResearchChatClient initialSessionId={searchParams?.sessionId} />
    </div>
  );
}
