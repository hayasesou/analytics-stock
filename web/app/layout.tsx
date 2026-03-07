import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Analytics Stock",
  description: "Weekly stock analysis and evidence-backed research"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ja">
      <body>
        <div className="app-shell">
          <header className="topbar">
            <div className="brand">Analytics Stock v1.1</div>
            <div className="nav-stack">
              <nav className="nav">
                <Link href="/top50">Top50</Link>
                <Link href="/reports/weekly">週間サマリ</Link>
                <Link href="/backtest">バックテスト</Link>
                <Link href={"/edge" as any}>Edge監視</Link>
                <Link href="/execution">執行監視</Link>
                <Link href="/research">研究管理</Link>
                <Link href="/events">日次イベント</Link>
                <Link href="/chat">質問チャット</Link>
              </nav>
              <nav className="nav nav-sub">
                <span className="nav-label">Research</span>
                <Link href={"/research/sessions" as any}>Sessions</Link>
                <Link href={"/research/chat" as any}>Chat</Link>
                <Link href={"/research/inputs" as any}>Inputs</Link>
                <Link href={"/research/hypotheses" as any}>Hypotheses</Link>
                <Link href={"/research/artifacts" as any}>Artifacts</Link>
                <Link href={"/research/validation" as any}>Validation</Link>
              </nav>
            </div>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
