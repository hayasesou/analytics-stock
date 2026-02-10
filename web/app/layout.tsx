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
            <nav className="nav">
              <Link href="/top50">Top50</Link>
              <Link href="/reports/weekly">週間サマリ</Link>
              <Link href="/backtest">バックテスト</Link>
              <Link href="/events">日次イベント</Link>
              <Link href="/chat">質問チャット</Link>
            </nav>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
