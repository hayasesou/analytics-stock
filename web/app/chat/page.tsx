import { ChatClient } from "@/components/ChatClient";

export default function ChatPage() {
  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>質問チャット（引用 + 差分更新）</h1>
        <p>既存レポートの引用を優先し、不足時は追加調査が必要である旨を明示します。</p>
      </div>
      <ChatClient />
    </div>
  );
}
