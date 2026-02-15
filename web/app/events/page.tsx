export const dynamic = "force-dynamic";

import { fetchDailyEvents, fetchLatestDailyEvents } from "@/lib/repository";

function formatJst(ts: string): string {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return ts;
  }
  return d.toLocaleString("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

export default async function EventsPage() {
  const [rows, latestRows] = await Promise.all([fetchDailyEvents(), fetchLatestDailyEvents(10)]);
  const nowMs = Date.now();
  const high = rows.filter((r) => r.importance === "high");
  const medium = rows.filter((r) => r.importance === "medium");
  const low = rows.filter((r) => r.importance === "low");

  const isWithin24Hours = (ts: string): boolean => {
    const t = new Date(ts).getTime();
    if (Number.isNaN(t)) {
      return false;
    }
    return nowMs - t <= 24 * 60 * 60 * 1000;
  };

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>日次イベント（直近24h）</h1>
        <div className="grid three">
          <div className="metric">
            <span className="k">High</span>
            <span className="v">{high.length}</span>
          </div>
          <div className="metric">
            <span className="k">Medium</span>
            <span className="v">{medium.length}</span>
          </div>
          <div className="metric">
            <span className="k">Low</span>
            <span className="v">{low.length}</span>
          </div>
        </div>
        {rows.length === 0 ? (
          <p style={{ marginTop: 10 }}>
            直近24時間に該当イベントはありません。下段に期間外を含む最新10件を表示しています。
          </p>
        ) : null}
      </div>

      <div className="card">
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Importance</th>
                <th>Time</th>
                <th>Type</th>
                <th>Title</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={5}>直近24時間に該当イベントはありません。</td>
                </tr>
              ) : (
                rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <span className={`pill ${r.importance}`}>{r.importance}</span>
                    </td>
                    <td>{formatJst(r.eventTime)} JST</td>
                    <td>{r.eventType}</td>
                    <td>{r.title}</td>
                    <td>{r.summary}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h2>最新10件（期間外含む）</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Window</th>
                <th>Importance</th>
                <th>Time</th>
                <th>Type</th>
                <th>Title</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {latestRows.length === 0 ? (
                <tr>
                  <td colSpan={6}>イベント履歴がありません。</td>
                </tr>
              ) : (
                latestRows.map((r) => (
                  <tr key={`latest-${r.id}`}>
                    <td>{isWithin24Hours(r.eventTime) ? "24h内" : "期間外"}</td>
                    <td>
                      <span className={`pill ${r.importance}`}>{r.importance}</span>
                    </td>
                    <td>{formatJst(r.eventTime)} JST</td>
                    <td>{r.eventType}</td>
                    <td>{r.title}</td>
                    <td>{r.summary}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
