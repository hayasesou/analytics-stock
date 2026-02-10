export const dynamic = "force-dynamic";

import { fetchDailyEvents } from "@/lib/repository";

export default async function EventsPage() {
  const rows = await fetchDailyEvents();
  const high = rows.filter((r) => r.importance === "high");
  const medium = rows.filter((r) => r.importance === "medium");
  const low = rows.filter((r) => r.importance === "low");

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
              {rows.map((r) => (
                <tr key={r.id}>
                  <td>
                    <span className={`pill ${r.importance}`}>{r.importance}</span>
                  </td>
                  <td>{r.eventTime}</td>
                  <td>{r.eventType}</td>
                  <td>{r.title}</td>
                  <td>{r.summary}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
