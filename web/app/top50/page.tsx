export const dynamic = "force-dynamic";

import { fetchTop50 } from "@/lib/repository";
import { Top50Table } from "@/components/Top50Table";

export default async function Top50Page({
  searchParams
}: {
  searchParams: { market?: string; confidence?: string; signalOnly?: string; sort?: string };
}) {
  let rows = await fetchTop50();

  if (searchParams.market) {
    rows = rows.filter((r) => r.market === searchParams.market);
  }
  if (searchParams.confidence) {
    rows = rows.filter((r) => r.confidence === searchParams.confidence);
  }
  if (searchParams.signalOnly === "1") {
    rows = rows.filter((r) => r.isSignal);
  }
  if (searchParams.sort === "score") {
    rows = [...rows].sort((a, b) => b.score - a.score);
  }

  return (
    <div className="grid" style={{ gap: 12 }}>
      <div className="card">
        <h1>日米混合 Top50</h1>
        <form className="grid two" style={{ alignItems: "end", marginTop: 10 }}>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="market">市場</label>
            <select id="market" name="market" defaultValue={searchParams.market ?? ""}>
              <option value="">ALL</option>
              <option value="JP">JP</option>
              <option value="US">US</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="confidence">Confidence</label>
            <select id="confidence" name="confidence" defaultValue={searchParams.confidence ?? ""}>
              <option value="">ALL</option>
              <option value="High">High</option>
              <option value="Medium">Medium</option>
              <option value="Low">Low</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="signalOnly">Signalのみ</label>
            <select id="signalOnly" name="signalOnly" defaultValue={searchParams.signalOnly ?? "0"}>
              <option value="0">No</option>
              <option value="1">Yes</option>
            </select>
          </div>
          <div className="grid" style={{ gap: 6 }}>
            <label htmlFor="sort">Sort</label>
            <select id="sort" name="sort" defaultValue={searchParams.sort ?? "rank"}>
              <option value="rank">Rank</option>
              <option value="score">Score</option>
            </select>
          </div>
          <div>
            <button type="submit">適用</button>
          </div>
        </form>
      </div>

      <div className="card">
        <Top50Table rows={rows} />
      </div>
    </div>
  );
}
