from __future__ import annotations

from datetime import datetime

import requests


class DiscordNotifier:
    def __init__(self, webhook_url: str | None, timeout_sec: int = 10):
        self.webhook_url = webhook_url
        self.timeout_sec = timeout_sec

    def _post(self, content: str) -> None:
        if not self.webhook_url:
            return
        requests.post(
            self.webhook_url,
            json={"content": content[:1900]},
            timeout=self.timeout_sec,
        ).raise_for_status()

    def send_daily_event_digest(self, now: datetime, events: list[dict[str, str]]) -> None:
        high = [e for e in events if e["importance"] == "high"]
        med = [e for e in events if e["importance"] == "medium"]
        low = [e for e in events if e["importance"] == "low"]

        lines = [f"[Daily 20:00 JST] 重要イベントまとめ ({now.strftime('%Y-%m-%d')})"]

        lines.append(f"High: {len(high)}")
        for e in high[:8]:
            lines.append(f"- {e['title']} | {e['summary']} | {e.get('source_url', '-')}")

        lines.append(f"Medium: {len(med)}")
        for e in med[:8]:
            lines.append(f"- {e['title']} | {e['summary']}")

        lines.append(f"Low: {len(low)} 件")
        self._post("\n".join(lines))

    def send_weekly_links(self, base_url: str, as_of: datetime) -> None:
        lines = [
            f"[Weekly] 更新完了 ({as_of.date().isoformat()})",
            f"- Top50: {base_url.rstrip('/')}/top50",
            f"- Weekly Summary: {base_url.rstrip('/')}/reports/weekly",
            f"- Backtest: {base_url.rstrip('/')}/backtest",
        ]
        self._post("\n".join(lines))
