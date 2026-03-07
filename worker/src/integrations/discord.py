from __future__ import annotations

from datetime import datetime
import os
import re

import requests

from src.integrations.youtube import normalize_youtube_url


YOUTUBE_URL_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s<>()\[\]{}\"']+",
    flags=re.IGNORECASE,
)


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

    def send_edge_radar(
        self,
        now: datetime,
        scope: str,
        rows: list[dict[str, object]],
        top_n: int = 10,
    ) -> None:
        normalized_scope = str(scope).strip().lower()
        label = "crypto" if normalized_scope == "crypto" else "equities"
        lines = [f"[Edge Radar] scope={label} ({now.strftime('%Y-%m-%d %H:%M JST')})"]
        if not rows:
            lines.append("- candidates: 0")
            self._post("\n".join(lines))
            return

        def _score(row: dict[str, object]) -> float:
            value = row.get("edge_score")
            if value is None:
                return 0.0
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        def _fmt_bps(value: object) -> str:
            if value is None:
                return "N/A"
            try:
                return f"{float(value):+.2f}bps"
            except (TypeError, ValueError):
                return "N/A"

        def _fmt_confidence(value: object) -> str:
            if value is None:
                return "-"
            try:
                return f"{float(value):.2f}"
            except (TypeError, ValueError):
                return "-"

        ranked = sorted(rows, key=_score, reverse=True)[: max(1, int(top_n))]
        lines.append(f"- candidates: {len(rows)} / top: {len(ranked)}")
        for idx, row in enumerate(ranked, start=1):
            symbol = str(row.get("symbol", "-"))
            edge_score = _score(row)
            net_edge = _fmt_bps(row.get("expected_net_edge_bps"))
            distance = _fmt_bps(row.get("distance_to_entry_bps"))
            confidence = _fmt_confidence(row.get("confidence"))
            explain = str(row.get("explain", "")).strip()
            if explain:
                explain = f" | {explain}"
            lines.append(
                f"{idx}. {symbol} score={edge_score:.1f} net={net_edge} dist={distance} conf={confidence}{explain}"
            )
        self._post("\n".join(lines))

    def send_executor_alert(self, title: str, details: dict[str, object] | None = None) -> None:
        lines = [f"[Executor Alert] {title}"]
        payload = details or {}
        for key, value in payload.items():
            lines.append(f"- {key}: {value}")
        self._post("\n".join(lines))

    def send_risk_bulletin(
        self,
        now: datetime,
        items: list[dict[str, object]],
        top_n: int = 8,
    ) -> None:
        lines = [f"[Risk速報] ({now.strftime('%Y-%m-%d %H:%M JST')})"]
        if not items:
            lines.append("- alerts: 0")
            self._post("\n".join(lines))
            return

        clipped = items[: max(1, int(top_n))]
        lines.append(f"- alerts: {len(items)} / top: {len(clipped)}")
        for idx, item in enumerate(clipped, start=1):
            category = str(item.get("category", "risk")).strip() or "risk"
            title = str(item.get("title", category)).strip() or category
            strategy_version_id = str(item.get("strategy_version_id", "")).strip()
            intent_id = str(item.get("intent_id", "")).strip()
            detail = str(item.get("detail", "")).strip()

            parts = [f"{idx}. [{category}] {title}"]
            if strategy_version_id:
                parts.append(f"strategy={strategy_version_id}")
            if intent_id:
                parts.append(f"intent={intent_id}")
            if detail:
                parts.append(detail)
            lines.append(" | ".join(parts))

        self._post("\n".join(lines))

    def send_research_kanban(
        self,
        now: datetime,
        counts: dict[str, int],
        samples: dict[str, list[str]] | None = None,
    ) -> None:
        order = ["new", "analyzing", "rejected", "candidate", "paper", "live"]
        total = sum(max(0, int(counts.get(status, 0))) for status in order)
        lines = [
            f"[Research Kanban] ({now.strftime('%Y-%m-%d %H:%M JST')})",
            f"- total: {total}",
        ]

        sample_map = samples or {}
        for status in order:
            count = max(0, int(counts.get(status, 0)))
            lines.append(f"- {status}: {count}")
            lane_samples = sample_map.get(status) or []
            for raw in lane_samples[:2]:
                text = re.sub(r"\s+", " ", str(raw).strip())
                if text:
                    lines.append(f"  {status}> {text[:90]}")

        self._post("\n".join(lines))


def build_web_session_url(session_id: str) -> str | None:
    base_url = str(os.getenv("WEB_BASE_URL", "") or "").strip().rstrip("/")
    normalized_session_id = str(session_id or "").strip()
    if not base_url or not normalized_session_id:
        return None
    return f"{base_url}/research/chat?sessionId={normalized_session_id}"


def _bot_headers(bot_token: str) -> dict[str, str]:
    return {"Authorization": f"Bot {bot_token}"}


def send_bot_message(bot_token: str | None, channel_id: str | None, content: str, timeout_sec: int = 10) -> None:
    token = str(bot_token or "").strip()
    target_channel_id = str(channel_id or "").strip()
    if not token or not target_channel_id:
        return
    requests.post(
        f"https://discord.com/api/v10/channels/{target_channel_id}/messages",
        headers={**_bot_headers(token), "Content-Type": "application/json"},
        json={"content": str(content or "")[:1900]},
        timeout=timeout_sec,
    ).raise_for_status()


def send_bot_file(
    bot_token: str | None,
    channel_id: str | None,
    *,
    filename: str,
    content: bytes,
    message: str | None = None,
    content_type: str = "application/octet-stream",
    timeout_sec: int = 20,
) -> None:
    token = str(bot_token or "").strip()
    target_channel_id = str(channel_id or "").strip()
    safe_filename = str(filename or "").strip() or "attachment.bin"
    if not token or not target_channel_id or not content:
        return
    requests.post(
        f"https://discord.com/api/v10/channels/{target_channel_id}/messages",
        headers=_bot_headers(token),
        data={"content": str(message or "")[:1900]},
        files={"files[0]": (safe_filename, content, content_type)},
        timeout=timeout_sec,
    ).raise_for_status()


def parse_ingest_youtube_command(message_text: str) -> str | None:
    text = str(message_text or "").strip()
    if not text:
        return None
    first_line = text.splitlines()[0].strip()
    match = re.match(r"^(?:/|!)?ingest_youtube\s+(\S+)\s*$", first_line, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def extract_youtube_urls(message_text: str, max_urls: int = 5) -> list[str]:
    text = str(message_text or "")
    if not text:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for match in YOUTUBE_URL_PATTERN.finditer(text):
        raw = str(match.group(0)).strip()
        if not raw:
            continue
        normalized: str | None = None
        try:
            normalized = normalize_youtube_url(raw)
        except ValueError:
            normalized = None
        if not normalized or normalized in seen:
            continue
        out.append(normalized)
        seen.add(normalized)
        if len(out) >= max(1, int(max_urls)):
            break
    return out
