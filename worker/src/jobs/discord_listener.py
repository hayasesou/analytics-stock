from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
import time
from typing import Any

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import extract_youtube_urls
from src.jobs.ingest_youtube import run_ingest_youtube


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(minimum, min(maximum, out))


def _to_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        out = default
    return max(minimum, min(maximum, out))


def _normalize_channel_name(name: Any) -> str:
    text = str(name or "").strip().lower()
    if text.startswith("#"):
        text = text[1:]
    return text


def _resolve_discord_ingest_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("discord_ingest", {})
    if not isinstance(root, dict):
        root = {}
    allowed_guild_ids_raw = root.get("allowed_guild_ids", [])
    if not isinstance(allowed_guild_ids_raw, list):
        allowed_guild_ids_raw = []
    allowed_guild_ids = [str(x).strip() for x in allowed_guild_ids_raw if str(x).strip()]
    return {
        "enabled": _to_bool(root.get("enabled"), True),
        "inbox_channel_name": _normalize_channel_name(root.get("inbox_channel_name", "inbox")),
        "max_urls_per_message": _to_int(root.get("max_urls_per_message"), 3, 1, 20),
        "reply_in_thread": _to_bool(root.get("reply_in_thread"), True),
        "allow_bot_messages": _to_bool(root.get("allow_bot_messages"), False),
        "message_dedupe_cache_size": _to_int(root.get("message_dedupe_cache_size"), 5000, 100, 50000),
        "url_dedupe_window_sec": _to_float(root.get("url_dedupe_window_sec"), 300.0, 0.0, 86400.0),
        "allowed_guild_ids": set(allowed_guild_ids),
    }


def _message_is_target(
    *,
    channel_name: str | None,
    guild_id: str | None,
    is_bot_author: bool,
    cfg: dict[str, Any],
) -> bool:
    if is_bot_author and not bool(cfg["allow_bot_messages"]):
        return False
    if _normalize_channel_name(channel_name) != str(cfg["inbox_channel_name"]):
        return False
    allowed_guild_ids = cfg.get("allowed_guild_ids") or set()
    if allowed_guild_ids and str(guild_id or "") not in allowed_guild_ids:
        return False
    return True


def _format_ingest_result(url: str, result: dict[str, Any]) -> str:
    if not bool(result.get("enabled", True)):
        return f"⏸ ingest disabled: {url}"
    if int(result.get("inserted_ideas", 0) or 0) <= 0:
        deduped = int(result.get("deduped_claims", 0) or 0)
        return f"↩️ 既存URLのためスキップ: {url} (deduped={deduped})"

    edge_count = int(result.get("edge_count", 0) or 0)
    comment_count = int(result.get("comment_count", 0) or 0)
    return (
        f"✅ ingest完了: {url}\n"
        f"- idea_id: {result.get('idea_id', '-')}\n"
        f"- comments: {comment_count}\n"
        f"- extracted_edges: {edge_count}\n"
        f"- enqueued_tasks: {int(result.get('enqueued_tasks', 0) or 0)}"
    )


class _RecentUrlCache:
    def __init__(self, window_sec: float):
        self.window_sec = max(0.0, float(window_sec))
        self._by_url: dict[str, float] = {}

    def seen_recently(self, url: str, now_monotonic: float | None = None) -> bool:
        if self.window_sec <= 0:
            return False
        now = now_monotonic if now_monotonic is not None else time.monotonic()
        ts = self._by_url.get(url)
        if ts is None:
            return False
        return (now - ts) <= self.window_sec

    def mark(self, url: str, now_monotonic: float | None = None) -> None:
        now = now_monotonic if now_monotonic is not None else time.monotonic()
        self._by_url[url] = now


def run_discord_listener() -> None:
    cfg = load_yaml_config()
    ingest_cfg = _resolve_discord_ingest_cfg(cfg)
    if not bool(ingest_cfg["enabled"]):
        print("[discord-listener] disabled", flush=True)
        return

    secrets = load_runtime_secrets()
    bot_token = str(getattr(secrets, "discord_bot_token", "") or "").strip()
    if not bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")

    try:
        import discord  # type: ignore[import-untyped]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("discord.py is required for discord_listener job") from exc

    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.message_content = True

    client = discord.Client(intents=intents)
    processed_message_ids: deque[int] = deque(maxlen=int(ingest_cfg["message_dedupe_cache_size"]))
    processed_message_id_set: set[int] = set()
    recent_url_cache = _RecentUrlCache(window_sec=float(ingest_cfg["url_dedupe_window_sec"]))

    async def _send_message(channel: Any, text: str) -> None:
        body = str(text).strip()
        if not body:
            return
        await channel.send(body[:1800])

    async def _resolve_reply_channel(message: Any) -> Any:
        if not bool(ingest_cfg["reply_in_thread"]):
            return message.channel

        channel = message.channel
        if getattr(channel, "type", None) and str(getattr(channel, "type")).lower().endswith("thread"):
            return channel
        create_thread = getattr(message, "create_thread", None)
        if create_thread is None:
            return channel
        try:
            thread_name = f"yt-ingest-{getattr(message, 'id', 'unknown')}"
            return await create_thread(name=thread_name[:80], auto_archive_duration=60)
        except Exception:  # noqa: BLE001
            return channel

    @client.event
    async def on_ready() -> None:
        print(f"[discord-listener] connected as {client.user}", flush=True)

    @client.event
    async def on_message(message: Any) -> None:
        if client.user is not None and getattr(message.author, "id", None) == getattr(client.user, "id", None):
            return

        message_id = int(getattr(message, "id", 0) or 0)
        if message_id <= 0:
            return
        if message_id in processed_message_id_set:
            return

        channel_name = str(getattr(getattr(message, "channel", None), "name", "") or "")
        guild_id = str(getattr(getattr(message, "guild", None), "id", "") or "")
        is_bot_author = bool(getattr(getattr(message, "author", None), "bot", False))
        if not _message_is_target(
            channel_name=channel_name,
            guild_id=guild_id,
            is_bot_author=is_bot_author,
            cfg=ingest_cfg,
        ):
            return

        processed_message_ids.append(message_id)
        processed_message_id_set.add(message_id)
        if len(processed_message_id_set) > int(ingest_cfg["message_dedupe_cache_size"]):
            while processed_message_ids:
                old_id = processed_message_ids.popleft()
                processed_message_id_set.discard(old_id)
                if len(processed_message_id_set) <= int(ingest_cfg["message_dedupe_cache_size"]):
                    break

        urls = extract_youtube_urls(
            str(getattr(message, "content", "") or ""),
            max_urls=int(ingest_cfg["max_urls_per_message"]),
        )
        if not urls:
            return

        reply_channel = await _resolve_reply_channel(message)
        await _send_message(reply_channel, f"🔎 YouTube ingest start ({len(urls)} URL)")

        for url in urls:
            if recent_url_cache.seen_recently(url):
                await _send_message(reply_channel, f"⏭ recent duplicate skip: {url}")
                continue
            recent_url_cache.mark(url)
            try:
                result = await asyncio.to_thread(
                    run_ingest_youtube,
                    url=url,
                    now=datetime.now(timezone.utc),
                )
                await _send_message(reply_channel, _format_ingest_result(url, result))
            except Exception as exc:  # noqa: BLE001
                await _send_message(reply_channel, f"❌ ingest failed: {url}\n- error: {exc}")

    client.run(bot_token)
