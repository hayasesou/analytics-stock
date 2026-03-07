from __future__ import annotations

from src.jobs import discord_listener as discord_listener_job


def test_resolve_discord_ingest_cfg_normalizes_values() -> None:
    cfg = discord_listener_job._resolve_discord_ingest_cfg(  # noqa: SLF001
        {
            "discord_ingest": {
                "enabled": True,
                "inbox_channel_name": "#InBoX",
                "max_urls_per_message": "4",
                "reply_in_thread": "true",
                "allow_bot_messages": "false",
                "allowed_guild_ids": ["123", "456"],
            }
        }
    )
    assert cfg["enabled"] is True
    assert cfg["inbox_channel_name"] == "inbox"
    assert cfg["max_urls_per_message"] == 4
    assert cfg["reply_in_thread"] is True
    assert cfg["allow_bot_messages"] is False
    assert cfg["allowed_guild_ids"] == {"123", "456"}


def test_message_is_target_checks_channel_guild_and_bot() -> None:
    cfg = {
        "inbox_channel_name": "inbox",
        "allow_bot_messages": False,
        "allowed_guild_ids": {"111"},
    }
    assert (
        discord_listener_job._message_is_target(  # noqa: SLF001
            channel_name="inbox",
            guild_id="111",
            is_bot_author=False,
            cfg=cfg,
        )
        is True
    )
    assert (
        discord_listener_job._message_is_target(  # noqa: SLF001
            channel_name="other",
            guild_id="111",
            is_bot_author=False,
            cfg=cfg,
        )
        is False
    )
    assert (
        discord_listener_job._message_is_target(  # noqa: SLF001
            channel_name="inbox",
            guild_id="999",
            is_bot_author=False,
            cfg=cfg,
        )
        is False
    )
    assert (
        discord_listener_job._message_is_target(  # noqa: SLF001
            channel_name="inbox",
            guild_id="111",
            is_bot_author=True,
            cfg=cfg,
        )
        is False
    )


def test_recent_url_cache_respects_window() -> None:
    cache = discord_listener_job._RecentUrlCache(window_sec=60.0)  # noqa: SLF001
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert cache.seen_recently(url, now_monotonic=100.0) is False
    cache.mark(url, now_monotonic=100.0)
    assert cache.seen_recently(url, now_monotonic=120.0) is True
    assert cache.seen_recently(url, now_monotonic=200.1) is False


def test_format_ingest_result_includes_counts() -> None:
    text = discord_listener_job._format_ingest_result(  # noqa: SLF001
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        {
            "enabled": True,
            "idea_id": "idea-1",
            "inserted_ideas": 1,
            "comment_count": 200,
            "edge_count": 3,
            "enqueued_tasks": 1,
        },
    )
    assert "ingest完了" in text
    assert "idea-1" in text
    assert "comments: 200" in text
