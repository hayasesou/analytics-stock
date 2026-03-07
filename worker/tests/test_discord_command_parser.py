from __future__ import annotations

from src.integrations.discord import extract_youtube_urls, parse_ingest_youtube_command


def test_parse_ingest_youtube_command_accepts_slash_prefix() -> None:
    url = parse_ingest_youtube_command("/ingest_youtube https://youtu.be/dQw4w9WgXcQ")
    assert url == "https://youtu.be/dQw4w9WgXcQ"


def test_parse_ingest_youtube_command_accepts_bang_prefix() -> None:
    url = parse_ingest_youtube_command("!ingest_youtube https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    assert url == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def test_parse_ingest_youtube_command_returns_none_on_non_command() -> None:
    assert parse_ingest_youtube_command("hello world") is None
    assert parse_ingest_youtube_command("/ingest_youtube") is None


def test_extract_youtube_urls_normalizes_and_dedupes() -> None:
    urls = extract_youtube_urls(
        "check https://youtu.be/dQw4w9WgXcQ and https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    )
    assert urls == ["https://www.youtube.com/watch?v=dQw4w9WgXcQ"]


def test_extract_youtube_urls_ignores_non_youtube_links() -> None:
    urls = extract_youtube_urls("https://example.com/x https://vimeo.com/123")
    assert urls == []
