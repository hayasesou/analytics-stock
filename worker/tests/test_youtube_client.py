from __future__ import annotations

from typing import Any

import pytest

from src.integrations.youtube import YouTubeClient, extract_video_id, normalize_youtube_url


class _FakeResponse:
    def __init__(self, status_code: int, body: Any) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Any:
        return self._body


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float) -> _FakeResponse:
        self.calls.append(
            {
                "url": url,
                "params": dict(params),
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("no fake response left")
        return self._responses.pop(0)


def test_extract_video_id_and_normalize_url() -> None:
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert normalize_youtube_url("dQw4w9WgXcQ") == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def test_extract_video_id_raises_for_invalid_url() -> None:
    with pytest.raises(ValueError, match="video_id_not_found"):
        extract_video_id("https://www.example.com/watch?v=abc")


def test_youtube_client_retries_on_server_error_for_metadata() -> None:
    session = _FakeSession(
        [
            _FakeResponse(status_code=500, body={"error": {"message": "temporary failure"}}),
            _FakeResponse(
                status_code=200,
                body={
                    "items": [
                        {
                            "snippet": {
                                "title": "Macro Update",
                                "description": "BTC outlook",
                                "channelId": "chan-1",
                                "channelTitle": "Macro Lab",
                                "publishedAt": "2026-02-20T09:00:00Z",
                            },
                            "statistics": {"viewCount": "123", "commentCount": "45"},
                            "contentDetails": {"duration": "PT10M"},
                        }
                    ]
                },
            ),
        ]
    )
    client = YouTubeClient(
        api_key="test-key",
        timeout_sec=2.0,
        retry_count=1,
        backoff_sec=0.0,
        session=session,  # type: ignore[arg-type]
        base_url="https://unit.test/youtube",
    )

    metadata = client.fetch_video_metadata("dQw4w9WgXcQ")

    assert metadata["video_id"] == "dQw4w9WgXcQ"
    assert metadata["title"] == "Macro Update"
    assert metadata["view_count"] == 123
    assert metadata["comment_count"] == 45
    assert len(session.calls) == 2
    assert session.calls[0]["url"] == "https://unit.test/youtube/videos"
