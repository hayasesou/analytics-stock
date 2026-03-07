from __future__ import annotations

from datetime import datetime, timezone
import re
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests


YOUTUBE_DATA_API_BASE_URL = "https://www.googleapis.com/youtube/v3"


def _to_iso8601(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def extract_video_id(url_or_id: str) -> str:
    raw = str(url_or_id).strip()
    if not raw:
        raise ValueError("youtube_url_required")

    # Allow direct video id.
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw):
        return raw

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    if host in {"youtu.be", "www.youtu.be"}:
        candidate = path.strip("/").split("/", 1)[0]
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
            return candidate

    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if path == "/watch":
            qs = parse_qs(parsed.query or "")
            candidate = (qs.get("v") or [""])[0]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
                return candidate
        if path.startswith("/shorts/") or path.startswith("/live/") or path.startswith("/embed/"):
            candidate = path.strip("/").split("/", 1)[1] if "/" in path.strip("/") else ""
            candidate = candidate.split("/", 1)[0]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
                return candidate

    raise ValueError("youtube_video_id_not_found")


def normalize_youtube_url(url_or_id: str) -> str:
    video_id = extract_video_id(url_or_id)
    return f"https://www.youtube.com/watch?v={video_id}"


class YouTubeClient:
    def __init__(
        self,
        api_key: str,
        *,
        timeout_sec: float = 8.0,
        retry_count: int = 2,
        backoff_sec: float = 0.5,
        session: requests.Session | None = None,
        base_url: str = YOUTUBE_DATA_API_BASE_URL,
    ) -> None:
        key = str(api_key or "").strip()
        if not key:
            raise RuntimeError("YOUTUBE_API_KEY is required")
        self.api_key = key
        self.timeout_sec = max(1.0, float(timeout_sec))
        self.retry_count = max(0, int(retry_count))
        self.backoff_sec = max(0.0, float(backoff_sec))
        self.base_url = str(base_url).rstrip("/")
        self._session = session or requests.Session()

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(params)
        merged["key"] = self.api_key
        attempts = self.retry_count + 1
        last_error: Exception | None = None
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(attempts):
            try:
                resp = self._session.get(url, params=merged, timeout=self.timeout_sec)
                status = int(resp.status_code)
                if status >= 500:
                    raise RuntimeError(f"youtube_http_{status}")
                if status >= 400:
                    try:
                        payload = resp.json()
                    except Exception:  # noqa: BLE001
                        payload = {}
                    reason = ""
                    if isinstance(payload, dict):
                        error_obj = payload.get("error")
                        if isinstance(error_obj, dict):
                            reason = str(error_obj.get("message", "")).strip()
                    raise RuntimeError(f"youtube_http_{status}:{reason}")
                body = resp.json()
                if not isinstance(body, dict):
                    raise RuntimeError("youtube_response_non_object")
                return body
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= attempts - 1:
                    break
                time.sleep(self.backoff_sec * (attempt + 1))
        raise RuntimeError(f"youtube_request_failed:{last_error}")

    def fetch_video_metadata(self, video_id: str) -> dict[str, Any]:
        body = self._request_json(
            "/videos",
            {
                "part": "snippet,contentDetails,statistics",
                "id": str(video_id).strip(),
                "maxResults": 1,
            },
        )
        items = body.get("items")
        if not isinstance(items, list) or not items:
            raise RuntimeError("youtube_video_not_found")
        first = items[0] if isinstance(items[0], dict) else {}
        snippet = first.get("snippet") if isinstance(first.get("snippet"), dict) else {}
        statistics = first.get("statistics") if isinstance(first.get("statistics"), dict) else {}
        content_details = first.get("contentDetails") if isinstance(first.get("contentDetails"), dict) else {}
        canonical_url = normalize_youtube_url(video_id)
        return {
            "video_id": video_id,
            "url": canonical_url,
            "title": str(snippet.get("title", "")).strip(),
            "description": str(snippet.get("description", "")).strip(),
            "channel_id": str(snippet.get("channelId", "")).strip(),
            "channel_title": str(snippet.get("channelTitle", "")).strip(),
            "published_at": _to_iso8601(str(snippet.get("publishedAt", "")).strip()),
            "tags": list(snippet.get("tags", [])) if isinstance(snippet.get("tags"), list) else [],
            "duration": str(content_details.get("duration", "")).strip(),
            "view_count": int(float(statistics.get("viewCount", 0) or 0)),
            "comment_count": int(float(statistics.get("commentCount", 0) or 0)),
        }

    def fetch_comments(
        self,
        video_id: str,
        *,
        max_comments: int = 20000,
        max_pages: int = 200,
        max_duration_sec: float = 600.0,
    ) -> list[dict[str, Any]]:
        target = max(0, int(max_comments))
        if target == 0:
            return []
        page_cap = max(1, int(max_pages))
        duration_cap = max(1.0, float(max_duration_sec))
        comments: list[dict[str, Any]] = []
        page_token: str | None = None
        page_count = 0
        started = time.monotonic()
        while len(comments) < target:
            if page_count >= page_cap:
                break
            if (time.monotonic() - started) >= duration_cap:
                break
            per_page = min(100, target - len(comments))
            params: dict[str, Any] = {
                "part": "snippet",
                "videoId": str(video_id).strip(),
                "order": "relevance",
                "textFormat": "plainText",
                "maxResults": per_page,
            }
            if page_token:
                params["pageToken"] = page_token
            body = self._request_json("/commentThreads", params)
            page_count += 1
            items = body.get("items")
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                snippet = item.get("snippet")
                if not isinstance(snippet, dict):
                    continue
                top_level = snippet.get("topLevelComment")
                if not isinstance(top_level, dict):
                    continue
                top_snippet = top_level.get("snippet")
                if not isinstance(top_snippet, dict):
                    continue
                text = str(top_snippet.get("textDisplay", "")).strip()
                if not text:
                    continue
                comments.append(
                    {
                        "comment_id": str(top_level.get("id", "")).strip(),
                        "author": str(top_snippet.get("authorDisplayName", "")).strip(),
                        "text": text,
                        "like_count": int(float(top_snippet.get("likeCount", 0) or 0)),
                        "published_at": _to_iso8601(str(top_snippet.get("publishedAt", "")).strip()),
                    }
                )
                if len(comments) >= target:
                    break
            page_token = str(body.get("nextPageToken", "")).strip() or None
            if not page_token:
                break
        return comments

    def fetch_transcript(self, video_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        # Transcript retrieval from official Data API requires OAuth scopes.
        # Keep the API surface for future extension and return empty by default.
        return []

    def fetch_video_bundle(
        self,
        url_or_id: str,
        *,
        max_comments: int = 20000,
        max_comment_pages: int = 200,
        max_comment_duration_sec: float = 600.0,
    ) -> dict[str, Any]:
        video_id = extract_video_id(url_or_id)
        metadata = self.fetch_video_metadata(video_id)
        comments = self.fetch_comments(
            video_id,
            max_comments=max_comments,
            max_pages=max_comment_pages,
            max_duration_sec=max_comment_duration_sec,
        )
        transcript = self.fetch_transcript(video_id)
        return {
            "video_id": video_id,
            "url": normalize_youtube_url(video_id),
            "metadata": metadata,
            "comments": comments,
            "transcript": transcript,
        }
