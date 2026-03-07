from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest

from src.jobs import ingest_youtube as ingest_youtube_job


def _sample_bundle() -> dict[str, Any]:
    return {
        "metadata": {
            "video_id": "dQw4w9WgXcQ",
            "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "title": "BTC ETF inflow keeps accelerating in 2026",
            "description": (
                "BTC demand is expanding as institutional inflows rise. "
                "ETH network activity also improves with fee growth."
            ),
            "channel_title": "Macro Lab",
            "published_at": "2026-02-20T09:00:00Z",
        },
        "comments": [
            {"text": "SOL ecosystem user growth may boost on-chain fees this quarter."},
            {"text": "BNB supply burn supports tighter circulating supply over time."},
        ],
        "transcript": [
            {"text": "USDC usage in settlement rails keeps rising across exchanges."},
        ],
    }


class _FakeYouTubeClient:
    bundle: dict[str, Any] = {}
    init_calls: list[dict[str, Any]] = []
    fetch_calls: list[dict[str, Any]] = []

    def __init__(self, api_key: str, timeout_sec: float = 8.0, retry_count: int = 2, **kwargs: Any) -> None:  # noqa: ARG002
        self.__class__.init_calls.append(
            {
                "api_key": api_key,
                "timeout_sec": timeout_sec,
                "retry_count": retry_count,
            }
        )

    def fetch_video_bundle(
        self,
        url_or_id: str,
        *,
        max_comments: int = 20000,
        max_comment_pages: int = 200,
        max_comment_duration_sec: float = 600.0,
    ) -> dict[str, Any]:
        self.__class__.fetch_calls.append(
            {
                "url_or_id": url_or_id,
                "max_comments": max_comments,
                "max_comment_pages": max_comment_pages,
                "max_comment_duration_sec": max_comment_duration_sec,
            }
        )
        return dict(self.__class__.bundle)


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.existing_hashes: set[str] = set()
        self.doc_upserts: list[dict[str, Any]] = []
        self.created_ideas: list[Any] = []
        self.inserted_evidence: list[Any] = []
        self.enqueued_tasks: list[dict[str, Any]] = []
        self.fetch_hash_calls: list[dict[str, Any]] = []

    def fetch_idea_claim_hashes_by_source_url(self, *, source_type: str, source_url: str, limit: int = 1000) -> set[str]:
        self.fetch_hash_calls.append({"source_type": source_type, "source_url": source_url, "limit": limit})
        return set(self.existing_hashes)

    def upsert_document_with_version(self, **kwargs: Any) -> str:
        self.doc_upserts.append(dict(kwargs))
        return "doc-version-1"

    def create_idea(self, idea: Any) -> str:
        self.created_ideas.append(idea)
        return f"idea-{len(self.created_ideas)}"

    def insert_idea_evidence(self, evidence: Any) -> str:
        self.inserted_evidence.append(evidence)
        return f"evidence-{len(self.inserted_evidence)}"

    def enqueue_agent_task(self, task_type: str, payload: dict[str, Any], priority: int = 100) -> str:
        self.enqueued_tasks.append({"task_type": task_type, "payload": dict(payload), "priority": priority})
        return f"task-{len(self.enqueued_tasks)}"


def _ingest_cfg() -> dict[str, Any]:
    return {
        "youtube_ingest": {
            "enabled": True,
            "request_timeout_sec": 4.0,
            "retry_count": 1,
            "max_comments": 20,
            "max_comment_pages": 5,
            "max_comment_duration_sec": 120,
            "max_claims": 2,
            "min_claim_chars": 10,
            "idea_priority": 90,
            "task_priority": 35,
            "created_by": "discord-user",
        }
    }


def test_run_ingest_youtube_from_command_persists_idea_and_task(monkeypatch) -> None:
    fake_repo = _FakeRepo("postgresql://unused")
    _FakeYouTubeClient.bundle = _sample_bundle()
    _FakeYouTubeClient.init_calls.clear()
    _FakeYouTubeClient.fetch_calls.clear()

    monkeypatch.setattr(ingest_youtube_job, "load_yaml_config", _ingest_cfg)
    monkeypatch.setattr(
        ingest_youtube_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused"),
    )
    monkeypatch.setattr(ingest_youtube_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(ingest_youtube_job, "YouTubeClient", _FakeYouTubeClient)
    monkeypatch.setenv("YOUTUBE_API_KEY", "unit-test-key")

    result = ingest_youtube_job.run_ingest_youtube(
        command="/ingest_youtube https://youtu.be/dQw4w9WgXcQ",
        now=datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc),
    )

    assert result["enabled"] is True
    assert result["source_url"] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert result["inserted_ideas"] == 1
    assert result["inserted_evidence"] == 1
    assert result["enqueued_tasks"] == 1
    assert len(fake_repo.doc_upserts) == 1
    assert len(fake_repo.created_ideas) == 1
    assert len(fake_repo.inserted_evidence) == 1
    assert len(fake_repo.enqueued_tasks) == 1
    assert fake_repo.enqueued_tasks[0]["task_type"] == "idea_analysis"
    assert _FakeYouTubeClient.fetch_calls[0]["url_or_id"] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    first_idea = fake_repo.created_ideas[0]
    assert first_idea.source_type == "youtube"
    assert first_idea.metadata["video_id"] == "dQw4w9WgXcQ"
    assert first_idea.metadata["claim_hash"]
    assert first_idea.metadata["storage_unit"] == "url"
    assert isinstance(first_idea.metadata["extracted_edges"], list)


def test_run_ingest_youtube_dedupes_existing_url_hash(monkeypatch) -> None:
    fake_repo = _FakeRepo("postgresql://unused")
    bundle = _sample_bundle()
    _FakeYouTubeClient.bundle = bundle
    _FakeYouTubeClient.init_calls.clear()
    _FakeYouTubeClient.fetch_calls.clear()

    normalized_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    url_hash = hashlib.sha256(f"url:{normalized_url}".encode("utf-8")).hexdigest()
    fake_repo.existing_hashes = {url_hash}

    monkeypatch.setattr(ingest_youtube_job, "load_yaml_config", _ingest_cfg)
    monkeypatch.setattr(
        ingest_youtube_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused"),
    )
    monkeypatch.setattr(ingest_youtube_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(ingest_youtube_job, "YouTubeClient", _FakeYouTubeClient)
    monkeypatch.setenv("YOUTUBE_API_KEY", "unit-test-key")

    result = ingest_youtube_job.run_ingest_youtube(
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        now=datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc),
    )

    assert result["inserted_ideas"] == 0
    assert result["inserted_evidence"] == 0
    assert result["enqueued_tasks"] == 0
    assert result["deduped_claims"] >= 0
    assert len(fake_repo.doc_upserts) == 0
    assert len(fake_repo.created_ideas) == 0
    assert len(fake_repo.enqueued_tasks) == 0


def test_run_ingest_youtube_requires_url_input(monkeypatch) -> None:
    monkeypatch.setattr(ingest_youtube_job, "load_yaml_config", _ingest_cfg)
    with pytest.raises(ValueError, match="ingest_youtube_url_required"):
        ingest_youtube_job.run_ingest_youtube(command="hello world")
