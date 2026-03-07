from __future__ import annotations

from src.jobs import discord_research_listener as listener_job


def test_resolve_cfg_normalizes_defaults() -> None:
    cfg = listener_job._resolve_cfg(  # noqa: SLF001
        {
            "discord_research": {
                "enabled": "true",
                "auto_thread": "false",
                "max_urls_per_message": "9",
            }
        }
    )
    assert cfg["enabled"] is True
    assert cfg["auto_thread"] is False
    assert cfg["max_urls_per_message"] == 5


def test_extract_urls_and_security_id() -> None:
    text = "@bot https://example.com/a https://example.com/b US:NVDA is interesting"

    urls = listener_job._extract_urls(text, limit=5)  # noqa: SLF001
    security_id = listener_job._extract_security_id(text)  # noqa: SLF001

    assert urls == ["https://example.com/a", "https://example.com/b"]
    assert security_id == "US:NVDA"


def test_build_answer_contains_four_blocks() -> None:
    answer = listener_job._build_answer(  # noqa: SLF001
        hypotheses=[
            {
                "stance": "watch",
                "falsification_md": "Demand weakens.",
            }
        ],
        url_count=2,
        security_id="US:NVDA",
    )

    assert "## 結論" in answer
    assert "## 根拠" in answer
    assert "## 反証" in answer
    assert "## 次アクション" in answer


def test_bootstrap_session_includes_web_session_url(monkeypatch) -> None:
    class _FakeRepo:
        def create_chat_session(self, title: str) -> str:  # noqa: ARG002
            return "session-1"

        def append_chat_message(self, **kwargs):  # noqa: ANN003
            self.last_message = kwargs
            return "message-1"

        def insert_research_external_input(self, spec):  # noqa: ANN001
            return "input-1"

        def insert_research_hypothesis(self, spec):  # noqa: ANN001
            return "hyp-1"

        def insert_research_artifact(self, spec):  # noqa: ANN001
            return "artifact-1"

        def enqueue_agent_task(self, **kwargs):  # noqa: ANN003
            return "task-1"

    monkeypatch.setenv("WEB_BASE_URL", "https://example.test")
    repo = _FakeRepo()

    session_id, answer = listener_job._bootstrap_session(  # noqa: SLF001
        repo,
        content="US:NVDA https://example.com material",
        source_label="discord",
        discord_channel_id="thread-1",
        discord_source_message_id="message-raw-1",
    )

    assert session_id == "session-1"
    assert "https://example.test/research/chat?sessionId=session-1" in answer
