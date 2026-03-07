from __future__ import annotations

from datetime import datetime

import pytest

from src.config import RuntimeSecrets
from src.data.provider import HybridDataProvider


def _secrets() -> RuntimeSecrets:
    return RuntimeSecrets(
        database_url="postgresql://placeholder",
        discord_webhook_url=None,
        discord_bot_token=None,
        openai_api_key=None,
        r2_account_id=None,
        r2_access_key_id=None,
        r2_secret_access_key=None,
        r2_bucket_evidence=None,
        r2_bucket_data=None,
        r2_endpoint=None,
        jquants_api_key=None,
        jquants_email=None,
        jquants_password=None,
        massive_api_key=None,
        edinet_api_key="dummy-edinet-key",
        sec_user_agent="stock-analysis-test",
    )


def test_load_recent_events_prefers_live_sec(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 2, 15, 12, 0, 0)

    class _FakeSecClient:
        def __init__(self, user_agent: str):
            _ = user_agent

        def fetch_current_filings(self, count: int = 100) -> list[dict[str, str]]:
            _ = count
            return [
                {
                    "title": "8-K - Example Corp (0000000000) (Filer)",
                    "updated": "2026-02-15T10:00:00+09:00",
                    "form_type": "8-K",
                    "company_name": "Example Corp",
                    "summary": "Material event filing submitted.",
                    "source_url": "https://www.sec.gov/ixviewer/ix.html?doc=/Archives/example.htm",
                }
            ]

    class _FakeEdinetClient:
        def __init__(self, api_key: str | None):
            _ = api_key

        def available(self) -> bool:
            return False

    monkeypatch.setattr("src.data.provider.SecEdgarClient", _FakeSecClient)
    monkeypatch.setattr("src.data.provider.EdinetClient", _FakeEdinetClient)

    provider = HybridDataProvider(secrets=_secrets())
    events = provider.load_recent_events(now=now, hours=24)

    assert len(events) == 1
    event = events[0]
    assert event.title == "8-K: Example Corp"
    assert event.importance == "high"
    assert event.metadata.get("source") == "sec"
    assert "Mock event generated" not in event.summary


def test_load_recent_events_returns_empty_when_live_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 2, 15, 12, 0, 0)

    class _FakeSecClient:
        def __init__(self, user_agent: str):
            _ = user_agent

        def fetch_current_filings(self, count: int = 100) -> list[dict[str, str]]:
            _ = count
            return []

    class _FakeEdinetClient:
        def __init__(self, api_key: str | None):
            _ = api_key

        def available(self) -> bool:
            return False

    monkeypatch.setattr("src.data.provider.SecEdgarClient", _FakeSecClient)
    monkeypatch.setattr("src.data.provider.EdinetClient", _FakeEdinetClient)

    provider = HybridDataProvider(secrets=_secrets(), seed=42)
    events = provider.load_recent_events(now=now, hours=24)

    assert events == []
