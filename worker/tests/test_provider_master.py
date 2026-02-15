from __future__ import annotations

import pytest

from src.config import RuntimeSecrets
from src.data.provider import HybridDataProvider


def test_normalize_jp_code() -> None:
    assert HybridDataProvider._normalize_jp_code("13010") == "1301"
    assert HybridDataProvider._normalize_jp_code("1301") == "1301"
    assert HybridDataProvider._normalize_jp_code("  72030  ") == "7203"
    assert HybridDataProvider._normalize_jp_code("130A0") is None
    assert HybridDataProvider._normalize_jp_code("ABC") is None
    assert HybridDataProvider._normalize_jp_code("123") is None


def test_build_us_securities_from_sec_rows_assigns_master_ids() -> None:
    rows = [
        {"ticker": "MSFT", "name": "Microsoft Corporation", "exchange": "NASDAQ"},
        {"ticker": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ"},
        {"ticker": "BRK.B", "name": "Berkshire Hathaway Inc.", "exchange": "NYSE"},
        {"ticker": "ZZZZZZZZ", "name": "invalid ticker length", "exchange": "NASDAQ"},
        {"ticker": "ETF1", "name": "Fund Placeholder", "exchange": "ARCA"},
    ]

    securities = HybridDataProvider._build_us_securities_from_sec_rows(rows)
    assert [s.security_id for s in securities] == ["US:AAPL", "US:BRK.B", "US:MSFT"]
    assert [s.ticker for s in securities] == ["AAPL", "BRK.B", "MSFT"]
    assert [s.name for s in securities] == [
        "Apple Inc.",
        "Berkshire Hathaway Inc.",
        "Microsoft Corporation",
    ]
    assert all(s.metadata.get("source") == "sec" for s in securities)


def test_build_us_securities_from_massive_rows_assigns_master_ids() -> None:
    rows = [
        {"ticker": "NVDA", "name": "NVIDIA Corporation", "primary_exchange": "XNAS"},
        {"ticker": "GOOGL", "name": "Alphabet Inc.", "primary_exchange": "XNAS"},
        {"ticker": "META", "name": "Meta Platforms, Inc.", "primary_exchange": "XNAS"},
        {"ticker": "BAD/TICKER", "name": "invalid"},
    ]

    securities = HybridDataProvider._build_us_securities_from_massive_rows(rows)
    assert [s.security_id for s in securities] == ["US:GOOGL", "US:META", "US:NVDA"]
    assert [s.ticker for s in securities] == ["GOOGL", "META", "NVDA"]
    assert [s.name for s in securities] == [
        "Alphabet Inc.",
        "Meta Platforms, Inc.",
        "NVIDIA Corporation",
    ]
    assert all(s.metadata.get("source") == "massive" for s in securities)


def test_load_jp_securities_live_supports_v2_field_names(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeJQuantsClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            _ = (args, kwargs)

        def available(self) -> bool:
            return True

        def fetch_listed_info(self) -> list[dict]:
            return [
                {"Code": "13010", "CoName": "極洋", "MktNm": "プライム", "S33Nm": "水産・農林業", "Mkt": "0111"},
                {"Code": "13020", "CoName": "値なし市場", "MktNm": "ETF", "S33Nm": "水産・農林業", "Mkt": "0100"},
            ]

    monkeypatch.setattr("src.data.provider.JQuantsClient", _FakeJQuantsClient)

    provider = HybridDataProvider(
        secrets=RuntimeSecrets(
            database_url="postgresql://placeholder",
            discord_webhook_url=None,
            openai_api_key=None,
            r2_account_id=None,
            r2_access_key_id=None,
            r2_secret_access_key=None,
            r2_bucket_evidence=None,
            r2_bucket_data=None,
            r2_endpoint=None,
            jquants_api_key="test-api-key",
            jquants_email=None,
            jquants_password=None,
            massive_api_key=None,
            edinet_api_key=None,
            sec_user_agent="stock-analysis-test",
        )
    )

    securities = provider._load_jp_securities_live()
    assert len(securities) == 1
    assert securities[0].security_id == "JP:1301"
    assert securities[0].ticker == "1301"
    assert securities[0].name == "極洋"
    assert securities[0].sector == "水産・農林業"
    assert securities[0].metadata.get("market_name") == "プライム"
