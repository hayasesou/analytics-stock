from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.jobs import crypto_marketdata as crypto_marketdata_job
from src.types import CryptoMarketSnapshot


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.snapshots: list[CryptoMarketSnapshot] = []
        self.quality_rows = []

    def insert_crypto_market_snapshots(self, snapshots):  # noqa: ANN001
        self.snapshots.extend(snapshots)
        return len(snapshots)

    def insert_crypto_data_quality_snapshots(self, rows):  # noqa: ANN001
        self.quality_rows.extend(rows)
        return len(rows)

    def fetch_crypto_market_inputs_for_edge(
        self,
        max_missing_ratio: float = 0.25,
        max_latency_ms: float = 3000.0,
        lookback_minutes: int = 60,  # noqa: ARG002
        limit: int = 200,  # noqa: ARG002
    ):
        out = []
        for row in self.quality_rows:
            latency = row.latency_p95_ms if row.latency_p95_ms is not None else (max_latency_ms + 1.0)
            if row.missing_ratio <= max_missing_ratio and latency <= max_latency_ms and row.eligible_for_edge:
                out.append({"exchange": row.exchange, "symbol": row.symbol, "market_type": row.market_type})
        return out


class _FakeBinanceClient:
    def __init__(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
        pass

    def fetch_market_snapshot(
        self,
        symbol: str,
        market_type: str = "perp",
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshot:
        ts = observed_at or datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
        if market_type == "spot":
            return CryptoMarketSnapshot(
                exchange="binance",
                symbol=symbol,
                market_type="spot",
                observed_at=ts,
                best_bid=100.0,
                best_ask=100.2,
                mid=100.1,
                spread_bps=19.98,
                source_mode="ws",
                latency_ms=120.0,
                data_quality={"ws_failed": False},
            )

        return CryptoMarketSnapshot(
            exchange="binance",
            symbol=symbol,
            market_type="perp",
            observed_at=ts,
            best_bid=100.0,
            best_ask=100.2,
            mid=100.1,
            spread_bps=19.98,
            funding_rate=0.0001,
            open_interest=1234.0,
            mark_price=100.12,
            index_price=100.0,
            basis_bps=12.0,
            source_mode="rest",
            latency_ms=180.0,
            data_quality={"ws_failed": True},
        )


class _FakeHyperliquidClient:
    def __init__(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
        pass

    def fetch_market_snapshot(
        self,
        symbol: str,
        market_type: str = "perp",
        observed_at: datetime | None = None,
    ) -> CryptoMarketSnapshot:
        ts = observed_at or datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
        return CryptoMarketSnapshot(
            exchange="hyperliquid",
            symbol=symbol,
            market_type=market_type,
            observed_at=ts,
            best_bid=99.9,
            best_ask=100.1,
            mid=100.0,
            spread_bps=20.0,
            funding_rate=0.00015,
            open_interest=456.0,
            mark_price=100.02,
            index_price=100.0,
            basis_bps=2.0,
            source_mode="ws",
            latency_ms=110.0,
            data_quality={"ws_failed": False},
        )


def test_run_crypto_marketdata_collects_and_persists(monkeypatch) -> None:
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(
        crypto_marketdata_job,
        "load_yaml_config",
        lambda: {
            "timezone": "Asia/Tokyo",
            "crypto_marketdata": {
                "enabled": True,
                "run_minute_jst": 3,
                "quality_window_sec": 60,
                "pairs": [
                    {
                        "symbol": "BTC",
                        "binance_symbol": "BTCUSDT",
                        "hyperliquid_symbol": "BTC",
                        "collect_spot": True,
                        "collect_perp": True,
                    }
                ],
                "binance": {"enabled": True},
                "hyperliquid": {"enabled": True},
                "data_quality": {"max_missing_ratio": 0.25, "max_latency_ms": 3000, "lookback_minutes": 60},
            },
        },
    )
    monkeypatch.setattr(
        crypto_marketdata_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused"),
    )
    monkeypatch.setattr(crypto_marketdata_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(crypto_marketdata_job, "BinanceMarketClient", _FakeBinanceClient)
    monkeypatch.setattr(crypto_marketdata_job, "HyperliquidMarketClient", _FakeHyperliquidClient)

    result = crypto_marketdata_job.run_crypto_marketdata(now=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc))

    assert result["enabled"] is True
    assert result["inserted_snapshots"] == 3
    assert result["inserted_data_quality"] == 3
    assert result["eligible_for_edge"] == 3
    assert result["errors"] == []
    assert len(fake_repo.snapshots) == 3
    assert any(s.source_mode == "rest" for s in fake_repo.snapshots)


def test_run_crypto_marketdata_marks_missing_as_not_eligible(monkeypatch) -> None:
    class _MissingFundingBinance(_FakeBinanceClient):
        def fetch_market_snapshot(
            self,
            symbol: str,
            market_type: str = "perp",
            observed_at: datetime | None = None,
        ) -> CryptoMarketSnapshot:
            ts = observed_at or datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
            return CryptoMarketSnapshot(
                exchange="binance",
                symbol=symbol,
                market_type=market_type,
                observed_at=ts,
                best_bid=100.0,
                best_ask=100.2,
                mid=100.1,
                spread_bps=19.98,
                funding_rate=None,
                source_mode="ws",
                latency_ms=120.0,
                data_quality={"ws_failed": False},
            )

    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(
        crypto_marketdata_job,
        "load_yaml_config",
        lambda: {
            "timezone": "Asia/Tokyo",
            "crypto_marketdata": {
                "enabled": True,
                "pairs": [
                    {
                        "symbol": "BTC",
                        "binance_symbol": "BTCUSDT",
                        "collect_spot": False,
                        "collect_perp": True,
                    }
                ],
                "binance": {"enabled": True},
                "hyperliquid": {"enabled": False},
                "data_quality": {"max_missing_ratio": 0.0, "max_latency_ms": 3000, "lookback_minutes": 60},
            },
        },
    )
    monkeypatch.setattr(
        crypto_marketdata_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused"),
    )
    monkeypatch.setattr(crypto_marketdata_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(crypto_marketdata_job, "BinanceMarketClient", _MissingFundingBinance)
    monkeypatch.setattr(crypto_marketdata_job, "HyperliquidMarketClient", _FakeHyperliquidClient)

    result = crypto_marketdata_job.run_crypto_marketdata(now=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc))

    assert result["inserted_snapshots"] == 1
    assert result["inserted_data_quality"] == 1
    assert result["eligible_for_edge"] == 0
    assert fake_repo.quality_rows[0].eligible_for_edge is False
