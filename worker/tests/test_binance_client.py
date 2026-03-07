from __future__ import annotations

from datetime import datetime, timezone

from src.integrations.binance import BinanceMarketClient


def test_binance_snapshot_falls_back_to_rest_when_ws_fails() -> None:
    client = BinanceMarketClient(rest_timeout_sec=1.0, ws_timeout_sec=0.2)

    def _raise_ws(symbol: str, market_type: str = "perp"):  # noqa: ARG001
        raise RuntimeError("ws unavailable")

    client.fetch_book_ticker_ws = _raise_ws  # type: ignore[method-assign]
    client.fetch_book_ticker_rest = lambda symbol, market_type="perp": {  # type: ignore[method-assign]  # noqa: ARG005
        "best_bid": 100.0,
        "best_ask": 100.2,
        "raw": {"source": "rest"},
    }
    client.fetch_perp_metrics_rest = lambda symbol: {  # type: ignore[method-assign]  # noqa: ARG005
        "funding_rate": 0.0001,
        "mark_price": 100.15,
        "index_price": 100.0,
        "open_interest": 1234.0,
        "basis_bps": 15.0,
        "raw": {"source": "rest"},
    }

    snapshot = client.fetch_market_snapshot(
        symbol="BTCUSDT",
        market_type="perp",
        observed_at=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot.exchange == "binance"
    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.market_type == "perp"
    assert snapshot.source_mode == "rest"
    assert snapshot.mid == 100.1
    assert snapshot.spread_bps is not None
    assert snapshot.funding_rate == 0.0001
    assert snapshot.data_quality["ws_failed"] is True


def test_binance_spot_snapshot_skips_perp_metrics() -> None:
    client = BinanceMarketClient(rest_timeout_sec=1.0, ws_timeout_sec=0.2)
    client.fetch_book_ticker_ws = lambda symbol, market_type="spot": {  # type: ignore[method-assign]  # noqa: ARG005
        "best_bid": 2500.0,
        "best_ask": 2500.5,
        "raw": {"source": "ws"},
    }

    snapshot = client.fetch_market_snapshot(
        symbol="ETHUSDT",
        market_type="spot",
        observed_at=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot.market_type == "spot"
    assert snapshot.source_mode == "ws"
    assert snapshot.funding_rate is None
    assert snapshot.open_interest is None
