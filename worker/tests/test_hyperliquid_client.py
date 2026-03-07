from __future__ import annotations

from datetime import datetime, timezone

from src.integrations.hyperliquid import HyperliquidMarketClient


def test_hyperliquid_snapshot_falls_back_to_rest_when_ws_fails() -> None:
    client = HyperliquidMarketClient(rest_timeout_sec=1.0, ws_timeout_sec=0.2)

    def _raise_ws(symbol: str):  # noqa: ARG001
        raise RuntimeError("ws unavailable")

    client.fetch_l2_book_ws = _raise_ws  # type: ignore[method-assign]
    client.fetch_l2_book_rest = lambda symbol: {  # type: ignore[method-assign]  # noqa: ARG005
        "best_bid": 99.8,
        "best_ask": 100.0,
        "raw": {"source": "rest"},
    }
    client.fetch_market_context_rest = lambda symbol: {  # type: ignore[method-assign]  # noqa: ARG005
        "funding_rate": 0.00012,
        "open_interest": 456.7,
        "mark_price": 99.9,
        "index_price": 99.8,
        "basis_bps": 10.0,
        "raw": {"source": "rest"},
    }

    snapshot = client.fetch_market_snapshot(
        symbol="BTC",
        market_type="perp",
        observed_at=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot.exchange == "hyperliquid"
    assert snapshot.symbol == "BTC"
    assert snapshot.source_mode == "rest"
    assert snapshot.mid == 99.9
    assert snapshot.funding_rate == 0.00012
    assert snapshot.data_quality["ws_failed"] is True


def test_hyperliquid_rejects_non_perp_market() -> None:
    client = HyperliquidMarketClient(rest_timeout_sec=1.0, ws_timeout_sec=0.2)
    try:
        client.fetch_market_snapshot(symbol="BTC", market_type="spot")
    except ValueError as exc:
        assert "perp" in str(exc)
    else:
        raise AssertionError("expected ValueError")
