from __future__ import annotations

from src.analytics.strategies import evaluate_cash_carry_edge, evaluate_perp_perp_edge


def test_evaluate_perp_perp_edge_returns_eligible_when_net_edge_positive() -> None:
    snapshot = {
        "venue_a": "binance_perp",
        "venue_b": "hyperliquid_perp",
        "symbol_a": "CRYPTO:BTCUSDT.PERP.BINANCE",
        "symbol_b": "CRYPTO:BTCUSDT.PERP.HYPER",
        "pair_symbol": "BTC-PERP-SPREAD",
        "price_a": 101.2,
        "price_b": 100.0,
        "spread_history_bps": [1.0, 6.0, 12.0, 18.0],
        "funding_a_bps": 1.0,
        "funding_b_bps": 3.0,
        "basis_bps": 0.5,
        "fee_bps": 0.8,
        "slippage_bps": 0.6,
        "borrow_bps": 0.0,
        "liquidity_score": 0.9,
        "liquidation_distance_pct": 0.25,
        "net_notional_usd": 0.5,
        "target_notional_usd": 1500.0,
    }
    params = {
        "ewma_alpha": 0.2,
        "z_entry": 1.5,
        "z_exit": 0.5,
        "z_signal_scale_bps": 2.5,
        "entry_min_edge_bps": 0.5,
        "epsilon_notional_usd": 5.0,
        "min_liquidity_score": 0.35,
        "min_liquidation_distance_pct": 0.15,
        "score_per_bps": 3.0,
    }

    out = evaluate_perp_perp_edge(snapshot=snapshot, params=params)

    assert out["strategy_type"] == "perp_perp"
    assert out["eligible"] is True
    assert out["expected_net_edge_bps"] > 0
    assert out["distance_to_entry_bps"] == 0.0
    assert out["edge_score"] > 50.0


def test_evaluate_perp_perp_edge_blocks_when_delta_neutrality_broken() -> None:
    snapshot = {
        "price_a": 100.5,
        "price_b": 100.0,
        "spread_history_bps": [1.0, 2.0, 3.0, 4.0],
        "funding_a_bps": 0.0,
        "funding_b_bps": 0.0,
        "basis_bps": 0.0,
        "fee_bps": 0.1,
        "slippage_bps": 0.1,
        "borrow_bps": 0.0,
        "liquidity_score": 0.9,
        "liquidation_distance_pct": 0.25,
        "net_notional_usd": 25.0,
    }
    params = {
        "z_entry": 0.2,
        "z_exit": 0.1,
        "z_signal_scale_bps": 2.0,
        "entry_min_edge_bps": 0.0,
        "epsilon_notional_usd": 5.0,
    }

    out = evaluate_perp_perp_edge(snapshot=snapshot, params=params)

    assert out["eligible"] is False
    assert out["entry_block_reason"] == "delta_neutrality_breach"


def test_evaluate_cash_carry_edge_cost_boundary_turns_negative() -> None:
    snapshot = {
        "symbol_spot": "CRYPTO:ETHUSDT.SPOT.BINANCE",
        "symbol_perp": "CRYPTO:ETHUSDT.PERP.HYPER",
        "spot_price": 100.0,
        "perp_price": 100.2,
        "basis_history_bps": [5.0, 8.0, 10.0, 12.0],
        "funding_short_bps": 0.2,
        "funding_long_bps": 0.1,
        "fee_bps": 12.0,
        "slippage_bps": 12.0,
        "borrow_bps": 2.0,
        "liquidity_score": 0.9,
        "liquidation_distance_pct": 0.25,
        "net_notional_usd": 0.0,
    }
    params = {
        "z_entry": 0.5,
        "z_exit": 0.1,
        "z_signal_scale_bps": 1.5,
        "basis_entry_bps": 2.0,
        "basis_exit_bps": 1.0,
        "entry_min_edge_bps": 0.0,
        "epsilon_notional_usd": 5.0,
    }

    out = evaluate_cash_carry_edge(snapshot=snapshot, params=params)

    assert out["strategy_type"] == "cash_carry"
    assert out["expected_net_edge_bps"] <= 0.0
    assert out["eligible"] is False
    assert out["entry_block_reason"] == "expected_net_edge_non_positive"
