from __future__ import annotations

from typing import Any

from src.analytics.edge import (
    check_dollar_neutrality,
    compute_distance_to_entry_bps,
    compute_edge_score,
    compute_ewma_zscore,
    compute_expected_net_edge_bps,
)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_history(snapshot: dict[str, Any], current_basis_bps: float) -> list[float]:
    raw = snapshot.get("basis_history_bps")
    if not isinstance(raw, list):
        return [current_basis_bps]
    values: list[float] = []
    for v in raw:
        try:
            values.append(float(v))
        except (TypeError, ValueError):
            continue
    if not values or values[-1] != current_basis_bps:
        values.append(current_basis_bps)
    return values


def evaluate_cash_carry_edge(
    snapshot: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    venue_spot = str(snapshot.get("venue_spot", "binance_spot"))
    venue_perp = str(snapshot.get("venue_perp", "hyperliquid_perp"))
    symbol_spot = str(snapshot.get("symbol_spot", "CRYPTO:UNKNOWN_SPOT"))
    symbol_perp = str(snapshot.get("symbol_perp", "CRYPTO:UNKNOWN_PERP"))
    pair_symbol = str(snapshot.get("pair_symbol", f"{symbol_spot}|{symbol_perp}"))

    spot_price = _to_float(snapshot.get("spot_price"), 0.0)
    perp_price = _to_float(snapshot.get("perp_price"), 0.0)
    basis_bps = _to_float(snapshot.get("basis_bps"), 0.0)
    if basis_bps == 0.0 and spot_price > 0 and perp_price > 0:
        basis_bps = ((perp_price / spot_price) - 1.0) * 10_000.0

    history = _extract_history(snapshot, basis_bps)
    zscore = compute_ewma_zscore(history, alpha=_to_float(params.get("ewma_alpha"), 0.2))

    z_entry = _to_float(params.get("z_entry"), 1.8)
    z_exit = _to_float(params.get("z_exit"), 0.5)
    z_signal_scale_bps = _to_float(params.get("z_signal_scale_bps"), 2.0)
    expected_reversion_bps = 0.0
    if zscore is not None:
        expected_reversion_bps = max(0.0, abs(zscore) - z_exit) * z_signal_scale_bps

    basis_entry_bps = _to_float(params.get("basis_entry_bps"), 8.0)
    basis_exit_bps = _to_float(params.get("basis_exit_bps"), 3.0)
    basis_capture_bps = max(0.0, abs(basis_bps) - basis_exit_bps)
    funding_short_bps = _to_float(snapshot.get("funding_short_bps"), 0.0)
    funding_long_bps = _to_float(snapshot.get("funding_long_bps"), 0.0)
    funding_net_bps = funding_short_bps - funding_long_bps
    fee_bps = _to_float(snapshot.get("fee_bps"), _to_float(params.get("fee_bps"), 0.0))
    slippage_bps = _to_float(snapshot.get("slippage_bps"), _to_float(params.get("slippage_bps"), 0.0))
    borrow_bps = _to_float(snapshot.get("borrow_bps"), _to_float(params.get("borrow_bps"), 0.0))

    expected_net_edge_bps = compute_expected_net_edge_bps(
        spread_bps=basis_capture_bps + expected_reversion_bps,
        funding_bps=funding_net_bps,
        basis_bps=0.0,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        borrow_bps=borrow_bps,
    )

    entry_min_edge_bps = _to_float(params.get("entry_min_edge_bps"), 0.0)
    distance_to_entry_bps = compute_distance_to_entry_bps(expected_net_edge_bps, entry_min_edge_bps)
    edge_score = compute_edge_score(
        expected_net_edge_bps=expected_net_edge_bps,
        score_per_bps=_to_float(params.get("score_per_bps"), 3.0),
    )

    net_notional_usd = snapshot.get("net_notional_usd")
    epsilon_notional_usd = _to_float(params.get("epsilon_notional_usd"), 10.0)
    neutral_ok, neutral_reason = check_dollar_neutrality(net_notional_usd, epsilon_notional_usd)
    liquidity_score = _to_float(snapshot.get("liquidity_score"), 0.0)
    min_liquidity_score = _to_float(params.get("min_liquidity_score"), 0.35)
    liquidation_distance_pct = _to_float(snapshot.get("liquidation_distance_pct"), 0.0)
    min_liquidation_distance_pct = _to_float(params.get("min_liquidation_distance_pct"), 0.15)

    entry_block_reason: str | None = None
    if abs(basis_bps) < basis_entry_bps:
        entry_block_reason = "basis_below_entry"
    elif zscore is None:
        entry_block_reason = "insufficient_zscore_data"
    elif abs(zscore) < z_entry:
        entry_block_reason = "zscore_below_entry"
    elif expected_net_edge_bps <= 0:
        entry_block_reason = "expected_net_edge_non_positive"
    elif liquidity_score < min_liquidity_score:
        entry_block_reason = "liquidity_insufficient"
    elif liquidation_distance_pct < min_liquidation_distance_pct:
        entry_block_reason = "liquidation_distance_too_small"
    elif not neutral_ok:
        entry_block_reason = neutral_reason

    target_notional_usd = _to_float(snapshot.get("target_notional_usd"), _to_float(params.get("target_notional_usd"), 1_000.0))
    timeout_sec = int(max(1, _to_float(params.get("timeout_sec"), 30)))
    confidence = 0.35
    if zscore is not None:
        confidence = min(0.95, 0.55 + (abs(zscore) * 0.08))
        if entry_block_reason is not None:
            confidence *= 0.75

    return {
        "strategy_type": "cash_carry",
        "pair_symbol": pair_symbol,
        "symbol": pair_symbol,
        "eligible": entry_block_reason is None,
        "entry_block_reason": entry_block_reason,
        "zscore": zscore,
        "z_entry": z_entry,
        "z_exit": z_exit,
        "basis_bps": basis_bps,
        "basis_capture_bps": basis_capture_bps,
        "expected_reversion_bps": expected_reversion_bps,
        "funding_net_bps": funding_net_bps,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "borrow_bps": borrow_bps,
        "expected_net_edge_bps": expected_net_edge_bps,
        "distance_to_entry_bps": distance_to_entry_bps,
        "edge_score": edge_score,
        "confidence": max(0.0, min(1.0, confidence)),
        "timeout_sec": timeout_sec,
        "exit_rules": {
            "basis_exit_bps": basis_exit_bps,
            "z_exit": z_exit,
            "timeout_sec": timeout_sec,
            "force_flat_on_partial_fill": True,
        },
        "order_template": {
            "symbol_long": symbol_spot,
            "symbol_short": symbol_perp,
            "price_long": spot_price,
            "price_short": perp_price,
            "venue_long": venue_spot,
            "venue_short": venue_perp,
            "target_notional_usd": target_notional_usd,
            "net_notional_usd": _to_float(net_notional_usd, 0.0),
            "epsilon_notional_usd": epsilon_notional_usd,
            "instrument_type": "CRYPTO",
            "timeout_sec": timeout_sec,
        },
        "risk": {
            "liquidity_score": liquidity_score,
            "min_liquidity_score": min_liquidity_score,
            "liquidation_distance_pct": liquidation_distance_pct,
            "min_liquidation_distance_pct": min_liquidation_distance_pct,
            "neutral_ok": neutral_ok,
            "neutral_reason": neutral_reason,
        },
    }
