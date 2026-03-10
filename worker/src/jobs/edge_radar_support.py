from __future__ import annotations

from datetime import datetime
from typing import Any

from src.analytics.strategies import evaluate_cash_carry_edge, evaluate_perp_perp_edge
from src.types import EdgeRisk, EdgeState, OrderIntent, PortfolioSpec


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _normalize_scope(scope: str) -> str:
    normalized = str(scope or "").strip().lower()
    if normalized in {"equities", "equity", "eq", "stock", "stocks"}:
        return "equities"
    if normalized in {"crypto", "crypt", "digital"}:
        return "crypto"
    return "all"


def _resolve_edge_radar_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("edge_radar", {})
    if not isinstance(root, dict):
        root = {}
    equities = root.get("equities", {}) if isinstance(root.get("equities", {}), dict) else {}
    crypto = root.get("crypto", {}) if isinstance(root.get("crypto", {}), dict) else {}
    notify = root.get("notify", {}) if isinstance(root.get("notify", {}), dict) else {}
    formula = root.get("formula", {}) if isinstance(root.get("formula", {}), dict) else {}
    conf_multiplier = formula.get("confidence_multiplier", {}) if isinstance(formula.get("confidence_multiplier", {}), dict) else {}
    crypto_status_bonus = formula.get("crypto_status_bonus_bps", {}) if isinstance(formula.get("crypto_status_bonus_bps", {}), dict) else {}
    equities_cost = equities.get("expected_cost_bps", {}) if isinstance(equities.get("expected_cost_bps", {}), dict) else {}
    strategy_defaults = crypto.get("strategy_defaults", {}) if isinstance(crypto.get("strategy_defaults", {}), dict) else {}
    perp_perp_cfg = strategy_defaults.get("perp_perp", {}) if isinstance(strategy_defaults.get("perp_perp", {}), dict) else {}
    cash_carry_cfg = strategy_defaults.get("cash_carry", {}) if isinstance(strategy_defaults.get("cash_carry", {}), dict) else {}
    intent_cfg = crypto.get("intent_generation", {}) if isinstance(crypto.get("intent_generation", {}), dict) else {}
    broker_map_cfg = intent_cfg.get("broker_map", {}) if isinstance(intent_cfg.get("broker_map", {}), dict) else {}
    return {
        "enabled": bool(root.get("enabled", True)),
        "notify_top_n": max(1, _to_int(notify.get("top_n"), 10)),
        "equities": {
            "run_hour_jst": _clamp(float(_to_int(equities.get("run_hour_jst"), 20)), 0.0, 23.0),
            "run_minute_jst": _clamp(float(_to_int(equities.get("run_minute_jst"), 10)), 0.0, 59.0),
            "max_candidates": max(1, _to_int(equities.get("max_candidates"), 50)),
            "entry_threshold_bps": _to_float(equities.get("entry_threshold_bps"), 2.0),
            "expected_cost_bps": {"JP_EQ": _to_float(equities_cost.get("JP_EQ"), 8.0), "US_EQ": _to_float(equities_cost.get("US_EQ"), 10.0)},
        },
        "crypto": {
            "run_minute_jst": _clamp(float(_to_int(crypto.get("run_minute_jst"), 5)), 0.0, 59.0),
            "max_candidates": max(1, _to_int(crypto.get("max_candidates"), 20)),
            "entry_threshold_bps": _to_float(crypto.get("entry_threshold_bps"), 1.5),
            "expected_cost_bps": _to_float(crypto.get("expected_cost_bps"), 12.0),
            "strategy_defaults": {
                "perp_perp": {key: (_to_int(perp_perp_cfg.get(key), 30) if key == "timeout_sec" else _to_float(perp_perp_cfg.get(key), default)) for key, default in {"ewma_alpha": 0.2, "z_entry": 2.0, "z_exit": 0.6, "z_signal_scale_bps": 2.5, "entry_min_edge_bps": 0.8, "fee_bps": 4.0, "slippage_bps": 3.0, "borrow_bps": 0.0, "min_liquidity_score": 0.35, "min_liquidation_distance_pct": 0.15, "epsilon_notional_usd": 10.0, "target_notional_usd": 1000.0, "timeout_sec": 30, "score_per_bps": 3.0}.items()},
                "cash_carry": {key: (_to_int(cash_carry_cfg.get(key), 30) if key == "timeout_sec" else _to_float(cash_carry_cfg.get(key), default)) for key, default in {"ewma_alpha": 0.2, "z_entry": 1.8, "z_exit": 0.5, "z_signal_scale_bps": 2.0, "basis_entry_bps": 8.0, "basis_exit_bps": 3.0, "entry_min_edge_bps": 0.8, "fee_bps": 4.0, "slippage_bps": 3.0, "borrow_bps": 0.0, "min_liquidity_score": 0.35, "min_liquidation_distance_pct": 0.15, "epsilon_notional_usd": 10.0, "target_notional_usd": 1000.0, "timeout_sec": 30, "score_per_bps": 3.0}.items()},
            },
            "intent_generation": {
                "enabled": bool(intent_cfg.get("enabled", True)),
                "status": str(intent_cfg.get("status", "proposed")),
                "portfolio_name": str(intent_cfg.get("portfolio_name", "crypto_arb")),
                "base_currency": str(intent_cfg.get("base_currency", "USD")),
                "max_new_intents_per_run": max(1, _to_int(intent_cfg.get("max_new_intents_per_run"), 5)),
                "lookback_minutes": max(1, _to_int(intent_cfg.get("lookback_minutes"), 180)),
                "min_edge_score": _to_float(intent_cfg.get("min_edge_score"), 55.0),
                "min_confidence": _to_float(intent_cfg.get("min_confidence"), 0.55),
                "require_positive_expected_edge": bool(intent_cfg.get("require_positive_expected_edge", True)),
                "broker_map": {"CRYPTO": str(broker_map_cfg.get("CRYPTO", "crypto_gateway"))},
            },
        },
        "formula": {
            "combined_weight": _to_float(formula.get("combined_weight"), 0.55),
            "legacy_edge_weight": _to_float(formula.get("legacy_edge_weight"), 0.45),
            "signal_scale_bps": _to_float(formula.get("signal_scale_bps"), 0.30),
            "confidence_multiplier": {"High": _to_float(conf_multiplier.get("High"), 1.0), "Medium": _to_float(conf_multiplier.get("Medium"), 0.75), "Low": _to_float(conf_multiplier.get("Low"), 0.5)},
            "source_boost_bps_per_primary": _to_float(formula.get("source_boost_bps_per_primary"), 0.8),
            "max_source_boost_bps": _to_float(formula.get("max_source_boost_bps"), 4.0),
            "contradiction_penalty_bps": _to_float(formula.get("contradiction_penalty_bps"), 5.0),
            "missing_penalty_bps_per_10pct": _to_float(formula.get("missing_penalty_bps_per_10pct"), 1.0),
            "score_per_bps": _to_float(formula.get("score_per_bps"), 3.0),
            "crypto_sharpe_bps_factor": _to_float(formula.get("crypto_sharpe_bps_factor"), 10.0),
            "crypto_cagr_bps_factor": _to_float(formula.get("crypto_cagr_bps_factor"), 80.0),
            "crypto_drawdown_penalty_bps_factor": _to_float(formula.get("crypto_drawdown_penalty_bps_factor"), 25.0),
            "crypto_status_bonus_bps": {"candidate": _to_float(crypto_status_bonus.get("candidate"), 0.0), "approved": _to_float(crypto_status_bonus.get("approved"), 0.5), "paper": _to_float(crypto_status_bonus.get("paper"), 1.0), "live": _to_float(crypto_status_bonus.get("live"), 2.0), "paused": _to_float(crypto_status_bonus.get("paused"), -1.0)},
        },
    }


def _market_scope_from_security_market(market: Any) -> str:
    market_text = str(market or "").strip().upper()
    return "JP_EQ" if market_text == "JP" else "US_EQ" if market_text == "US" else "MIXED"


def _confidence_value(label: str, missing_ratio: float) -> float:
    base = {"High": 0.90, "Medium": 0.65, "Low": 0.40}.get(label, 0.40)
    return round(_clamp(base - (max(0.0, min(1.0, missing_ratio)) * 0.25), 0.0, 1.0), 4)


def _build_equity_edge_state(row: dict[str, Any], observed_at: datetime, resolved_cfg: dict[str, Any]) -> EdgeState:
    formula_cfg = resolved_cfg["formula"]
    equities_cfg = resolved_cfg["equities"]
    security_id = str(row.get("security_id", "UNKNOWN")).strip() or "UNKNOWN"
    market_scope = _market_scope_from_security_market(row.get("market"))
    confidence_label = str(row.get("confidence", "Low"))
    missing_ratio = _clamp(_to_float(row.get("missing_ratio"), 1.0), 0.0, 1.0)
    primary_sources = max(0, _to_int(row.get("primary_source_count"), 0))
    contradiction = bool(row.get("has_major_contradiction") or False)
    combined_score = _to_optional_float(row.get("combined_score"))
    legacy_edge_score = _to_optional_float(row.get("edge_score"))
    expected_net_edge_bps = None
    distance_to_entry_bps = None
    edge_score_value = 0.0
    if combined_score is not None and legacy_edge_score is not None:
        confidence_multiplier = dict(formula_cfg["confidence_multiplier"]).get(confidence_label, 0.5)
        source_boost = min(float(primary_sources) * float(formula_cfg["source_boost_bps_per_primary"]), float(formula_cfg["max_source_boost_bps"]))
        contradiction_penalty = float(formula_cfg["contradiction_penalty_bps"]) if contradiction else 0.0
        missing_penalty = (missing_ratio * 10.0) * float(formula_cfg["missing_penalty_bps_per_10pct"])
        raw_signal = ((combined_score * float(formula_cfg["combined_weight"])) + (legacy_edge_score * float(formula_cfg["legacy_edge_weight"]))) * float(formula_cfg["signal_scale_bps"])
        expected_net_edge_bps = round((raw_signal * confidence_multiplier) + source_boost - contradiction_penalty - missing_penalty - float(dict(equities_cfg["expected_cost_bps"]).get(market_scope, 8.0)), 6)
        distance_to_entry_bps = round(max(0.0, float(equities_cfg["entry_threshold_bps"]) - expected_net_edge_bps), 6)
        edge_score_value = _clamp(50.0 + (expected_net_edge_bps * float(formula_cfg["score_per_bps"])), 0.0, 100.0)
    explain = f"{security_id} edge_est={expected_net_edge_bps:.2f}bps" if expected_net_edge_bps is not None else f"{security_id} edge_est=N/A (insufficient inputs)"
    risk_payload = EdgeRisk(missing_ratio=missing_ratio, primary_source_count=primary_sources, has_major_contradiction=contradiction, extra={"market": str(row.get("market", "")), "confidence_label": confidence_label})
    return EdgeState(strategy_name="edge-radar-equities", strategy_version_id=None, market_scope=market_scope, symbol=security_id, observed_at=observed_at, edge_score=round(edge_score_value, 4), expected_net_edge_bps=expected_net_edge_bps, distance_to_entry_bps=distance_to_entry_bps, confidence=_confidence_value(confidence_label, missing_ratio), risk_json=risk_payload, risk=risk_payload, explain=explain, meta={"confidence_label": confidence_label, "combined_score": combined_score, "legacy_edge_score": legacy_edge_score})


def _resolve_crypto_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    artifacts = row.get("artifacts") if isinstance(row.get("artifacts"), dict) else {}
    strategy_spec = row.get("strategy_spec") if isinstance(row.get("strategy_spec"), dict) else {}
    crypto_spec = strategy_spec.get("crypto_arb") if isinstance(strategy_spec.get("crypto_arb"), dict) else {}
    snapshot = artifacts.get("market_snapshot") if isinstance(artifacts.get("market_snapshot"), dict) else crypto_spec.get("snapshot")
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    if "target_notional_usd" not in snapshot and "target_notional_usd" in crypto_spec:
        snapshot["target_notional_usd"] = crypto_spec.get("target_notional_usd")
    if "net_notional_usd" not in snapshot and "net_notional_usd" in artifacts:
        snapshot["net_notional_usd"] = artifacts.get("net_notional_usd")
    return snapshot


def _infer_crypto_strategy_type(row: dict[str, Any], snapshot: dict[str, Any]) -> str:
    strategy_spec = row.get("strategy_spec")
    if isinstance(strategy_spec, dict):
        crypto_spec = strategy_spec.get("crypto_arb")
        if isinstance(crypto_spec, dict):
            strategy_type = str(crypto_spec.get("type", "")).strip().lower()
            if strategy_type in {"perp_perp", "cash_carry"}:
                return strategy_type
    snapshot_type = str(snapshot.get("strategy_type", "")).strip().lower()
    if snapshot_type in {"perp_perp", "cash_carry"}:
        return snapshot_type
    name = str(row.get("strategy_name", "")).strip().lower()
    return "cash_carry" if ("cash" in name or "carry" in name) else "perp_perp"


def _resolve_crypto_params(resolved_cfg: dict[str, Any], row: dict[str, Any], strategy_type: str) -> dict[str, Any]:
    cfg_defaults = dict(resolved_cfg["crypto"]["strategy_defaults"].get(strategy_type, {}))
    strategy_spec = row.get("strategy_spec")
    if isinstance(strategy_spec, dict):
        crypto_spec = strategy_spec.get("crypto_arb")
        if isinstance(crypto_spec, dict) and isinstance(crypto_spec.get("params"), dict):
            cfg_defaults.update(crypto_spec["params"])
    return cfg_defaults


def _build_crypto_edge_state(row: dict[str, Any], observed_at: datetime, resolved_cfg: dict[str, Any]) -> EdgeState:
    strategy_name = str(row.get("strategy_name", "unknown-crypto-strategy")).strip() or "unknown-crypto-strategy"
    strategy_status = str(row.get("status", "candidate")).strip().lower() or "candidate"
    strategy_version_id = str(row.get("strategy_version_id")) if row.get("strategy_version_id") is not None else None
    eval_type = str(row.get("eval_type", "")).strip().lower()
    snapshot = _resolve_crypto_snapshot(row)
    strategy_type = _infer_crypto_strategy_type(row, snapshot)
    params = _resolve_crypto_params(resolved_cfg, row, strategy_type)
    if snapshot:
        decision = evaluate_cash_carry_edge(snapshot=snapshot, params=params) if strategy_type == "cash_carry" else evaluate_perp_perp_edge(snapshot=snapshot, params=params)
    else:
        formula_cfg = resolved_cfg["formula"]
        crypto_cfg = resolved_cfg["crypto"]
        sharpe = _to_optional_float(row.get("sharpe"))
        cagr = _to_optional_float(row.get("cagr"))
        max_dd = _to_optional_float(row.get("max_dd"))
        expected_net_edge_bps = None
        distance_to_entry_bps = None
        edge_score_value = 0.0
        if sharpe is not None and cagr is not None:
            gross_edge_bps = max(0.0, sharpe) * float(formula_cfg["crypto_sharpe_bps_factor"]) + max(0.0, cagr) * float(formula_cfg["crypto_cagr_bps_factor"]) + float(dict(formula_cfg["crypto_status_bonus_bps"]).get(strategy_status, 0.0)) - abs(min(max_dd or 0.0, 0.0)) * float(formula_cfg["crypto_drawdown_penalty_bps_factor"])
            expected_net_edge_bps = round(gross_edge_bps - float(crypto_cfg["expected_cost_bps"]), 6)
            distance_to_entry_bps = round(max(0.0, float(crypto_cfg["entry_threshold_bps"]) - expected_net_edge_bps), 6)
            edge_score_value = _clamp(50.0 + (expected_net_edge_bps * float(formula_cfg["score_per_bps"])), 0.0, 100.0)
        decision = {"strategy_type": strategy_type, "symbol": strategy_name, "eligible": False, "entry_block_reason": "missing_market_snapshot", "edge_score": edge_score_value, "expected_net_edge_bps": expected_net_edge_bps, "distance_to_entry_bps": distance_to_entry_bps, "confidence": 0.35, "risk": {"status": strategy_status, "eval_type": eval_type, "sharpe": sharpe, "max_dd": max_dd, "cagr": cagr}, "order_template": {}, "exit_rules": {}}
    expected_net_edge_bps = _to_optional_float(decision.get("expected_net_edge_bps"))
    explain = f"{strategy_name} {strategy_type} edge_est={expected_net_edge_bps:.2f}bps" if expected_net_edge_bps is not None else f"{strategy_name} {strategy_type} edge_est=N/A"
    if str(decision.get("entry_block_reason") or ""):
        explain = f"{explain} block={decision.get('entry_block_reason')}"
    decision_risk = decision.get("risk") if isinstance(decision.get("risk"), dict) else {}
    risk_payload = EdgeRisk.from_mapping({"liquidity_score": decision_risk.get("liquidity_score"), "min_liquidity_score": decision_risk.get("min_liquidity_score"), "liquidation_distance_pct": decision_risk.get("liquidation_distance_pct"), "min_liquidation_distance_pct": decision_risk.get("min_liquidation_distance_pct"), "neutral_ok": decision_risk.get("neutral_ok"), "neutral_reason": decision_risk.get("neutral_reason"), "status": strategy_status, "eval_type": eval_type, "sharpe": decision_risk.get("sharpe"), "max_dd": decision_risk.get("max_dd"), "cagr": decision_risk.get("cagr"), "entry_block_reason": decision.get("entry_block_reason"), "extra": {key: item for key, item in decision_risk.items() if key not in {"liquidity_score", "min_liquidity_score", "liquidation_distance_pct", "min_liquidation_distance_pct", "neutral_ok", "neutral_reason", "status", "eval_type", "sharpe", "max_dd", "cagr", "entry_block_reason"}}})
    return EdgeState(strategy_name=strategy_name, strategy_version_id=strategy_version_id, market_scope="CRYPTO", symbol=str(decision.get("symbol") or strategy_name), observed_at=observed_at, edge_score=round(_to_float(decision.get("edge_score"), 0.0), 4), expected_net_edge_bps=expected_net_edge_bps, distance_to_entry_bps=_to_optional_float(decision.get("distance_to_entry_bps")), confidence=round(_to_float(decision.get("confidence"), 0.35), 4), risk_json=risk_payload, risk=risk_payload, explain=explain, meta={"strategy_id": row.get("strategy_id"), "version": row.get("version"), "strategy_type": strategy_type, "eligible": bool(decision.get("eligible", False)), "entry_block_reason": decision.get("entry_block_reason"), "order_template": decision.get("order_template") if isinstance(decision.get("order_template"), dict) else {}, "exit_rules": decision.get("exit_rules") if isinstance(decision.get("exit_rules"), dict) else {}})


def _to_discord_payload(states: list[EdgeState]) -> list[dict[str, Any]]:
    return [{"symbol": state.symbol, "strategy_name": state.strategy_name, "edge_score": float(state.edge_score), "expected_net_edge_bps": state.expected_net_edge_bps, "distance_to_entry_bps": state.distance_to_entry_bps, "confidence": state.confidence, "explain": state.explain} for state in states]


def _build_intent_positions_from_state(state: EdgeState) -> tuple[list[dict[str, Any]] | None, str | None]:
    if not isinstance(state.meta, dict):
        return None, "missing_meta"
    order_template = state.meta.get("order_template")
    if not isinstance(order_template, dict):
        return None, "missing_order_template"
    symbol_long = str(order_template.get("symbol_long", "")).strip()
    symbol_short = str(order_template.get("symbol_short", "")).strip()
    price_long = _to_float(order_template.get("price_long"), 0.0)
    price_short = _to_float(order_template.get("price_short"), 0.0)
    target_notional_usd = _to_float(order_template.get("target_notional_usd"), 0.0)
    if not symbol_long or not symbol_short:
        return None, "missing_leg_symbol"
    if price_long <= 0 or price_short <= 0:
        return None, "missing_leg_price"
    if target_notional_usd <= 0:
        return None, "missing_target_notional"
    instrument_type = str(order_template.get("instrument_type", "CRYPTO"))
    timeout_sec = int(max(1, _to_int(order_template.get("timeout_sec"), 30)))
    return [
        {"symbol": symbol_long, "target_qty": target_notional_usd / price_long, "delta_qty": target_notional_usd / price_long, "instrument_type": instrument_type, "price_hint": price_long, "venue": order_template.get("venue_long"), "timeout_sec": timeout_sec},
        {"symbol": symbol_short, "target_qty": -(target_notional_usd / price_short), "delta_qty": -(target_notional_usd / price_short), "instrument_type": instrument_type, "price_hint": price_short, "venue": order_template.get("venue_short"), "timeout_sec": timeout_sec},
    ], None


def _create_crypto_order_intents(repo, states: list[EdgeState], observed_at: datetime, resolved_cfg: dict[str, Any]) -> int:
    intent_cfg = dict(resolved_cfg["crypto"]["intent_generation"])
    if not bool(intent_cfg.get("enabled", True)):
        return 0
    portfolio_id = repo.upsert_portfolio(PortfolioSpec(name=str(intent_cfg.get("portfolio_name", "crypto_arb")), base_currency=str(intent_cfg.get("base_currency", "USD")), broker_map=dict(intent_cfg.get("broker_map") or {"CRYPTO": "crypto_gateway"})))
    created = 0
    for state in sorted(states, key=lambda item: float(item.edge_score), reverse=True):
        if created >= _to_int(intent_cfg.get("max_new_intents_per_run"), 5):
            break
        if not state.strategy_version_id or float(state.edge_score) < _to_float(intent_cfg.get("min_edge_score"), 55.0) or _to_float(state.confidence, 0.0) < _to_float(intent_cfg.get("min_confidence"), 0.55):
            continue
        if bool(intent_cfg.get("require_positive_expected_edge", True)) and _to_float(state.expected_net_edge_bps, -999.0) <= 0.0:
            continue
        meta = state.meta if isinstance(state.meta, dict) else {}
        if not bool(meta.get("eligible", False)) or repo.has_recent_open_intent_for_strategy(strategy_version_id=state.strategy_version_id, lookback_minutes=_to_int(intent_cfg.get("lookback_minutes"), 180)):
            continue
        target_positions, reason = _build_intent_positions_from_state(state)
        if target_positions is None:
            continue
        repo.insert_order_intent(OrderIntent(portfolio_id=portfolio_id, strategy_version_id=state.strategy_version_id, as_of=observed_at, target_positions=target_positions, status=str(intent_cfg.get("status", "proposed")), reason=f"edge_radar crypto signal: {state.strategy_name}", risk_checks={"expected_net_edge_bps": state.expected_net_edge_bps, "distance_to_entry_bps": state.distance_to_entry_bps, "edge_score": float(state.edge_score), "confidence": state.confidence, "entry_block_reason": meta.get("entry_block_reason"), "intent_build_warning": reason, "strategy_type": meta.get("strategy_type")}))
        created += 1
    return created


__all__ = [
    "_build_crypto_edge_state",
    "_build_equity_edge_state",
    "_build_intent_positions_from_state",
    "_clamp",
    "_confidence_value",
    "_create_crypto_order_intents",
    "_infer_crypto_strategy_type",
    "_market_scope_from_security_market",
    "_normalize_scope",
    "_resolve_crypto_params",
    "_resolve_crypto_snapshot",
    "_resolve_edge_radar_cfg",
    "_to_discord_payload",
    "_to_float",
    "_to_int",
    "_to_optional_float",
]
