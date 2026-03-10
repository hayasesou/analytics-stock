from __future__ import annotations

from datetime import datetime
from typing import Any

from src.execution.reconcile import ReconcileSettings, reconcile_target_positions
from src.execution.risk import StrategyRiskThresholds, evaluate_strategy_risk_gate
from src.jobs.executor_support import *
from src.types import StrategyRiskEvent, StrategyRiskSnapshot


def _handle_portfolio_risk_gate(
    *,
    repo,
    notifier,
    now: datetime,
    intent_id: str,
    portfolio_id: str,
    strategy_version_id: str | None,
    risk_checks: dict[str, Any],
    thresholds,
    sharpe_window_days: int,
    stats: dict[str, int],
) -> bool:
    risk_state, risk_triggers = _create_risk_snapshot_if_existing(
        repo=repo,
        portfolio_id=portfolio_id,
        thresholds=thresholds,
        now=now,
        fallback_risk_checks=risk_checks,
        sharpe_window_days=sharpe_window_days,
    )
    if risk_state == "normal":
        return False
    detail = (
        f"state={risk_state} drawdown={risk_triggers.get('drawdown')} "
        f"sharpe_20d={risk_triggers.get('sharpe_20d')}"
    )
    _notify_risk_bulletin(
        notifier=notifier,
        now=now,
        category="dd_sharpe_gate",
        title="portfolio risk gate reject",
        intent_id=intent_id,
        strategy_version_id=strategy_version_id,
        detail=detail,
    )
    repo.update_order_intent_status(intent_id, "rejected")
    stats["rejected"] += 1
    return True


def _handle_strategy_risk_gate(
    *,
    repo,
    notifier,
    now: datetime,
    intent: dict[str, Any],
    intent_id: str,
    portfolio_id: str,
    strategy_version_id: str | None,
    risk_checks: dict[str, Any],
    broker_map: dict[str, Any],
    strategy_risk_cfg: dict[str, Any],
    jp_gateway_cfg: dict[str, Any],
    us_gateway_cfg: dict[str, Any],
    crypto_gateway_client,
    jp_gateway_client,
    us_gateway_client,
    stats: dict[str, int],
) -> bool:
    if not (bool(strategy_risk_cfg.get("enabled")) and strategy_version_id):
        return False
    strategy_drawdown, strategy_sharpe = _extract_strategy_risk_values(risk_checks)
    latest_strategy_snapshot = repo.fetch_latest_strategy_risk_snapshot(strategy_version_id)
    existing_cooldown_until = _to_utc_datetime((latest_strategy_snapshot or {}).get("cooldown_until"))
    recent_strategy_snapshots = repo.fetch_recent_strategy_risk_snapshots(
        strategy_version_id=strategy_version_id,
        limit=max(
            10,
            int(strategy_risk_cfg.get("warning_consecutive_days", 3))
            + int(strategy_risk_cfg.get("halt_consecutive_days", 2))
            + 10,
        ),
    )
    strategy_thresholds = StrategyRiskThresholds(
        max_drawdown_breach=_to_float(strategy_risk_cfg.get("max_drawdown_breach"), -0.03),
        warning_sharpe_threshold=_to_float(strategy_risk_cfg.get("warning_sharpe_threshold"), 0.30),
        warning_consecutive_days=int(strategy_risk_cfg.get("warning_consecutive_days", 3)),
        halt_sharpe_threshold=_to_float(strategy_risk_cfg.get("halt_sharpe_threshold"), 0.0),
        halt_consecutive_days=int(strategy_risk_cfg.get("halt_consecutive_days", 2)),
        cooldown_hours=int(strategy_risk_cfg.get("cooldown_hours", 24)),
    )
    strategy_eval_date = _resolve_strategy_eval_date(
        now=now,
        risk_checks=risk_checks,
        recent_strategy_snapshots=recent_strategy_snapshots,
    )
    history_for_eval = [
        {
            "as_of": now,
            "as_of_date": strategy_eval_date,
            "sharpe_20d": strategy_sharpe,
        },
        *recent_strategy_snapshots,
    ]
    strategy_state, strategy_triggers, cooldown_until = evaluate_strategy_risk_gate(
        now=now,
        drawdown=strategy_drawdown,
        sharpe_20d=strategy_sharpe,
        history_desc=history_for_eval,
        thresholds=strategy_thresholds,
        existing_cooldown_until=existing_cooldown_until,
    )
    repo.upsert_strategy_risk_snapshot(
        StrategyRiskSnapshot(
            strategy_version_id=strategy_version_id,
            as_of=now,
            drawdown=strategy_drawdown,
            sharpe_20d=strategy_sharpe,
            state=strategy_state,
            trigger_flags=strategy_triggers,
            cooldown_until=cooldown_until,
        )
    )

    if strategy_state == "warning":
        stats["strategy_warning"] += 1
        repo.insert_strategy_risk_event(
            StrategyRiskEvent(
                strategy_version_id=strategy_version_id,
                event_type="strategy_warning",
                payload={"intent_id": intent_id, "triggers": strategy_triggers},
                triggered_at=now,
            )
        )
        notifier.send_executor_alert(
            title="strategy risk warning",
            details={
                "intent_id": intent_id,
                "strategy_version_id": strategy_version_id,
                "warning_consecutive_count": strategy_triggers.get("warning_consecutive_count"),
                "sharpe_20d": strategy_sharpe,
            },
        )
        _notify_risk_bulletin(
            notifier=notifier,
            now=now,
            category="dd_sharpe_gate",
            title="strategy risk warning",
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            detail=(
                f"warning_count={strategy_triggers.get('warning_consecutive_count')} "
                f"sharpe_20d={strategy_sharpe}"
            ),
        )

    if strategy_state not in {"halted", "cooldown"}:
        return False

    event_type = "strategy_halt" if strategy_state == "halted" else "strategy_cooldown_reject"
    if strategy_state == "halted":
        stats["strategy_halt"] += 1
    else:
        stats["strategy_cooldown_reject"] += 1

    panic_summary = None
    should_panic_close = (
        strategy_state == "halted"
        and bool(strategy_risk_cfg.get("panic_close_on_halt", True))
        and bool(strategy_triggers.get("drawdown_breach") or strategy_triggers.get("halt_breach"))
    )
    if should_panic_close:
        panic_summary = _execute_strategy_panic_close(
            repo=repo,
            now=now,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            broker_map=broker_map,
            strategy_risk_cfg=strategy_risk_cfg,
            jp_gateway_cfg=jp_gateway_cfg,
            us_gateway_cfg=us_gateway_cfg,
            jp_gateway_client=jp_gateway_client,
            us_gateway_client=us_gateway_client,
            crypto_gateway_client=crypto_gateway_client,
        )
        stats["strategy_panic_close"] += 1

    repo.insert_strategy_risk_event(
        StrategyRiskEvent(
            strategy_version_id=strategy_version_id,
            event_type=event_type,
            payload={
                "intent_id": intent_id,
                "state": strategy_state,
                "triggers": strategy_triggers,
                "panic_close": panic_summary,
            },
            triggered_at=now,
        )
    )
    notifier.send_executor_alert(
        title="strategy risk halt" if strategy_state == "halted" else "strategy cooldown reject",
        details={
            "intent_id": intent_id,
            "strategy_version_id": strategy_version_id,
            "state": strategy_state,
            "cooldown_until": strategy_triggers.get("cooldown_until"),
            "panic_close_status": (panic_summary or {}).get("status") if isinstance(panic_summary, dict) else None,
        },
    )
    _notify_risk_bulletin(
        notifier=notifier,
        now=now,
        category="dd_sharpe_gate",
        title="strategy risk halt" if strategy_state == "halted" else "strategy cooldown reject",
        intent_id=intent_id,
        strategy_version_id=strategy_version_id,
        detail=(
            f"state={strategy_state} drawdown={strategy_triggers.get('drawdown')} "
            f"sharpe_20d={strategy_triggers.get('sharpe_20d')} "
            f"cooldown_until={strategy_triggers.get('cooldown_until')}"
        ),
    )
    repo.update_order_intent_status(intent_id, "rejected")
    stats["rejected"] += 1
    return True


def _fetch_ratings_and_reconcile(
    *,
    repo,
    notifier,
    now: datetime,
    intent: dict[str, Any],
    intent_id: str,
    risk_checks: dict[str, Any],
    portfolio_id: str,
    target_positions: list[dict[str, Any]],
    fundamental_overlay: dict[str, Any],
    reconcile_cfg: dict[str, Any],
    stats: dict[str, int],
) -> tuple[list[dict[str, Any]] | None, dict[str, str]]:
    symbols_for_rating: list[str] = []
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if symbol:
            symbols_for_rating.append(symbol)
    latest_rating_by_symbol = (
        repo.fetch_latest_fundamental_ratings_by_symbols(symbols_for_rating)
        if bool(fundamental_overlay.get("enabled")) and symbols_for_rating
        else {}
    )

    if bool(reconcile_cfg.get("enabled", True)) and target_positions:
        symbols = _collect_target_symbols(target_positions)
        current_rows = repo.fetch_positions_for_portfolio(portfolio_id=portfolio_id, symbols=symbols)
        open_rows = repo.fetch_open_orders_for_portfolio(portfolio_id=portfolio_id, symbols=symbols)
        price_by_symbol = _resolve_price_hints_for_symbols(
            repo=repo,
            target_positions=target_positions,
            symbols=symbols,
        )
        reconcile_result = reconcile_target_positions(
            target_positions=target_positions,
            current_position_qty_by_symbol=_build_position_qty_map(current_rows),
            open_orders_by_symbol=_build_open_order_map(open_rows),
            price_by_symbol=price_by_symbol,
            settings=ReconcileSettings(
                min_abs_delta_qty=_to_float(reconcile_cfg.get("min_abs_delta_qty"), 0.0),
                min_abs_delta_notional=_to_float(reconcile_cfg.get("min_abs_delta_notional"), 0.0),
                open_order_policy=str(reconcile_cfg.get("open_order_policy", "skip")),
                net_notional_epsilon=_to_float(reconcile_cfg.get("net_notional_epsilon"), 10.0),
            ),
            enforce_net_neutral=_should_enforce_neutrality(
                risk_checks=risk_checks,
                target_positions=target_positions,
                reconcile_cfg=reconcile_cfg,
            ),
        )

        stats["skipped_by_reconcile"] += len(reconcile_result.skipped)
        if reconcile_result.reject_reason:
            repo.update_order_intent_status(intent_id, "rejected")
            stats["rejected"] += 1
            strategy_version_id = intent.get("strategy_version_id")
            if strategy_version_id:
                repo.insert_strategy_risk_event(
                    StrategyRiskEvent(
                        strategy_version_id=str(strategy_version_id),
                        event_type="reconcile_rejected",
                        payload={
                            "intent_id": intent_id,
                            "reason": reconcile_result.reject_reason,
                            "net_target_notional": reconcile_result.net_target_notional,
                            "net_delta_notional": reconcile_result.net_delta_notional,
                        },
                        triggered_at=now,
                    )
                )
            notifier.send_executor_alert(
                title="reconcile rejected intent",
                details={
                    "intent_id": intent_id,
                    "reason": reconcile_result.reject_reason,
                    "net_target_notional": reconcile_result.net_target_notional,
                    "net_delta_notional": reconcile_result.net_delta_notional,
                },
            )
            return None, latest_rating_by_symbol

        target_positions = list(reconcile_result.target_positions)
        if not target_positions:
            has_open_conflict = any(
                str(row.get("reason", "")).strip().lower() == "open_order_conflict"
                for row in reconcile_result.skipped
                if isinstance(row, dict)
            )
            final_status = "sent" if has_open_conflict else "done"
            repo.update_order_intent_status(intent_id, final_status)
            if final_status == "sent":
                stats["sent"] += 1
            else:
                stats["done"] += 1
            return None, latest_rating_by_symbol

    return target_positions, latest_rating_by_symbol


def _classify_targets(
    target_positions: list[dict[str, Any]],
    broker_map: dict[str, Any],
    crypto_gateway_cfg: dict[str, Any],
    jp_gateway_cfg: dict[str, Any],
    us_gateway_cfg: dict[str, Any],
    crypto_gateway_client,
    jp_gateway_client,
    us_gateway_client,
) -> tuple[bool, bool, bool]:
    contains_crypto_target = False
    contains_jp_target = False
    contains_us_target = False
    has_other_target = False
    for position in target_positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", position.get("security_id", ""))).strip()
        if not symbol:
            continue
        instrument_type = _infer_instrument_type(symbol, position.get("instrument_type"))
        if instrument_type == "CRYPTO":
            contains_crypto_target = True
            continue
        if instrument_type == "JP_EQ":
            contains_jp_target = True
            continue
        if instrument_type == "US_EQ":
            contains_us_target = True
            continue
        has_other_target = True

    crypto_broker_name = str(broker_map.get("CRYPTO", "paper"))
    should_use_crypto_gateway = (
        bool(crypto_gateway_cfg.get("enabled"))
        and crypto_gateway_client is not None
        and contains_crypto_target
        and crypto_broker_name == str(crypto_gateway_cfg.get("broker_name", "crypto_gateway"))
    )
    jp_broker_name = str(broker_map.get("JP", "paper"))
    should_use_jp_gateway = (
        bool(jp_gateway_cfg.get("enabled"))
        and jp_gateway_client is not None
        and contains_jp_target
        and not contains_crypto_target
        and not contains_us_target
        and not has_other_target
        and jp_broker_name == str(jp_gateway_cfg.get("broker_name", "gateway_jp"))
    )
    us_broker_name = str(broker_map.get("US", "paper"))
    should_use_us_gateway = (
        bool(us_gateway_cfg.get("enabled"))
        and us_gateway_client is not None
        and contains_us_target
        and not contains_crypto_target
        and not contains_jp_target
        and not has_other_target
        and us_broker_name == str(us_gateway_cfg.get("broker_name", "gateway_us"))
    )
    return should_use_crypto_gateway, should_use_jp_gateway, should_use_us_gateway

