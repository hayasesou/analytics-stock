from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.execution.reconcile import ReconcileSettings, reconcile_target_positions
from src.execution.risk import StrategyRiskThresholds, evaluate_strategy_risk_gate
from src.jobs.executor_crypto_runtime import process_crypto_gateway_intent
from src.jobs.executor_jp_runtime import process_jp_gateway_intent
from src.jobs.executor_paper_runtime import process_paper_intent
from src.jobs.executor_support import *
from src.jobs.executor_us_runtime import process_us_gateway_intent
from src.types import StrategyRiskEvent, StrategyRiskSnapshot


def _build_gateway_clients(
    *,
    crypto_gateway_cfg: dict[str, Any],
    jp_gateway_cfg: dict[str, Any],
    us_gateway_cfg: dict[str, Any],
    CryptoGatewayClient_cls,
    JpGatewayClient_cls,
    USGatewayClient_cls,
) -> tuple[Any | None, Any | None, Any | None]:
    crypto_gateway_client = None
    jp_gateway_client = None
    us_gateway_client = None
    if bool(crypto_gateway_cfg.get("enabled")):
        base_url = str(crypto_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            crypto_gateway_client = CryptoGatewayClient_cls(
                base_url=base_url,
                auth_token=crypto_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(crypto_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    if bool(jp_gateway_cfg.get("enabled")):
        base_url = str(jp_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            jp_gateway_client = JpGatewayClient_cls(
                base_url=base_url,
                auth_token=jp_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(jp_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    if bool(us_gateway_cfg.get("enabled")):
        base_url = str(us_gateway_cfg.get("base_url", "")).strip()
        if base_url:
            us_gateway_client = USGatewayClient_cls(
                base_url=base_url,
                auth_token=us_gateway_cfg.get("auth_token"),
                timeout_sec=_to_float(us_gateway_cfg.get("request_timeout_sec"), 8.0),
            )
    return crypto_gateway_client, jp_gateway_client, us_gateway_client


from src.jobs.executor_prechecks import (
    _classify_targets,
    _fetch_ratings_and_reconcile,
    _handle_portfolio_risk_gate,
    _handle_strategy_risk_gate,
)

def run_executor_once_impl(
    limit: int = 20,
    *,
    load_yaml_config_fn,
    load_runtime_secrets_fn,
    NeonRepository_cls,
    DiscordNotifier_cls,
    CryptoGatewayClient_cls,
    JpGatewayClient_cls,
    USGatewayClient_cls,
) -> dict[str, int]:
    cfg = load_yaml_config_fn()
    thresholds = _resolve_thresholds(cfg)
    sharpe_window_days = _resolve_sharpe_window_days(cfg)
    fundamental_overlay = _resolve_fundamental_overlay(cfg)
    data_quality = _resolve_executor_data_quality(cfg)
    reconcile_cfg = _resolve_reconcile_cfg(cfg)
    strategy_risk_cfg = _resolve_strategy_risk_cfg(cfg)
    crypto_gateway_cfg = _resolve_crypto_gateway_cfg(cfg)
    jp_gateway_cfg = _resolve_jp_gateway_cfg(cfg)
    us_gateway_cfg = _resolve_us_gateway_cfg(cfg)
    secrets = load_runtime_secrets_fn()
    repo = NeonRepository_cls(secrets.database_url)
    notifier = DiscordNotifier_cls(getattr(secrets, "discord_webhook_url", None))
    crypto_gateway_client, jp_gateway_client, us_gateway_client = _build_gateway_clients(
        crypto_gateway_cfg=crypto_gateway_cfg,
        jp_gateway_cfg=jp_gateway_cfg,
        us_gateway_cfg=us_gateway_cfg,
        CryptoGatewayClient_cls=CryptoGatewayClient_cls,
        JpGatewayClient_cls=JpGatewayClient_cls,
        USGatewayClient_cls=USGatewayClient_cls,
    )
    now = datetime.now(timezone.utc)

    intents = repo.fetch_approved_order_intents(limit=limit)
    stats = {
        "fetched": len(intents),
        "processed": 0,
        "done": 0,
        "rejected": 0,
        "failed": 0,
        "sent": 0,
        "skipped_by_fundamental": 0,
        "skipped_by_data_quality": 0,
        "skipped_by_reconcile": 0,
        "strategy_warning": 0,
        "strategy_halt": 0,
        "strategy_cooldown_reject": 0,
        "strategy_panic_close": 0,
        "executed_via_crypto_gateway": 0,
        "executed_via_jp_gateway": 0,
        "executed_via_us_gateway": 0,
    }
    for intent in intents:
        stats["processed"] += 1
        intent_id = str(intent["intent_id"])
        portfolio_id = str(intent["portfolio_id"])
        strategy_version_id = str(intent.get("strategy_version_id") or "").strip() or None
        risk_checks = intent.get("risk_checks") or {}
        if not isinstance(risk_checks, dict):
            risk_checks = {}

        repo.update_order_intent_status(intent_id, "executing")
        if _handle_portfolio_risk_gate(
            repo=repo,
            notifier=notifier,
            now=now,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            risk_checks=risk_checks,
            thresholds=thresholds,
            sharpe_window_days=sharpe_window_days,
            stats=stats,
        ):
            continue

        broker_map = intent.get("broker_map") or {}
        if not isinstance(broker_map, dict):
            broker_map = {}
        target_positions = intent.get("target_positions") or []
        if not isinstance(target_positions, list):
            target_positions = []

        if _handle_strategy_risk_gate(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            risk_checks=risk_checks,
            broker_map=broker_map,
            strategy_risk_cfg=strategy_risk_cfg,
            jp_gateway_cfg=jp_gateway_cfg,
            us_gateway_cfg=us_gateway_cfg,
            crypto_gateway_client=crypto_gateway_client,
            jp_gateway_client=jp_gateway_client,
            us_gateway_client=us_gateway_client,
            stats=stats,
        ):
            continue

        target_positions, latest_rating_by_symbol = _fetch_ratings_and_reconcile(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            intent_id=intent_id,
            risk_checks=risk_checks,
            portfolio_id=portfolio_id,
            target_positions=target_positions,
            fundamental_overlay=fundamental_overlay,
            reconcile_cfg=reconcile_cfg,
            stats=stats,
        )
        if target_positions is None:
            continue

        should_use_crypto_gateway, should_use_jp_gateway, should_use_us_gateway = _classify_targets(
            target_positions=target_positions,
            broker_map=broker_map,
            crypto_gateway_cfg=crypto_gateway_cfg,
            jp_gateway_cfg=jp_gateway_cfg,
            us_gateway_cfg=us_gateway_cfg,
            crypto_gateway_client=crypto_gateway_client,
            jp_gateway_client=jp_gateway_client,
            us_gateway_client=us_gateway_client,
        )

        if should_use_jp_gateway and process_jp_gateway_intent(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            stats=stats,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            target_positions=target_positions,
            jp_gateway_cfg=jp_gateway_cfg,
            jp_gateway_client=jp_gateway_client,
        ):
            continue

        if should_use_us_gateway and process_us_gateway_intent(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            stats=stats,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            target_positions=target_positions,
            us_gateway_cfg=us_gateway_cfg,
            us_gateway_client=us_gateway_client,
        ):
            continue

        if should_use_crypto_gateway and process_crypto_gateway_intent(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            stats=stats,
            intent_id=intent_id,
            portfolio_id=portfolio_id,
            strategy_version_id=strategy_version_id,
            target_positions=target_positions,
            crypto_gateway_cfg=crypto_gateway_cfg,
            crypto_gateway_client=crypto_gateway_client,
        ):
            continue

        process_paper_intent(
            repo=repo,
            notifier=notifier,
            now=now,
            intent=intent,
            stats=stats,
            intent_id=intent_id,
            strategy_version_id=strategy_version_id,
            portfolio_id=portfolio_id,
            broker_map=broker_map,
            target_positions=target_positions,
            latest_rating_by_symbol=latest_rating_by_symbol,
            fundamental_overlay=fundamental_overlay,
            data_quality=data_quality,
        )

    return stats
