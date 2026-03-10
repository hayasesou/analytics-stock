from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.jobs.edge_radar_support import (
    _build_crypto_edge_state,
    _build_equity_edge_state,
    _create_crypto_order_intents,
    _normalize_scope,
    _resolve_edge_radar_cfg,
    _to_discord_payload,
)


def run_edge_radar_impl(
    *,
    scope: str = "all",
    now: datetime | None = None,
    send_notification: bool = True,
    load_yaml_config_fn,
    load_runtime_secrets_fn,
    NeonRepository_cls,
    DiscordNotifier_cls,
) -> dict[str, Any]:
    requested_scope = _normalize_scope(scope)
    cfg = load_yaml_config_fn()
    resolved_cfg = _resolve_edge_radar_cfg(cfg)
    if not bool(resolved_cfg["enabled"]):
        return {"scope": requested_scope, "enabled": False, "inserted": 0, "intents_created": 0}
    secrets = load_runtime_secrets_fn()
    repo = NeonRepository_cls(secrets.database_url)
    notifier = DiscordNotifier_cls(secrets.discord_webhook_url)
    jst = ZoneInfo(str(cfg.get("timezone", "Asia/Tokyo")))
    if now is None:
        observed_now_utc = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        observed_now_utc = now.replace(tzinfo=timezone.utc)
    else:
        observed_now_utc = now.astimezone(timezone.utc)
    now_jst = observed_now_utc.astimezone(jst)
    equities_states = []
    crypto_states = []
    if requested_scope in {"equities", "all"}:
        equities_cfg = resolved_cfg["equities"]
        equities_slot_utc = now_jst.replace(hour=int(float(equities_cfg["run_hour_jst"])), minute=int(float(equities_cfg["run_minute_jst"])), second=0, microsecond=0).astimezone(timezone.utc)
        equities_states = [_build_equity_edge_state(row=row, observed_at=equities_slot_utc, resolved_cfg=resolved_cfg) for row in repo.fetch_latest_weekly_candidates(limit=int(equities_cfg["max_candidates"]))]
    if requested_scope in {"crypto", "all"}:
        crypto_cfg = resolved_cfg["crypto"]
        crypto_slot_utc = now_jst.replace(minute=int(float(crypto_cfg["run_minute_jst"])), second=0, microsecond=0).astimezone(timezone.utc)
        crypto_states = [_build_crypto_edge_state(row=row, observed_at=crypto_slot_utc, resolved_cfg=resolved_cfg) for row in repo.fetch_latest_strategy_edge_inputs(asset_scope="CRYPTO", statuses=["candidate", "approved", "paper", "live"], limit=int(crypto_cfg["max_candidates"]))]
    all_states = [*equities_states, *crypto_states]
    inserted = repo.insert_edge_states(all_states)
    intents_created = _create_crypto_order_intents(repo=repo, states=crypto_states, observed_at=observed_now_utc, resolved_cfg=resolved_cfg) if requested_scope in {"crypto", "all"} and crypto_states else 0
    if send_notification:
        top_n = int(resolved_cfg["notify_top_n"])
        if requested_scope in {"equities", "all"}:
            notifier.send_edge_radar(now=now_jst, scope="equities", rows=_to_discord_payload(equities_states), top_n=top_n)
        if requested_scope in {"crypto", "all"}:
            notifier.send_edge_radar(now=now_jst, scope="crypto", rows=_to_discord_payload(crypto_states), top_n=top_n)
    return {"scope": requested_scope, "enabled": True, "inserted": inserted, "equities": len(equities_states), "crypto": len(crypto_states), "intents_created": intents_created}


__all__ = ["run_edge_radar_impl"]
