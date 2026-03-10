from __future__ import annotations

import time

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.crypto_gateway import CryptoGatewayClient
from src.integrations.discord import DiscordNotifier
from src.integrations.jp_gateway import JpGatewayClient
from src.integrations.us_gateway import USGatewayClient
from src.jobs.executor_runtime import run_executor_once_impl
from src.jobs.executor_support import *
from src.storage.db import NeonRepository


def run_executor_once(limit: int = 20) -> dict[str, int]:
    return run_executor_once_impl(
        limit=limit,
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
        DiscordNotifier_cls=DiscordNotifier,
        CryptoGatewayClient_cls=CryptoGatewayClient,
        JpGatewayClient_cls=JpGatewayClient,
        USGatewayClient_cls=USGatewayClient,
    )


def run_executor(poll_seconds: int = 20, batch_limit: int = 20) -> None:
    while True:
        stats = run_executor_once(limit=batch_limit)
        print(
            "[executor] fetched=%s processed=%s done=%s sent=%s rejected=%s failed=%s reconcile_skipped=%s strat_warn=%s strat_halt=%s strat_cooldown=%s strat_panic=%s crypto_gateway=%s jp_gateway=%s us_gateway=%s"
            % (
                stats["fetched"],
                stats["processed"],
                stats["done"],
                stats.get("sent", 0),
                stats["rejected"],
                stats["failed"],
                stats.get("skipped_by_reconcile", 0),
                stats.get("strategy_warning", 0),
                stats.get("strategy_halt", 0),
                stats.get("strategy_cooldown_reject", 0),
                stats.get("strategy_panic_close", 0),
                stats.get("executed_via_crypto_gateway", 0),
                stats.get("executed_via_jp_gateway", 0),
                stats.get("executed_via_us_gateway", 0),
            ),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))
