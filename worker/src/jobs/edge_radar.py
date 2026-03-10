from __future__ import annotations

from datetime import datetime

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import DiscordNotifier
from src.jobs.edge_radar_runtime import run_edge_radar_impl
from src.jobs.edge_radar_support import *
from src.storage.db import NeonRepository


def run_edge_radar(
    scope: str = "all",
    now: datetime | None = None,
    send_notification: bool = True,
) -> dict[str, object]:
    return run_edge_radar_impl(
        scope=scope,
        now=now,
        send_notification=send_notification,
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
        DiscordNotifier_cls=DiscordNotifier,
    )
