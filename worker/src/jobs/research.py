from __future__ import annotations

from src.analytics.validation import run_walk_forward_validation
from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import DiscordNotifier
from src.jobs.research_lifecycle import _paper_requirements_for_scope, _resolve_lifecycle_cfg, _run_strategy_lifecycle
from src.jobs.research_runtime import run_research_impl
from src.jobs.research_support import *
from src.research import build_deep_research_snapshot, compute_fundamental_rating, parse_deep_research_file_if_configured
from src.storage.db import NeonRepository
from src.storage.r2 import R2Storage


def run_research(limit: int | None = None) -> str:
    return run_research_impl(
        limit=limit,
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
        DiscordNotifier_cls=DiscordNotifier,
        R2Storage_cls=R2Storage,
        compute_fundamental_rating_fn=compute_fundamental_rating,
        run_walk_forward_validation_fn=run_walk_forward_validation,
        parse_deep_research_file_if_configured_fn=parse_deep_research_file_if_configured,
        build_deep_research_snapshot_fn=build_deep_research_snapshot,
    )
