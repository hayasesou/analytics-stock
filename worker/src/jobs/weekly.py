from __future__ import annotations

from src.config import load_runtime_secrets, load_yaml_config
from src.data.provider import HybridDataProvider
from src.integrations.discord import DiscordNotifier
from src.jobs.weekly_runtime import run_weekly_impl
from src.jobs.weekly_support import *
from src.llm.reporting import (
    generate_security_report,
    generate_security_report_with_llm,
    generate_weekly_summary_report,
    generate_weekly_summary_report_with_llm,
)
from src.storage.db import NeonRepository


def run_weekly() -> str:
    return run_weekly_impl(
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
        HybridDataProvider_cls=HybridDataProvider,
        DiscordNotifier_cls=DiscordNotifier,
        generate_security_report_fn=generate_security_report,
        generate_security_report_with_llm_fn=generate_security_report_with_llm,
        generate_weekly_summary_report_fn=generate_weekly_summary_report,
        generate_weekly_summary_report_with_llm_fn=generate_weekly_summary_report_with_llm,
    )
