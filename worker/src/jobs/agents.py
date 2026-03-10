from __future__ import annotations

from src.config import load_runtime_secrets, load_yaml_config
from src.jobs.agents_evaluation import (
    _decide_openclaw_go_no_go,
    _evaluate_adapter_runs,
    _render_openclaw_evaluation_markdown,
    run_openclaw_evaluation_impl,
)
from src.jobs.agents_runtime import run_agents_impl, run_agents_once_impl
from src.jobs.agents_support import *
from src.storage.db import NeonRepository


def run_agents_once(limit: int = 20) -> dict[str, int]:
    return run_agents_once_impl(
        limit=limit,
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
    )


def run_openclaw_evaluation() -> dict[str, object]:
    return run_openclaw_evaluation_impl(load_yaml_config_fn=load_yaml_config)


def run_agents(poll_seconds: int = 20, batch_limit: int = 20) -> None:
    run_agents_impl(
        poll_seconds=poll_seconds,
        batch_limit=batch_limit,
        run_agents_once_fn=run_agents_once,
    )
