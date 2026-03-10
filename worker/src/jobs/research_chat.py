from __future__ import annotations

import requests

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import send_bot_file, send_bot_message
from src.storage.db import NeonRepository
from src.jobs.research_chat_runtime import run_research_chat_impl, run_research_chat_once_impl
from src.jobs.research_chat_support import *
from src.jobs.research_chat_support import (
    _build_discord_follow_up,
    _clean_text,
    _fallback_hypotheses,
    _fetch_url_excerpt,
)
from src.jobs.research_chat_support import _send_discord_follow_up_for_session as _send_discord_follow_up_for_session_impl
from src.jobs.research_chat_charts import (
    _build_chart_png,
    _build_chart_svg,
    _build_discord_chart_message,
    _create_chart_artifacts_from_run as _create_chart_artifacts_from_run_impl,
    _execute_python,
    _execute_readonly_sql,
    _fallback_chart_specs_from_python_result,
    _fallback_chart_specs_from_sql_result,
    _is_sql_safe,
    _normalize_chart_spec,
)
from src.jobs.research_chat_tasks import _process_chart_generate as _process_chart_generate_impl, process_research_task as process_research_task_impl


def _create_chart_artifacts_from_run(repo, **kwargs):
    kwargs.setdefault("load_runtime_secrets_fn", load_runtime_secrets)
    return _create_chart_artifacts_from_run_impl(repo, **kwargs)


def _send_discord_chart_follow_up(*, payload, session_id, source_title, charts, load_runtime_secrets_fn=None):
    if str(payload.get("requested_by") or "").strip().lower() != "discord":
        return
    channel_id = _clean_text(payload.get("discord_channel_id"))
    if not channel_id or not charts:
        return
    secrets = (load_runtime_secrets_fn or load_runtime_secrets)()
    token = getattr(secrets, "discord_bot_token", None)
    send_bot_message(token, channel_id, _build_discord_chart_message(session_id=session_id, source_title=source_title, charts=charts))
    for idx, chart in enumerate(charts[:3], start=1):
        png = _build_chart_png(chart)
        if png:
            send_bot_file(token, channel_id, filename=f"research-chart-{idx}.png", content=png, message=f"{chart.get('title', 'chart')} ({chart.get('kind', '-')})", content_type="image/png")


def _send_discord_follow_up_for_session(repo, *, payload, session_id, summary, load_runtime_secrets_fn=None):
    return _send_discord_follow_up_for_session_impl(repo, payload=payload, session_id=session_id, summary=summary, load_runtime_secrets_fn=load_runtime_secrets_fn or load_runtime_secrets, send_bot_message_fn=send_bot_message)


def _process_chart_generate(repo, payload):
    return _process_chart_generate_impl(repo, payload, load_runtime_secrets_fn=load_runtime_secrets, create_chart_artifacts_from_run_fn=_create_chart_artifacts_from_run, send_discord_chart_follow_up_fn=_send_discord_chart_follow_up)


def process_research_task(repo, task, *, load_runtime_secrets_fn=None):
    return process_research_task_impl(repo, task, load_runtime_secrets_fn=load_runtime_secrets_fn or load_runtime_secrets, send_discord_follow_up_for_session_fn=_send_discord_follow_up_for_session, create_chart_artifacts_from_run_fn=_create_chart_artifacts_from_run, send_discord_chart_follow_up_fn=_send_discord_chart_follow_up)


def run_research_chat_once(limit: int = 20, assigned_role: str | None = None) -> dict[str, int]:
    return run_research_chat_once_impl(limit=limit, assigned_role=assigned_role, load_runtime_secrets_fn=load_runtime_secrets, NeonRepository_cls=NeonRepository, process_research_task_fn=process_research_task)


def run_research_chat(limit: int | None = None) -> dict[str, int]:
    return run_research_chat_impl(limit=limit, load_yaml_config_fn=load_yaml_config, run_research_chat_once_fn=run_research_chat_once)
