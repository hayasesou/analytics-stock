from __future__ import annotations

from datetime import datetime

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.youtube import YouTubeClient
from src.jobs.ingest_youtube_runtime import run_ingest_youtube_impl
from src.jobs.ingest_youtube_support import *
from src.storage.db import NeonRepository


def run_ingest_youtube(
    *,
    command: str | None = None,
    url: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    return run_ingest_youtube_impl(
        command=command,
        url=url,
        now=now,
        load_yaml_config_fn=load_yaml_config,
        load_runtime_secrets_fn=load_runtime_secrets,
        NeonRepository_cls=NeonRepository,
        YouTubeClient_cls=YouTubeClient,
    )
