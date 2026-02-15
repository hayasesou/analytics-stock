from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class RuntimeSecrets:
    database_url: str
    discord_webhook_url: str | None
    openai_api_key: str | None
    r2_account_id: str | None
    r2_access_key_id: str | None
    r2_secret_access_key: str | None
    r2_bucket_evidence: str | None
    r2_bucket_data: str | None
    r2_endpoint: str | None
    jquants_api_key: str | None
    jquants_email: str | None
    jquants_password: str | None
    massive_api_key: str | None
    edinet_api_key: str | None
    sec_user_agent: str


def load_yaml_config(path: str | Path = "config.yaml") -> dict[str, Any]:
    cfg_path = Path(path)
    if not cfg_path.is_absolute():
        candidates: list[Path] = []
        here = Path(__file__).resolve()
        for parent in [here.parent, *here.parents]:
            candidates.append(parent / cfg_path)
        candidates.append(Path.cwd() / cfg_path)

        resolved = next((c for c in candidates if c.exists()), None)
        if not resolved:
            raise FileNotFoundError(f"config file not found: {path}")
        cfg_path = resolved
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def load_runtime_secrets() -> RuntimeSecrets:
    database_url = _env("NEON_DATABASE_URL")
    if not database_url:
        raise RuntimeError("NEON_DATABASE_URL is required")

    return RuntimeSecrets(
        database_url=database_url,
        discord_webhook_url=_env("DISCORD_WEBHOOK_URL"),
        openai_api_key=_env("OPENAI_API_KEY"),
        r2_account_id=_env("R2_ACCOUNT_ID"),
        r2_access_key_id=_env("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
        r2_bucket_evidence=_env("R2_BUCKET_EVIDENCE"),
        r2_bucket_data=_env("R2_BUCKET_DATA"),
        r2_endpoint=_env("R2_ENDPOINT"),
        jquants_api_key=_env("JQUANTS_API_KEY"),
        jquants_email=_env("JQUANTS_EMAIL"),
        jquants_password=_env("JQUANTS_PASSWORD"),
        massive_api_key=_env("MASSIVE_API_KEY"),
        edinet_api_key=_env("EDINET_API_KEY"),
        sec_user_agent=_env("SEC_USER_AGENT", "stock-analysis (contact: hayasesou3@gmail.com)") or "stock-analysis (contact: hayasesou3@gmail.com)",
    )
