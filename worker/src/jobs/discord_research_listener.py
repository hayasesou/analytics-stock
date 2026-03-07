from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import re
from typing import Any

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import build_web_session_url
from src.storage.db import NeonRepository
from src.types import ResearchArtifactSpec, ResearchExternalInput, ResearchHypothesisAssetSpec, ResearchHypothesisSpec


URL_RE = re.compile(r"(https?://[^\s]+)", flags=re.IGNORECASE)
SECURITY_ID_RE = re.compile(r"\b(JP:\d{4}|US:[A-Z][A-Z0-9.-]{0,6})\b", flags=re.IGNORECASE)


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _resolve_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("discord_research", {})
    if not isinstance(root, dict):
        root = {}
    return {
        "enabled": _to_bool(root.get("enabled"), True),
        "auto_thread": _to_bool(root.get("auto_thread"), True),
        "max_urls_per_message": max(1, min(5, int(root.get("max_urls_per_message", 5) or 5))),
    }


def _extract_urls(text: str, limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for match in URL_RE.finditer(text):
        url = str(match.group(1)).strip()
        if url and url not in seen:
            out.append(url)
            seen.add(url)
        if len(out) >= limit:
            break
    return out


def _extract_security_id(text: str) -> str | None:
    match = SECURITY_ID_RE.search(text)
    if not match:
        return None
    return match.group(1).upper()


def _build_initial_hypotheses(question: str, security_id: str | None) -> list[dict[str, Any]]:
    target = security_id or "関連銘柄"
    return [
        {
            "stance": "watch",
            "horizon_days": 5,
            "thesis_md": f"{target} は直近材料の整理後に再評価余地があります。まずはテーマ、需給、開示の整合を確認します。",
            "falsification_md": "一次情報を確認しても需給改善や業績寄与の裏付けがない場合は撤回します。",
            "confidence": 0.45,
        },
        {
            "stance": "bullish",
            "horizon_days": 20,
            "thesis_md": f"{target} は追加検証で bullish に昇格できる可能性があります。価格反応と業績ドライバーの一致を見ます。",
            "falsification_md": "価格が逆行し、開示やファンダの前提も弱い場合は bullish 仮説を破棄します。",
            "confidence": 0.36,
        },
    ]


def _build_answer(hypotheses: list[dict[str, Any]], url_count: int, security_id: str | None) -> str:
    lead = hypotheses[0]
    return (
        "## 結論\n"
        f"{security_id or '対象未確定'} について、初期状態では {lead['stance']} 仮説を保存しました。\n\n"
        "## 根拠\n"
        f"1. URL {url_count} 件と自然文を session に保存しました。\n"
        f"2. {len(hypotheses)} 本の初期仮説を作成しました。\n"
        "3. research / critic / quant / code / portfolio task をキュー投入しました。\n\n"
        "## 反証\n"
        f"{lead['falsification_md']}\n\n"
        "## 次アクション\n"
        "1. Web の Research Chat から session を確認する。\n"
        "2. Artifacts の SQL/Python を実行する。\n"
        "3. 必要なら対象銘柄を明示して追加入力する。"
    )


def _topic_from_message(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return "research"
    return cleaned[:40]


def _bootstrap_session(
    repo: NeonRepository,
    *,
    content: str,
    source_label: str,
    discord_channel_id: str | None = None,
    discord_source_message_id: str | None = None,
) -> tuple[str, str]:
    security_id = _extract_security_id(content)
    urls = _extract_urls(content, limit=5)
    session_id = repo.create_chat_session(title=f"Discord Research - {_topic_from_message(content)}")
    message_id = repo.append_chat_message(
        session_id=session_id,
        role="user",
        content=content,
    )
    input_ids: list[str] = []
    for url in urls:
        input_ids.append(
            repo.insert_research_external_input(
                ResearchExternalInput(
                    session_id=session_id,
                    message_id=message_id,
                    source_type="discord",
                    source_url=url,
                    raw_text=content,
                    metadata={
                        "requested_by": source_label,
                        "discord_channel_id": discord_channel_id,
                        "discord_source_message_id": discord_source_message_id,
                    },
                )
            )
        )
    stripped = URL_RE.sub(" ", content)
    if stripped.strip():
        input_ids.append(
            repo.insert_research_external_input(
                ResearchExternalInput(
                    session_id=session_id,
                    message_id=message_id,
                    source_type="text",
                    raw_text=stripped.strip(),
                    extracted_text=stripped.strip(),
                    extraction_status="success",
                    quality_grade="B",
                    metadata={
                        "requested_by": source_label,
                        "discord_channel_id": discord_channel_id,
                        "discord_source_message_id": discord_source_message_id,
                    },
                )
            )
        )
    hypotheses = _build_initial_hypotheses(content, security_id)
    hypothesis_ids: list[str] = []
    for item in hypotheses:
        assets = []
        if security_id:
            assets.append(
                ResearchHypothesisAssetSpec(
                    asset_class="JP_EQ" if security_id.startswith("JP:") else "US_EQ",
                    symbol_text=security_id,
                    weight_hint=0.2 if item["stance"] == "bullish" else 0.0,
                    confidence=float(item["confidence"]),
                )
            )
        hypothesis_ids.append(
            repo.insert_research_hypothesis(
                ResearchHypothesisSpec(
                    session_id=session_id,
                    external_input_id=input_ids[0] if input_ids else None,
                    parent_message_id=message_id,
                    stance=str(item["stance"]),
                    horizon_days=int(item["horizon_days"]),
                    thesis_md=str(item["thesis_md"]),
                    falsification_md=str(item["falsification_md"]),
                    confidence=float(item["confidence"]),
                    metadata={"source": "discord_research_listener"},
                    assets=assets,
                )
            )
        )
    note_artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=hypothesis_ids[0] if hypothesis_ids else None,
            artifact_type="note",
            title="Discord Research Intake",
            body_md=content,
            metadata={"source": "discord_research_listener"},
        )
    )
    for task_type, assigned_role in [
        ("research.extract_input", "research"),
        ("research.generate_hypothesis", "research"),
        ("research.critic_review", "critic"),
        ("research.quant_plan", "quant"),
        ("research.code_generate", "code"),
        ("research.portfolio_build", "portfolio"),
    ]:
        repo.enqueue_agent_task(
            task_type=task_type,
            payload={
                "session_id": session_id,
                "message_id": message_id,
                "external_input_ids": input_ids,
                "hypothesis_ids": hypothesis_ids,
                "artifact_id": note_artifact_id,
                "question": content,
                "security_id": security_id,
                "requested_by": source_label,
                "discord_channel_id": discord_channel_id,
                "discord_source_message_id": discord_source_message_id,
            },
            session_id=session_id,
            assigned_role=assigned_role,
            dedupe_key=f"{session_id}:{task_type}",
        )
    answer = _build_answer(hypotheses, len(urls), security_id)
    session_url = build_web_session_url(session_id)
    if session_url:
        answer = f"{answer}\n\n## Session URL\n{session_url}"
    repo.append_chat_message(
        session_id=session_id,
        role="assistant",
        content=answer,
        answer_after=answer,
        change_reason="discord research session bootstrapped",
    )
    return session_id, answer


def run_discord_research_listener() -> None:
    cfg = load_yaml_config()
    listener_cfg = _resolve_cfg(cfg)
    if not listener_cfg["enabled"]:
        print("[discord-research] disabled", flush=True)
        return

    secrets = load_runtime_secrets()
    bot_token = str(getattr(secrets, "discord_bot_token", "") or "").strip()
    if not bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")

    try:
        import discord  # type: ignore[import-untyped]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("discord.py is required for discord_research_listener") from exc

    repo = NeonRepository(secrets.database_url)
    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.message_content = True
    client = discord.Client(intents=intents)

    async def _reply_target(message: Any) -> Any:
        if not listener_cfg["auto_thread"]:
            return message.channel
        channel = message.channel
        if getattr(channel, "type", None) and str(getattr(channel, "type")).lower().endswith("thread"):
            return channel
        create_thread = getattr(message, "create_thread", None)
        if create_thread is None:
            return channel
        try:
            name = f"research-{datetime.now(timezone.utc).strftime('%m%d-%H%M')}"
            return await create_thread(name=name[:80], auto_archive_duration=60)
        except Exception:  # noqa: BLE001
            return channel

    @client.event
    async def on_ready() -> None:
        print(f"[discord-research] connected as {client.user}", flush=True)

    @client.event
    async def on_message(message: Any) -> None:
        if client.user is not None and getattr(message.author, "id", None) == getattr(client.user, "id", None):
            return
        if bool(getattr(message.author, "bot", False)):
            return
        user = client.user
        if user is None:
            return
        if user not in getattr(message, "mentions", []):
            return

        content = re.sub(rf"<@!?{getattr(user, 'id', 0)}>", " ", str(getattr(message, "content", "") or "")).strip()
        if not content:
            return

        target = await _reply_target(message)
        await target.send("research session を作成しています...")
        try:
            session_id, answer = await asyncio.to_thread(
                _bootstrap_session,
                repo,
                content=content,
                source_label="discord",
                discord_channel_id=str(getattr(target, "id", "") or ""),
                discord_source_message_id=str(getattr(message, "id", "") or ""),
            )
            await target.send(f"session={session_id}\n{answer[:1700]}")
        except Exception as exc:  # noqa: BLE001
            await target.send(f"research session 作成に失敗しました: {exc}")

    client.run(bot_token)
