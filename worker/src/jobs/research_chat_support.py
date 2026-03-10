from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import io
import json
import os
import re
from typing import Any

from src.integrations.discord import build_web_session_url, send_bot_message
from src.llm.openai_client import DEFAULT_OPENAI_MODEL, OpenAIClientError, request_openai_json
from src.llm.research_prompts import RESEARCH_HYPOTHESIS_JSON_SCHEMA, build_research_prompt, classify_research_mode
from src.storage.db import NeonRepository
import requests

SUPPORTED_TASK_TYPES = [
    "research.extract_input",
    "research.generate_hypothesis",
    "research.critic_review",
    "research.quant_plan",
    "research.code_generate",
    "research.portfolio_build",
    "research.artifact_run",
    "research.chart_generate",
    "research.validate_outcome",
    "research.session_summarize",
]
RESEARCH_CHART_PLAN_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["charts"],
    "properties": {
        "charts": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "kind", "x_axis_label", "y_axis_label", "summary", "series"],
                "properties": {
                    "title": {"type": "string", "minLength": 1, "maxLength": 120},
                    "kind": {"type": "string", "enum": ["line", "bar", "scatter", "area"]},
                    "x_axis_label": {"type": "string", "minLength": 1, "maxLength": 80},
                    "y_axis_label": {"type": "string", "minLength": 1, "maxLength": 80},
                    "summary": {"type": "string", "minLength": 1, "maxLength": 240},
                    "series": {"type": "array", "minItems": 1, "maxItems": 4},
                },
            },
        }
    },
}
FORBIDDEN_SQL_PATTERNS = [r"\binsert\b", r"\bupdate\b", r"\bdelete\b", r"\bdrop\b", r"\balter\b", r"\btruncate\b", r"\bcopy\b", r"\bcreate\b", r"\bgrant\b", r"\brevoke\b"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _split_urls_and_text(inputs: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    urls: list[str] = []
    texts: list[str] = []
    for item in inputs:
        source_url = _clean_text(item.get("source_url"))
        if source_url:
            urls.append(source_url)
        text = _clean_text(item.get("extracted_text") or item.get("raw_text"))
        if text:
            texts.append(text)
    return urls, texts


def _resolve_openai_model() -> str:
    return str(os.getenv("OPENAI_MODEL", "") or "").strip() or DEFAULT_OPENAI_MODEL


def _resolve_runtime_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("research_chat", {})
    if not isinstance(root, dict):
        root = {}
    try:
        poll_interval_sec = max(1.0, float(root.get("poll_interval_sec", 5)))
    except (TypeError, ValueError):
        poll_interval_sec = 5.0
    try:
        batch_size = max(1, int(root.get("batch_size", 20)))
    except (TypeError, ValueError):
        batch_size = 20
    return {"poll_interval_sec": poll_interval_sec, "batch_size": batch_size}


def _fetch_url_excerpt(url: str, timeout_sec: float = 10.0) -> dict[str, Any]:
    headers = {
        "User-Agent": "analytics-stock research-bot/1.0 (+https://localhost)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5",
    }
    resp = requests.get(url, headers=headers, timeout=timeout_sec)
    resp.raise_for_status()
    content_type = str(resp.headers.get("content-type", "")).lower()
    raw = resp.text[:300_000]
    title_match = re.search(r"<title[^>]*>(.*?)</title>", raw, flags=re.IGNORECASE | re.DOTALL)
    title = _clean_text(re.sub(r"<[^>]+>", " ", title_match.group(1))) if title_match else ""
    if "html" in content_type or "<html" in raw.lower():
        body = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", raw)
        body = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", body)
        body = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", body)
        body = re.sub(r"(?s)<[^>]+>", " ", body)
    else:
        body = raw
    excerpt = _clean_text(body)[:4000]
    return {"title": title, "excerpt": excerpt, "content_type": content_type or None, "fetched_at": _utc_now().isoformat()}


def _fallback_hypotheses(*, question: str, urls: list[str], texts: list[str], security_id: str | None) -> dict[str, Any]:
    target = security_id or "関連銘柄"
    basis = f"URL {len(urls)} 件とテキスト {len(texts)} 件" if urls or texts else "入力テキスト"
    mode = classify_research_mode(question=question, security_id=security_id, urls=urls)
    return {
        "mode": mode,
        "summary": f"{basis} を元に初期仮説を作成しました。 まだ一次情報、業績寄与、競合比較、何が織り込み済みかの裏取りは不十分です。",
        "hypotheses": [
            {
                "stance": "watch",
                "horizon_days": 5,
                "thesis_md": f"{target} は短期的に再評価余地がありますが、現時点では材料と業績寄与の接続が弱いです。 {basis} を起点に、売上・需要・価格反応のどこが未織り込みかを確認する価値があります。",
                "falsification_md": "価格、開示、需給、競合比較のいずれにも優位な裏付けが見られない場合は撤回します。",
                "confidence": 0.42,
                "validation_plan": "イベント後 1日/5日/20日の価格反応、出来高、関連開示、コンセンサス修正、競合比較を確認して、未織り込み論点が本当に株価に転化したかを検証する。",
                "key_metrics": ["ret_1d", "ret_5d", "ret_20d", "volume_change", "estimate_revision", "peer_relative_return"],
            },
            {
                "stance": "bullish",
                "horizon_days": 20,
                "thesis_md": f"{target} に対してポジティブ仮説を検討できますが、現時点では追加検証前提です。 材料が中期の業績や受注に接続し、かつ市場が十分に織り込んでいない場合にのみ強気へ昇格します。",
                "falsification_md": "一次情報、競合比較、価格反応を見ても成長ドライバーや未織り込み余地が確認できない場合は bullish 仮説を破棄します。",
                "confidence": 0.33,
                "validation_plan": "相対リターン、出来高スパイク、業績/契約の追加開示、ガイダンスや見積修正を 20 営業日追跡する。",
                "key_metrics": ["ret_20d", "relative_return_vs_sector", "volume_spike", "guidance_change", "peer_growth_gap"],
            },
        ],
    }


def _generate_hypotheses_via_llm(*, question: str, security_id: str | None, urls: list[str], texts: list[str], url_summaries: list[dict[str, Any]], load_runtime_secrets_fn) -> dict[str, Any]:
    secrets = load_runtime_secrets_fn()
    api_key = str(getattr(secrets, "openai_api_key", "") or "").strip()
    mode = classify_research_mode(question=question, security_id=security_id, urls=urls)
    if not api_key:
        return _fallback_hypotheses(question=question, urls=urls, texts=texts, security_id=security_id)
    prompt = build_research_prompt(mode=mode, question=question, security_id=security_id, url_summaries=url_summaries, text_blocks=texts)
    try:
        result = request_openai_json(prompt=prompt, api_key=api_key, model=_resolve_openai_model(), json_schema=RESEARCH_HYPOTHESIS_JSON_SCHEMA, max_output_tokens=1200)
        if "mode" not in result:
            result["mode"] = mode
        return result
    except (OpenAIClientError, requests.RequestException, RuntimeError):
        return _fallback_hypotheses(question=question, urls=urls, texts=texts, security_id=security_id)


def _build_session_summary(hypotheses: list[dict[str, Any]], urls: list[str]) -> str:
    if not hypotheses:
        return "## 結論\n仮説はまだ生成されていません。\n\n## 根拠\n1. session に入力は保存済みです。\n2. 後続 task の結果待ちです。\n3. 手動レビューで補完可能です。\n\n## 反証\n入力から対象銘柄や論点が特定できない場合、この session は継続価値が低いです。\n\n## 次アクション\n1. 対象銘柄を明示する。\n2. URL か補足文を追加する。"
    lead = hypotheses[0]
    return f"## 結論\n主仮説は {lead.get('stance', 'watch')} / {lead.get('horizon_days', '-')}d として保持します。\n\n## 根拠\n1. session に {len(urls)} 件の URL が保存されています。\n2. 仮説数は {len(hypotheses)} 件です。\n3. critic / quant / code / portfolio の補助 artifact を生成しました。\n\n## 反証\n{lead.get('falsification_md') or '一次情報で前提が崩れた場合は撤回します。'}\n\n## 次アクション\n1. Artifacts の SQL/Python を実行する。\n2. 価格推移を確認して validation を回す。\n3. 必要なら対象銘柄を絞って再度 session を投げる。"


def _trim_block(text: str, limit: int) -> str:
    normalized = _clean_text(text)
    return normalized if len(normalized) <= limit else f"{normalized[: max(0, limit - 1)].rstrip()}…"


def _build_discord_follow_up(*, session_id: str, summary: str, hypotheses: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> str:
    session_url = build_web_session_url(session_id)
    lines = ["research follow-up", f"session={session_id}"]
    if session_url:
        lines.append(f"url={session_url}")
    lines.extend(["", "summary", _trim_block(summary, 600), "", "hypotheses"])
    if hypotheses:
        for idx, item in enumerate(hypotheses[:3], start=1):
            metadata = _as_dict(item.get("metadata"))
            metrics = list(metadata.get("key_metrics") or [])
            lines.extend([f"{idx}. {item.get('stance', '-')} / {item.get('horizon_days', '-')}d / conf={item.get('confidence', '-')}", f"thesis: {_trim_block(str(item.get('thesis_md') or ''), 220)}", f"falsification: {_trim_block(str(item.get('falsification_md') or ''), 160)}", f"validation: {_trim_block(str(metadata.get('validation_plan') or ''), 180)}", f"metrics: {', '.join(str(metric) for metric in metrics[:6]) or '-'}"])
    else:
        lines.append("(none)")
    lines.extend(["", "artifacts"])
    lines.extend([f"- {artifact.get('artifact_type', '-')} | {artifact.get('title', '-')}" for artifact in artifacts[:8]] or ["(none)"])
    return "\n".join(lines)[:1900]


def _send_discord_follow_up_for_session(repo: NeonRepository, *, payload: dict[str, Any], session_id: str, summary: str, load_runtime_secrets_fn, send_bot_message_fn=send_bot_message) -> None:
    if str(payload.get("requested_by") or "").strip().lower() != "discord":
        return
    channel_id = _clean_text(payload.get("discord_channel_id"))
    if not channel_id:
        return
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    artifacts = repo.fetch_research_artifacts_for_session(session_id)
    secrets = load_runtime_secrets_fn()
    send_bot_message_fn(getattr(secrets, "discord_bot_token", None), channel_id, _build_discord_follow_up(session_id=session_id, summary=summary, hypotheses=hypotheses, artifacts=artifacts))


def _build_portfolio_note(hypotheses: list[dict[str, Any]]) -> tuple[str, dict[str, float]]:
    bullish = sum(1 for item in hypotheses if str(item.get("stance")) == "bullish")
    bearish = sum(1 for item in hypotheses if str(item.get("stance")) == "bearish")
    watch = sum(1 for item in hypotheses if str(item.get("stance")) == "watch")
    total = max(1, len(hypotheses))
    risky = bullish + bearish
    cash = 0.6 if risky <= 1 else 0.35 if risky == 2 else 0.2
    active = max(0.0, 1.0 - cash)
    weights = {
        "long_per_hypothesis": round(active * bullish / max(1, risky), 4) if bullish > 0 else 0.0,
        "short_per_hypothesis": round(active * bearish / max(1, risky), 4) if bearish > 0 else 0.0,
        "cash": round(cash, 4),
        "watch_budget": round(0.05 if watch > 0 else 0.0, 4),
    }
    body = f"- hypotheses: {total}\n- bullish: {bullish}\n- bearish: {bearish}\n- watch: {watch}\n- suggested cash: {weights['cash']:.2%}\n- long per bullish hypothesis: {weights['long_per_hypothesis']:.2%}\n- short per bearish hypothesis: {weights['short_per_hypothesis']:.2%}"
    return body, weights


def _build_critic_note(hypotheses: list[dict[str, Any]]) -> str:
    if not hypotheses:
        return "仮説がないため critic review を生成できません。"
    lines = []
    for idx, item in enumerate(hypotheses, start=1):
        thesis = _clean_text(item.get("thesis_md"))
        lines.extend([f"{idx}. 対象仮説: {thesis[:140]}", "   - リスク: 対象銘柄・ドライバー・観測期間の特定がまだ粗いです。", "   - 追加確認: 決算、ガイダンス、価格反応、出来高、一次情報 citation。", "   - バイアス: テーマ先行で価格確認が後追いになっている可能性があります。"])
    return "\n".join(lines)


def _build_quant_sql(symbol_text: str | None) -> str:
    if symbol_text:
        return "with target as (\n  select id\n  from securities\n" + f"  where security_id = '{symbol_text}' or upper(ticker) = upper('{symbol_text.split(':', 1)[-1]}')\n" + "  limit 1\n)\nselect p.trade_date, p.close_raw\nfrom prices_daily p\njoin target t on t.id = p.security_id\norder by p.trade_date desc\nlimit 60;"
    return "select current_date as as_of_date;"


def _build_python_template(symbol_text: str | None) -> str:
    label = symbol_text or "TARGET"
    return "import math\n\n" + f"symbol = {label!r}\n" + "returns = [0.01, -0.005, 0.012, 0.004]\nmean_ret = sum(returns) / len(returns)\nvariance = sum((x - mean_ret) ** 2 for x in returns) / len(returns)\nvol = math.sqrt(variance)\nprint({'symbol': symbol, 'mean_return': round(mean_ret, 6), 'volatility': round(vol, 6)})\n"


def _select_primary_symbol(assets: list[dict[str, Any]]) -> str | None:
    if not assets:
        return None
    first = assets[0]
    return _clean_text(first.get("security_id") or first.get("symbol_text") or first.get("ticker")) or None


def _resolve_entry_and_returns(repo: NeonRepository, symbol: str, created_at: datetime) -> dict[str, Any] | None:
    resolved = repo.fetch_latest_price_for_symbol(symbol)
    if not resolved:
        return None
    security_id = str(resolved.get("security_id", "")).strip()
    if not security_id:
        return None
    history = repo.fetch_price_history_for_security(security_id, created_at.date() - timedelta(days=5), created_at.date() + timedelta(days=35))
    if history.empty:
        return None
    history = history.sort_values("trade_date").reset_index(drop=True)
    eligible = history[history["trade_date"] >= created_at.date()]
    if eligible.empty:
        return None
    entry_row = eligible.iloc[0]
    entry_price = float(entry_row["close_raw"])
    closes = [float(value) for value in eligible["close_raw"].tolist()]
    trade_dates = eligible["trade_date"].tolist()

    def _ret_at(offset: int) -> float | None:
        return None if len(closes) <= offset else closes[offset] / entry_price - 1.0

    return {"security_id": security_id, "entry_date": trade_dates[0], "entry_price": entry_price, "ret_1d": _ret_at(1), "ret_5d": _ret_at(5), "ret_20d": _ret_at(20), "mfe": max(closes) / entry_price - 1.0, "mae": min(closes) / entry_price - 1.0}


def _label_outcome(stance: str, ret_5d: float | None) -> str:
    if ret_5d is None:
        return "open"
    if stance == "bullish":
        return "hit" if ret_5d > 0 else "miss"
    if stance == "bearish":
        return "hit" if ret_5d < 0 else "miss"
    return "partial" if abs(ret_5d) < 0.02 else "miss"
