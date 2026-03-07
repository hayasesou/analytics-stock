from __future__ import annotations

from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
import io
import json
import os
import re
import struct
import time
from typing import Any
import zlib

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import build_web_session_url, send_bot_file, send_bot_message
from src.llm.openai_client import DEFAULT_OPENAI_MODEL, OpenAIClientError, request_openai_json
from src.llm.research_prompts import (
    RESEARCH_HYPOTHESIS_JSON_SCHEMA,
    build_research_prompt,
    classify_research_mode,
)
from src.storage.db import NeonRepository
from src.types import ResearchArtifactRunSpec, ResearchArtifactSpec, ResearchHypothesisOutcomeSpec
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
                    "series": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "data"],
                            "properties": {
                                "name": {"type": "string", "minLength": 1, "maxLength": 80},
                                "data": {
                                    "type": "array",
                                    "minItems": 2,
                                    "maxItems": 200,
                                    "items": {
                                        "type": "array",
                                        "minItems": 2,
                                        "maxItems": 2,
                                        "prefixItems": [
                                            {"type": "string", "minLength": 1, "maxLength": 80},
                                            {"type": "number"},
                                        ],
                                        "items": False,
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    },
}
FORBIDDEN_SQL_PATTERNS = [
    r"\binsert\b",
    r"\bupdate\b",
    r"\bdelete\b",
    r"\bdrop\b",
    r"\balter\b",
    r"\btruncate\b",
    r"\bcopy\b",
    r"\bcreate\b",
    r"\bgrant\b",
    r"\brevoke\b",
]
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
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
    candidate = os.getenv("OPENAI_MODEL", "")
    selected = str(candidate or "").strip()
    return selected or DEFAULT_OPENAI_MODEL


def _resolve_runtime_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("research_chat", {})
    if not isinstance(root, dict):
        root = {}
    poll_interval = root.get("poll_interval_sec", 5)
    batch_size = root.get("batch_size", 20)
    try:
        poll_interval_sec = max(1.0, float(poll_interval))
    except (TypeError, ValueError):
        poll_interval_sec = 5.0
    try:
        normalized_batch_size = max(1, int(batch_size))
    except (TypeError, ValueError):
        normalized_batch_size = 20
    return {
        "poll_interval_sec": poll_interval_sec,
        "batch_size": normalized_batch_size,
    }


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
    body = _clean_text(body)
    excerpt = body[:4000]
    return {
        "title": title,
        "excerpt": excerpt,
        "content_type": content_type or None,
        "fetched_at": _utc_now().isoformat(),
    }


def _fallback_hypotheses(
    *,
    question: str,
    urls: list[str],
    texts: list[str],
    security_id: str | None,
) -> dict[str, Any]:
    target = security_id or "関連銘柄"
    basis = f"URL {len(urls)} 件とテキスト {len(texts)} 件" if urls or texts else "入力テキスト"
    mode = classify_research_mode(question=question, security_id=security_id, urls=urls)
    return {
        "mode": mode,
        "summary": (
            f"{basis} を元に初期仮説を作成しました。"
            " まだ一次情報、業績寄与、競合比較、何が織り込み済みかの裏取りは不十分です。"
        ),
        "hypotheses": [
            {
                "stance": "watch",
                "horizon_days": 5,
                "thesis_md": (
                    f"{target} は短期的に再評価余地がありますが、現時点では材料と業績寄与の接続が弱いです。"
                    f" {basis} を起点に、売上・需要・価格反応のどこが未織り込みかを確認する価値があります。"
                ),
                "falsification_md": "価格、開示、需給、競合比較のいずれにも優位な裏付けが見られない場合は撤回します。",
                "confidence": 0.42,
                "validation_plan": (
                    "イベント後 1日/5日/20日の価格反応、出来高、関連開示、コンセンサス修正、"
                    "競合比較を確認して、未織り込み論点が本当に株価に転化したかを検証する。"
                ),
                "key_metrics": ["ret_1d", "ret_5d", "ret_20d", "volume_change", "estimate_revision", "peer_relative_return"],
            },
            {
                "stance": "bullish",
                "horizon_days": 20,
                "thesis_md": (
                    f"{target} に対してポジティブ仮説を検討できますが、現時点では追加検証前提です。"
                    " 材料が中期の業績や受注に接続し、かつ市場が十分に織り込んでいない場合にのみ強気へ昇格します。"
                ),
                "falsification_md": "一次情報、競合比較、価格反応を見ても成長ドライバーや未織り込み余地が確認できない場合は bullish 仮説を破棄します。",
                "confidence": 0.33,
                "validation_plan": "相対リターン、出来高スパイク、業績/契約の追加開示、ガイダンスや見積修正を 20 営業日追跡する。",
                "key_metrics": ["ret_20d", "relative_return_vs_sector", "volume_spike", "guidance_change", "peer_growth_gap"],
            },
        ],
    }


def _generate_hypotheses_via_llm(
    *,
    question: str,
    security_id: str | None,
    urls: list[str],
    texts: list[str],
    url_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    secrets = load_runtime_secrets()
    api_key = str(getattr(secrets, "openai_api_key", "") or "").strip()
    mode = classify_research_mode(question=question, security_id=security_id, urls=urls)
    if not api_key:
        return _fallback_hypotheses(question=question, urls=urls, texts=texts, security_id=security_id)
    prompt = build_research_prompt(
        mode=mode,
        question=question,
        security_id=security_id,
        url_summaries=url_summaries,
        text_blocks=texts,
    )
    try:
        result = request_openai_json(
            prompt=prompt,
            api_key=api_key,
            model=_resolve_openai_model(),
            json_schema=RESEARCH_HYPOTHESIS_JSON_SCHEMA,
            max_output_tokens=1200,
        )
        if "mode" not in result:
            result["mode"] = mode
        return result
    except (OpenAIClientError, requests.RequestException, RuntimeError):
        return _fallback_hypotheses(question=question, urls=urls, texts=texts, security_id=security_id)


def _build_session_summary(hypotheses: list[dict[str, Any]], urls: list[str]) -> str:
    if not hypotheses:
        return (
            "## 結論\n仮説はまだ生成されていません。\n\n"
            "## 根拠\n1. session に入力は保存済みです。\n2. 後続 task の結果待ちです。\n3. 手動レビューで補完可能です。\n\n"
            "## 反証\n入力から対象銘柄や論点が特定できない場合、この session は継続価値が低いです。\n\n"
            "## 次アクション\n1. 対象銘柄を明示する。\n2. URL か補足文を追加する。"
        )
    lead = hypotheses[0]
    return (
        "## 結論\n"
        f"主仮説は {lead.get('stance', 'watch')} / {lead.get('horizon_days', '-')}d として保持します。\n\n"
        "## 根拠\n"
        f"1. session に {len(urls)} 件の URL が保存されています。\n"
        f"2. 仮説数は {len(hypotheses)} 件です。\n"
        "3. critic / quant / code / portfolio の補助 artifact を生成しました。\n\n"
        "## 反証\n"
        f"{lead.get('falsification_md') or '一次情報で前提が崩れた場合は撤回します。'}\n\n"
        "## 次アクション\n"
        "1. Artifacts の SQL/Python を実行する。\n"
        "2. 価格推移を確認して validation を回す。\n"
        "3. 必要なら対象銘柄を絞って再度 session を投げる。"
    )


def _trim_block(text: str, limit: int) -> str:
    normalized = _clean_text(text)
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: max(0, limit - 1)].rstrip()}…"


def _build_discord_follow_up(
    *,
    session_id: str,
    summary: str,
    hypotheses: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
) -> str:
    session_url = build_web_session_url(session_id)
    lines = [
        "research follow-up",
        f"session={session_id}",
    ]
    if session_url:
        lines.append(f"url={session_url}")
    lines.extend(["", "summary", _trim_block(summary, 600), "", "hypotheses"])
    if hypotheses:
        for idx, item in enumerate(hypotheses[:3], start=1):
            metadata = _as_dict(item.get("metadata"))
            metrics = list(metadata.get("key_metrics") or [])
            lines.extend(
                [
                    f"{idx}. {item.get('stance', '-')} / {item.get('horizon_days', '-')}d / conf={item.get('confidence', '-')}",
                    f"thesis: {_trim_block(str(item.get('thesis_md') or ''), 220)}",
                    f"falsification: {_trim_block(str(item.get('falsification_md') or ''), 160)}",
                    f"validation: {_trim_block(str(metadata.get('validation_plan') or ''), 180)}",
                    f"metrics: {', '.join(str(metric) for metric in metrics[:6]) or '-'}",
                ]
            )
    else:
        lines.append("(none)")
    lines.extend(["", "artifacts"])
    if artifacts:
        for artifact in artifacts[:8]:
            lines.append(f"- {artifact.get('artifact_type', '-')} | {artifact.get('title', '-')}")
    else:
        lines.append("(none)")
    return "\n".join(lines)[:1900]


def _send_discord_follow_up_for_session(
    repo: NeonRepository,
    *,
    payload: dict[str, Any],
    session_id: str,
    summary: str,
) -> None:
    if str(payload.get("requested_by") or "").strip().lower() != "discord":
        return
    channel_id = _clean_text(payload.get("discord_channel_id"))
    if not channel_id:
        return
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    artifacts = repo.fetch_research_artifacts_for_session(session_id)
    secrets = load_runtime_secrets()
    send_bot_message(
        getattr(secrets, "discord_bot_token", None),
        channel_id,
        _build_discord_follow_up(
            session_id=session_id,
            summary=summary,
            hypotheses=hypotheses,
            artifacts=artifacts,
        ),
    )


def _build_discord_chart_message(
    *,
    session_id: str,
    source_title: str,
    charts: list[dict[str, Any]],
) -> str:
    session_url = build_web_session_url(session_id)
    lines = [
        "research charts",
        f"session={session_id}",
        f"source={source_title}",
    ]
    if session_url:
        lines.append(f"url={session_url}")
    for idx, chart in enumerate(charts[:3], start=1):
        lines.extend(
            [
                "",
                f"{idx}. {chart.get('title', '-')}",
                f"kind: {chart.get('kind', '-')}",
                f"summary: {_trim_block(str(chart.get('summary') or ''), 220)}",
            ]
        )
    return "\n".join(lines)[:1900]


def _escape_svg(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _chart_color(idx: int) -> str:
    palette = ["#235789", "#c1292e", "#f1a208", "#3f7d20"]
    return palette[idx % len(palette)]


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    raw = str(color or "").strip().lstrip("#")
    if len(raw) != 6:
        return (0, 0, 0)
    try:
        return (int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16))
    except ValueError:
        return (0, 0, 0)


def _set_pixel(buffer: bytearray, width: int, height: int, x: int, y: int, color: tuple[int, int, int]) -> None:
    if x < 0 or y < 0 or x >= width or y >= height:
        return
    idx = (y * width + x) * 4
    buffer[idx : idx + 4] = bytes((color[0], color[1], color[2], 255))


def _draw_line(
    buffer: bytearray,
    width: int,
    height: int,
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    color: tuple[int, int, int],
) -> None:
    dx = abs(x1 - x0)
    sx = 1 if x0 < x1 else -1
    dy = -abs(y1 - y0)
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    while True:
        _set_pixel(buffer, width, height, x0, y0, color)
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x0 += sx
        if e2 <= dx:
            err += dx
            y0 += sy


def _draw_rect(
    buffer: bytearray,
    width: int,
    height: int,
    x: int,
    y: int,
    rect_width: int,
    rect_height: int,
    color: tuple[int, int, int],
) -> None:
    for yy in range(y, y + rect_height):
        for xx in range(x, x + rect_width):
            _set_pixel(buffer, width, height, xx, yy, color)


def _draw_circle(
    buffer: bytearray,
    width: int,
    height: int,
    cx: int,
    cy: int,
    radius: int,
    color: tuple[int, int, int],
) -> None:
    for y in range(cy - radius, cy + radius + 1):
        for x in range(cx - radius, cx + radius + 1):
            if (x - cx) ** 2 + (y - cy) ** 2 <= radius ** 2:
                _set_pixel(buffer, width, height, x, y, color)


def _blend_pixel(buffer: bytearray, width: int, height: int, x: int, y: int, color: tuple[int, int, int], alpha: float) -> None:
    if x < 0 or y < 0 or x >= width or y >= height:
        return
    idx = (y * width + x) * 4
    existing = buffer[idx : idx + 4]
    out = []
    for i in range(3):
        out.append(int(existing[i] * (1.0 - alpha) + color[i] * alpha))
    buffer[idx : idx + 4] = bytes((out[0], out[1], out[2], 255))


def _build_png_bytes(width: int, height: int, rgba: bytearray) -> bytes:
    header = b"\x89PNG\r\n\x1a\n"

    def _chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack("!I", len(data))
            + tag
            + data
            + struct.pack("!I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    raw = bytearray()
    stride = width * 4
    for y in range(height):
        raw.append(0)
        start = y * stride
        raw.extend(rgba[start : start + stride])
    ihdr = struct.pack("!IIBBBBB", width, height, 8, 6, 0, 0, 0)
    idat = zlib.compress(bytes(raw), level=9)
    return header + _chunk(b"IHDR", ihdr) + _chunk(b"IDAT", idat) + _chunk(b"IEND", b"")


def _build_chart_svg(chart: dict[str, Any], width: int = 960, height: int = 540) -> str | None:
    spec = _normalize_chart_spec(chart)
    if not spec:
        return None
    series = list(spec.get("series") or [])
    if not series:
        return None
    all_y = [float(point[1]) for item in series for point in list(item.get("data") or []) if _coerce_float(point[1]) is not None]
    if len(all_y) < 2:
        return None
    plot_left = 84
    plot_top = 68
    plot_right = width - 36
    plot_bottom = height - 92
    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top
    min_y = min(all_y)
    max_y = max(all_y)
    if min_y == max_y:
        max_y += 1.0

    def _scale_x(index: int, total: int) -> float:
        if total <= 1:
            return float(plot_left)
        return plot_left + (plot_width * index / (total - 1))

    def _scale_y(value: float) -> float:
        return plot_bottom - ((value - min_y) / (max_y - min_y)) * plot_height

    x_labels = [str(point[0]) for point in list(series[0].get("data") or [])]
    grid_lines: list[str] = []
    axis_labels: list[str] = []
    for idx in range(5):
        y_val = min_y + ((max_y - min_y) * idx / 4)
        y = _scale_y(y_val)
        grid_lines.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_right}" y2="{y:.1f}" stroke="#d7dde5" stroke-width="1" />')
        axis_labels.append(
            f'<text x="{plot_left - 12}" y="{y + 4:.1f}" text-anchor="end" font-size="12" fill="#425466">{y_val:.2f}</text>'
        )
    x_tick_count = min(6, len(x_labels))
    for idx in range(x_tick_count):
        source_idx = round(idx * (len(x_labels) - 1) / max(1, x_tick_count - 1))
        x = _scale_x(source_idx, len(x_labels))
        label = _escape_svg(x_labels[source_idx])
        grid_lines.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" stroke="#eef2f6" stroke-width="1" />')
        axis_labels.append(
            f'<text x="{x:.1f}" y="{plot_bottom + 22}" text-anchor="middle" font-size="12" fill="#425466">{label}</text>'
        )

    series_paths: list[str] = []
    legend_items: list[str] = []
    for idx, item in enumerate(series):
        points = list(item.get("data") or [])
        total = len(points)
        if total < 2:
            continue
        color = _chart_color(idx)
        coords = []
        for point_idx, point in enumerate(points):
            y_value = _coerce_float(point[1])
            if y_value is None:
                continue
            coords.append((_scale_x(point_idx, total), _scale_y(y_value)))
        if len(coords) < 2:
            continue
        if spec.get("kind") == "scatter":
            for x, y in coords:
                series_paths.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" opacity="0.9" />')
        else:
            path = " ".join(
                [f"M {coords[0][0]:.1f} {coords[0][1]:.1f}"]
                + [f"L {x:.1f} {y:.1f}" for x, y in coords[1:]]
            )
            if spec.get("kind") == "area":
                area_path = (
                    f"{path} L {coords[-1][0]:.1f} {plot_bottom:.1f} "
                    f"L {coords[0][0]:.1f} {plot_bottom:.1f} Z"
                )
                series_paths.append(f'<path d="{area_path}" fill="{color}" opacity="0.18" />')
            series_paths.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" />')
        legend_items.append(
            f'<g transform="translate({plot_left + idx * 160},{height - 38})">'
            f'<rect x="0" y="-10" width="18" height="4" fill="{color}" rx="2" />'
            f'<text x="26" y="-5" font-size="12" fill="#23313f">{_escape_svg(str(item.get("name") or "series"))}</text>'
            "</g>"
        )

    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            '<rect width="100%" height="100%" fill="#f8fbff" />',
            f'<text x="{plot_left}" y="34" font-size="22" font-weight="700" fill="#1d2a38">{_escape_svg(str(spec.get("title") or "Chart"))}</text>',
            f'<text x="{plot_left}" y="54" font-size="13" fill="#5b6b7b">{_escape_svg(str(spec.get("summary") or ""))}</text>',
            f'<rect x="{plot_left}" y="{plot_top}" width="{plot_width}" height="{plot_height}" fill="#ffffff" stroke="#d7dde5" />',
            *grid_lines,
            *axis_labels,
            f'<text x="{(plot_left + plot_right) / 2:.1f}" y="{height - 12}" text-anchor="middle" font-size="13" fill="#425466">{_escape_svg(str(spec.get("xAxisLabel") or "X"))}</text>',
            f'<text x="24" y="{(plot_top + plot_bottom) / 2:.1f}" text-anchor="middle" font-size="13" fill="#425466" transform="rotate(-90 24 {(plot_top + plot_bottom) / 2:.1f})">{_escape_svg(str(spec.get("yAxisLabel") or "Value"))}</text>',
            *series_paths,
            *legend_items,
            "</svg>",
        ]
    )


def _build_chart_png(chart: dict[str, Any], width: int = 960, height: int = 540) -> bytes | None:
    spec = _normalize_chart_spec(chart)
    if not spec:
        return None
    series = list(spec.get("series") or [])
    if not series:
        return None
    all_y = [float(point[1]) for item in series for point in list(item.get("data") or []) if _coerce_float(point[1]) is not None]
    if len(all_y) < 2:
        return None
    rgba = bytearray([248, 251, 255, 255] * width * height)
    plot_left = 84
    plot_top = 68
    plot_right = width - 36
    plot_bottom = height - 92
    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top
    min_y = min(all_y)
    max_y = max(all_y)
    if min_y == max_y:
        max_y += 1.0

    def _scale_x(index: int, total: int) -> int:
        if total <= 1:
            return plot_left
        return int(round(plot_left + (plot_width * index / (total - 1))))

    def _scale_y(value: float) -> int:
        return int(round(plot_bottom - ((value - min_y) / (max_y - min_y)) * plot_height))

    _draw_rect(rgba, width, height, plot_left, plot_top, plot_width, plot_height, (255, 255, 255))
    border = (215, 221, 229)
    _draw_line(rgba, width, height, plot_left, plot_top, plot_right, plot_top, border)
    _draw_line(rgba, width, height, plot_left, plot_bottom, plot_right, plot_bottom, border)
    _draw_line(rgba, width, height, plot_left, plot_top, plot_left, plot_bottom, border)
    _draw_line(rgba, width, height, plot_right, plot_top, plot_right, plot_bottom, border)

    for idx in range(5):
        y_val = min_y + ((max_y - min_y) * idx / 4)
        y = _scale_y(y_val)
        _draw_line(rgba, width, height, plot_left, y, plot_right, y, (225, 232, 238))

    first_series_points = list(series[0].get("data") or [])
    x_tick_count = min(6, len(first_series_points))
    for idx in range(x_tick_count):
        source_idx = round(idx * (len(first_series_points) - 1) / max(1, x_tick_count - 1))
        x = _scale_x(source_idx, len(first_series_points))
        _draw_line(rgba, width, height, x, plot_top, x, plot_bottom, (238, 242, 246))

    for idx, item in enumerate(series):
        points = list(item.get("data") or [])
        total = len(points)
        if total < 2:
            continue
        color = _hex_to_rgb(_chart_color(idx))
        coords: list[tuple[int, int]] = []
        for point_idx, point in enumerate(points):
            y_value = _coerce_float(point[1])
            if y_value is None:
                continue
            coords.append((_scale_x(point_idx, total), _scale_y(y_value)))
        if len(coords) < 2:
            continue
        kind = str(spec.get("kind") or "line")
        if kind == "bar":
            baseline = _scale_y(0.0 if min_y <= 0.0 <= max_y else min_y)
            bar_half = max(3, int(plot_width / max(20, len(coords) * 4)))
            for x, y in coords:
                top = min(y, baseline)
                bottom = max(y, baseline)
                _draw_rect(rgba, width, height, x - bar_half, top, bar_half * 2, max(1, bottom - top), color)
        elif kind == "scatter":
            for x, y in coords:
                _draw_circle(rgba, width, height, x, y, 4, color)
        else:
            if kind == "area":
                for x, y in coords:
                    for yy in range(y, plot_bottom):
                        _blend_pixel(rgba, width, height, x, yy, color, 0.12)
            for start, end in zip(coords, coords[1:]):
                _draw_line(rgba, width, height, start[0], start[1], end[0], end[1], color)

    return _build_png_bytes(width, height, rgba)


def _send_discord_chart_follow_up(
    *,
    payload: dict[str, Any],
    session_id: str,
    source_title: str,
    charts: list[dict[str, Any]],
) -> None:
    if str(payload.get("requested_by") or "").strip().lower() != "discord":
        return
    channel_id = _clean_text(payload.get("discord_channel_id"))
    if not channel_id or not charts:
        return
    secrets = load_runtime_secrets()
    send_bot_message(
        getattr(secrets, "discord_bot_token", None),
        channel_id,
        _build_discord_chart_message(session_id=session_id, source_title=source_title, charts=charts),
    )
    for idx, chart in enumerate(charts[:3], start=1):
        png = _build_chart_png(chart)
        if not png:
            continue
        send_bot_file(
            getattr(secrets, "discord_bot_token", None),
            channel_id,
            filename=f"research-chart-{idx}.png",
            content=png,
            message=f"{chart.get('title', 'chart')} ({chart.get('kind', '-')})",
            content_type="image/png",
        )


def _build_portfolio_note(hypotheses: list[dict[str, Any]]) -> tuple[str, dict[str, float]]:
    bullish = sum(1 for item in hypotheses if str(item.get("stance")) == "bullish")
    bearish = sum(1 for item in hypotheses if str(item.get("stance")) == "bearish")
    watch = sum(1 for item in hypotheses if str(item.get("stance")) == "watch")
    total = max(1, len(hypotheses))
    risky = bullish + bearish
    cash = 0.6 if risky <= 1 else 0.35 if risky == 2 else 0.2
    active = max(0.0, 1.0 - cash)
    long_weight = round(active * bullish / max(1, risky), 4) if bullish > 0 else 0.0
    short_weight = round(active * bearish / max(1, risky), 4) if bearish > 0 else 0.0
    weights = {
        "long_per_hypothesis": long_weight,
        "short_per_hypothesis": short_weight,
        "cash": round(cash, 4),
        "watch_budget": round(0.05 if watch > 0 else 0.0, 4),
    }
    body = (
        f"- hypotheses: {total}\n"
        f"- bullish: {bullish}\n"
        f"- bearish: {bearish}\n"
        f"- watch: {watch}\n"
        f"- suggested cash: {weights['cash']:.2%}\n"
        f"- long per bullish hypothesis: {weights['long_per_hypothesis']:.2%}\n"
        f"- short per bearish hypothesis: {weights['short_per_hypothesis']:.2%}"
    )
    return body, weights


def _build_critic_note(hypotheses: list[dict[str, Any]]) -> str:
    lines = []
    for idx, item in enumerate(hypotheses, start=1):
        thesis = _clean_text(item.get("thesis_md"))
        lines.append(f"{idx}. 対象仮説: {thesis[:140]}")
        lines.append("   - リスク: 対象銘柄・ドライバー・観測期間の特定がまだ粗いです。")
        lines.append("   - 追加確認: 決算、ガイダンス、価格反応、出来高、一次情報 citation。")
        lines.append("   - バイアス: テーマ先行で価格確認が後追いになっている可能性があります。")
    return "\n".join(lines) if lines else "仮説がないため critic review を生成できません。"


def _build_quant_sql(symbol_text: str | None) -> str:
    if symbol_text:
        return (
            "with target as (\n"
            "  select id\n"
            "  from securities\n"
            f"  where security_id = '{symbol_text}' or upper(ticker) = upper('{symbol_text.split(':', 1)[-1]}')\n"
            "  limit 1\n"
            ")\n"
            "select p.trade_date, p.close_raw\n"
            "from prices_daily p\n"
            "join target t on t.id = p.security_id\n"
            "order by p.trade_date desc\n"
            "limit 60;"
        )
    return "select current_date as as_of_date;"


def _build_python_template(symbol_text: str | None) -> str:
    label = symbol_text or "TARGET"
    return (
        "import math\n\n"
        f"symbol = {label!r}\n"
        "returns = [0.01, -0.005, 0.012, 0.004]\n"
        "mean_ret = sum(returns) / len(returns)\n"
        "variance = sum((x - mean_ret) ** 2 for x in returns) / len(returns)\n"
        "vol = math.sqrt(variance)\n"
        "print({'symbol': symbol, 'mean_return': round(mean_ret, 6), 'volatility': round(vol, 6)})\n"
    )


def _is_sql_safe(sql_text: str) -> bool:
    normalized = sql_text.strip().lower()
    if not normalized.startswith(("select", "with")):
        return False
    return not any(re.search(pattern, normalized) for pattern in FORBIDDEN_SQL_PATTERNS)


def _execute_readonly_sql(repo: NeonRepository, sql_text: str) -> dict[str, Any]:
    if not _is_sql_safe(sql_text):
        raise ValueError("only read-only SELECT/WITH SQL is allowed")
    with repo._conn() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(sql_text)
        rows = cur.fetchmany(200)
        columns = [str(col.name) for col in list(cur.description or [])]
    return {
        "columns": columns,
        "row_count": len(rows),
        "rows": _json_safe(rows),
    }


def _execute_python(code_text: str) -> dict[str, Any]:
    stdout_buffer = io.StringIO()
    globals_dict = {
        "__builtins__": {
            "print": print,
            "len": len,
            "range": range,
            "min": min,
            "max": max,
            "sum": sum,
            "sorted": sorted,
            "abs": abs,
            "round": round,
        }
    }
    with redirect_stdout(stdout_buffer):
        exec(compile(code_text, "<research-artifact>", "exec"), globals_dict, {})
    return {
        "stdout": stdout_buffer.getvalue(),
        "globals": sorted(key for key in globals_dict.keys() if not key.startswith("__")),
    }


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_chart_spec(raw: dict[str, Any]) -> dict[str, Any] | None:
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in {"line", "bar", "scatter", "area"}:
        return None
    series_payload = raw.get("series")
    if not isinstance(series_payload, list) or not series_payload:
        return None
    normalized_series: list[dict[str, Any]] = []
    for entry in series_payload[:4]:
        if not isinstance(entry, dict):
            continue
        points = entry.get("data")
        if not isinstance(points, list):
            continue
        normalized_points: list[list[Any]] = []
        for point in points[:200]:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            y_value = _coerce_float(point[1])
            x_value = _clean_text(point[0])
            if y_value is None or not x_value:
                continue
            normalized_points.append([x_value, y_value])
        if len(normalized_points) < 2:
            continue
        normalized_series.append(
            {
                "name": _clean_text(entry.get("name")) or "series",
                "data": normalized_points,
            }
        )
    if not normalized_series:
        return None
    return {
        "title": _clean_text(raw.get("title")) or "Generated Chart",
        "kind": kind,
        "xAxisLabel": _clean_text(raw.get("x_axis_label") or raw.get("xAxisLabel")) or "X",
        "yAxisLabel": _clean_text(raw.get("y_axis_label") or raw.get("yAxisLabel")) or "Value",
        "summary": _clean_text(raw.get("summary")) or "",
        "series": normalized_series,
    }


def _build_chart_planning_prompt(
    *,
    artifact: dict[str, Any],
    result: dict[str, Any],
    chart_type: str | None = None,
    instruction: str | None = None,
) -> str:
    result_preview = {
        "columns": result.get("columns"),
        "row_count": result.get("row_count"),
        "rows": list(result.get("rows") or [])[:20],
    }
    return "\n".join(
        [
            "あなたは buy-side のデータ可視化リサーチャーです。",
            "与えられた artifact 実行結果から、検証価値の高い chart を 1-3 個だけ提案してください。",
            "見た目よりも、投資判断に効く比較・変化・異常値・トレンドを優先してください。",
            "chart は固定ではなく、データの列構造を見て最適な種類を選んでください。",
            "kind は line/bar/scatter/area のみ使えます。",
            "返答は JSON のみ。コードフェンスは禁止。",
            "",
            f"artifact_type: {artifact.get('artifact_type', '-')}",
            f"title: {artifact.get('title', '-')}",
            f"body: {_trim_block(str(artifact.get('body_md') or ''), 400)}",
            f"code: {_trim_block(str(artifact.get('code_text') or ''), 800)}",
            f"preferred_chart_type: {_clean_text(chart_type) or 'auto'}",
            f"user_instruction: {_trim_block(str(instruction or ''), 300) or '(none)'}",
            "",
            "result preview:",
            json.dumps(result_preview, ensure_ascii=False),
            "",
            "要件:",
            "- x 軸と y 軸の意味が自然になるように選ぶこと。",
            "- 同じデータから別観点の chart を作ってよい。",
            "- summary にはその chart で何を確認したいかを1-2文で書くこと。",
            "- series.data は [x, y] 形式にすること。",
            "- preferred_chart_type が auto でなければ、その意図を優先して構成すること。",
            "- user_instruction があれば、その意図を尊重して chart の観点を選ぶこと。",
        ]
    )


def _fallback_chart_specs_from_sql_result(
    result: dict[str, Any],
    title: str,
    *,
    preferred_chart_type: str | None = None,
    instruction: str | None = None,
) -> list[dict[str, Any]]:
    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        return []
    points: list[list[Any]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        y_value = _coerce_float(row[1])
        if y_value is None:
            continue
        x_value = str(row[0])
        points.append([x_value, y_value])
    if len(points) < 2:
        return []
    pct_points: list[list[Any]] = []
    first = _coerce_float(points[0][1])
    if first not in {None, 0.0}:
        for x_value, y_value in points:
            pct_points.append([x_value, ((float(y_value) / float(first)) - 1.0) * 100.0])
    charts = [
        {
            "title": title,
            "kind": "line",
            "xAxisLabel": str((result.get("columns") or ["X"])[0] if isinstance(result.get("columns"), list) and result.get("columns") else "X"),
            "yAxisLabel": str((result.get("columns") or ["X", "Value"])[1] if isinstance(result.get("columns"), list) and len(result.get("columns") or []) > 1 else "Value"),
            "summary": "原系列の水準推移を確認する。",
            "series": [
                {
                    "name": "series_1",
                    "data": points,
                }
            ],
        }
    ]
    if len(pct_points) >= 2:
        charts.append(
            {
                "title": f"{title} (% change)",
                "kind": "bar",
                "xAxisLabel": str((result.get("columns") or ["X"])[0] if isinstance(result.get("columns"), list) and result.get("columns") else "X"),
                "yAxisLabel": "% change from first point",
                "summary": "初期点からの変化率でトレンドの強さを確認する。",
                "series": [
                    {
                        "name": "pct_change",
                        "data": pct_points,
                    }
                ],
            }
        )
    preferred = _clean_text(preferred_chart_type).lower()
    instruction_text = _clean_text(instruction).lower()
    if preferred in {"scatter"}:
        charts.insert(
            0,
            {
                "title": f"{title} (scatter)",
                "kind": "scatter",
                "xAxisLabel": str((result.get("columns") or ["X"])[0] if isinstance(result.get("columns"), list) and result.get("columns") else "X"),
                "yAxisLabel": str((result.get("columns") or ["X", "Value"])[1] if isinstance(result.get("columns"), list) and len(result.get("columns") or []) > 1 else "Value"),
                "summary": "点の散らばりと外れ値を確認する。",
                "series": [{"name": "scatter", "data": points}],
            },
        )
    elif preferred in {"bar_compare", "volume"} or "出来高" in instruction_text:
        charts[0]["kind"] = "bar"
        charts[0]["summary"] = "棒グラフで水準差を確認する。"
    elif preferred in {"cumulative_return", "relative_return"}:
        if len(pct_points) >= 2:
            charts.insert(0, charts.pop(1))
    return charts


def _fallback_chart_specs_from_python_result(
    result: dict[str, Any],
    title: str,
    *,
    preferred_chart_type: str | None = None,
    instruction: str | None = None,
) -> list[dict[str, Any]]:
    payload = result.get("chart")
    if not isinstance(payload, dict):
        return []
    normalized = _normalize_chart_spec(
        {
            "title": str(payload.get("title") or title),
            "kind": str(payload.get("kind") or "line"),
            "xAxisLabel": str(payload.get("xAxisLabel") or "X"),
            "yAxisLabel": str(payload.get("yAxisLabel") or "Value"),
            "summary": str(payload.get("summary") or "Python artifact が返した可視化データ。"),
            "series": payload.get("series"),
        }
    )
    if not normalized:
        return []
    preferred = _clean_text(preferred_chart_type).lower()
    instruction_text = _clean_text(instruction).lower()
    if preferred in {"scatter", "bar_compare"}:
        normalized["kind"] = "scatter" if preferred == "scatter" else "bar"
    elif "エリア" in instruction_text or preferred == "cumulative_return":
        normalized["kind"] = "area"
    return [normalized]


def _plan_chart_specs_via_llm(
    artifact: dict[str, Any],
    result: dict[str, Any],
    *,
    chart_type: str | None = None,
    instruction: str | None = None,
) -> list[dict[str, Any]]:
    try:
        secrets = load_runtime_secrets()
    except RuntimeError:
        return []
    api_key = str(getattr(secrets, "openai_api_key", "") or "").strip()
    if not api_key:
        return []
    prompt = _build_chart_planning_prompt(
        artifact=artifact,
        result=result,
        chart_type=chart_type,
        instruction=instruction,
    )
    try:
        payload = request_openai_json(
            prompt=prompt,
            api_key=api_key,
            model=_resolve_openai_model(),
            json_schema=RESEARCH_CHART_PLAN_JSON_SCHEMA,
            max_output_tokens=1600,
        )
    except (OpenAIClientError, requests.RequestException, RuntimeError):
        return []
    charts = payload.get("charts")
    if not isinstance(charts, list):
        return []
    out: list[dict[str, Any]] = []
    for chart in charts:
        if not isinstance(chart, dict):
            continue
        normalized = _normalize_chart_spec(chart)
        if normalized:
            out.append(normalized)
    return out


def _create_chart_artifacts_from_run(
    repo: NeonRepository,
    *,
    artifact: dict[str, Any],
    result: dict[str, Any],
    run_id: str,
    chart_type: str | None = None,
    instruction: str | None = None,
) -> list[dict[str, Any]]:
    artifact_type = str(artifact.get("artifact_type") or "").strip().lower()
    source_title = str(artifact.get("title") or "Artifact").strip() or "Artifact"
    chart_specs: list[dict[str, Any]] = _plan_chart_specs_via_llm(
        artifact,
        result,
        chart_type=chart_type,
        instruction=instruction,
    )
    if not chart_specs:
        if artifact_type == "sql":
            chart_specs = _fallback_chart_specs_from_sql_result(
                result,
                source_title,
                preferred_chart_type=chart_type,
                instruction=instruction,
            )
        elif artifact_type == "python":
            chart_specs = _fallback_chart_specs_from_python_result(
                result,
                source_title,
                preferred_chart_type=chart_type,
                instruction=instruction,
            )
    created: list[dict[str, Any]] = []
    for chart_spec in chart_specs[:3]:
        artifact_id = repo.insert_research_artifact(
            ResearchArtifactSpec(
                session_id=str(artifact.get("session_id") or ""),
                hypothesis_id=str(artifact.get("hypothesis_id") or "") or None,
                artifact_type="chart",
                title=f"Chart: {chart_spec.get('title') or source_title}",
                body_md=str(chart_spec.get("summary") or f"Auto-generated chart from {artifact_type} artifact run."),
                metadata={
                    "processor": "research.chart_generate",
                    "source_artifact_id": str(artifact.get("id") or ""),
                    "source_run_id": run_id,
                    "chart_spec": chart_spec,
                },
            )
        )
        created.append(
            {
                "id": artifact_id,
                "title": str(chart_spec.get("title") or source_title),
                "summary": str(chart_spec.get("summary") or ""),
                "kind": str(chart_spec.get("kind") or ""),
            }
        )
    return created


def _select_primary_symbol(assets: list[dict[str, Any]]) -> str | None:
    if not assets:
        return None
    first = assets[0]
    return _clean_text(first.get("security_id") or first.get("symbol_text") or first.get("ticker")) or None


def _resolve_entry_and_returns(
    repo: NeonRepository,
    symbol: str,
    created_at: datetime,
) -> dict[str, Any] | None:
    resolved = repo.fetch_latest_price_for_symbol(symbol)
    if not resolved:
        return None
    security_id = str(resolved.get("security_id", "")).strip()
    if not security_id:
        return None
    start = created_at.date() - timedelta(days=5)
    end = created_at.date() + timedelta(days=35)
    history = repo.fetch_price_history_for_security(security_id, start, end)
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
        if len(closes) <= offset:
            return None
        return closes[offset] / entry_price - 1.0

    return {
        "security_id": security_id,
        "entry_date": trade_dates[0],
        "entry_price": entry_price,
        "ret_1d": _ret_at(1),
        "ret_5d": _ret_at(5),
        "ret_20d": _ret_at(20),
        "mfe": max(closes) / entry_price - 1.0,
        "mae": min(closes) / entry_price - 1.0,
    }


def _label_outcome(stance: str, ret_5d: float | None) -> str:
    if ret_5d is None:
        return "open"
    if stance == "bullish":
        return "hit" if ret_5d > 0 else "miss"
    if stance == "bearish":
        return "hit" if ret_5d < 0 else "miss"
    if abs(ret_5d) < 0.02:
        return "partial"
    return "miss"


def _process_extract_input(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    inputs = repo.fetch_research_external_inputs(session_id=session_id)
    updated = 0
    fetched = 0
    failed = 0
    for item in inputs:
        extraction_status = str(item.get("extraction_status", "queued"))
        if extraction_status == "success" and _clean_text(item.get("extracted_text")):
            continue
        source_type = str(item.get("source_type", "text"))
        source_url = _clean_text(item.get("source_url"))
        raw_text = _clean_text(item.get("raw_text"))
        if source_type in {"web_url", "youtube", "x"} and source_url:
            try:
                fetched_payload = _fetch_url_excerpt(source_url)
                extracted = "\n".join(
                    part for part in [
                        fetched_payload.get("title") or "",
                        fetched_payload.get("excerpt") or "",
                    ] if part
                )
                repo.update_research_external_input(
                    str(item["id"]),
                    extracted_text=extracted,
                    quality_grade="A" if fetched_payload.get("excerpt") else "B",
                    extraction_status="success" if fetched_payload.get("excerpt") else "partial",
                    metadata_patch={
                        "processed_at": _utc_now().isoformat(),
                        "processor": "research.extract_input",
                        "fetch": fetched_payload,
                    },
                )
                fetched += 1
            except requests.RequestException as exc:
                repo.update_research_external_input(
                    str(item["id"]),
                    extracted_text=f"Fetch failed for {source_url}",
                    quality_grade="C",
                    extraction_status="failed",
                    metadata_patch={
                        "processed_at": _utc_now().isoformat(),
                        "processor": "research.extract_input",
                        "error": str(exc),
                    },
                )
                failed += 1
        else:
            repo.update_research_external_input(
                str(item["id"]),
                extracted_text=raw_text or None,
                quality_grade="B" if raw_text else None,
                extraction_status="success" if raw_text else "partial",
                metadata_patch={"processed_at": _utc_now().isoformat(), "processor": "research.extract_input"},
            )
        updated += 1
    return {"updated_inputs": updated, "fetched_urls": fetched, "failed_urls": failed}


def _process_generate_hypothesis(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    hypothesis_ids = [str(item) for item in (payload.get("hypothesis_ids") or [])]
    hypotheses = repo.fetch_research_hypotheses_by_ids(hypothesis_ids)
    session_id = str(payload.get("session_id", "")).strip()
    inputs = repo.fetch_research_external_inputs(session_id)
    urls, texts = _split_urls_and_text(inputs)
    url_summaries: list[dict[str, Any]] = []
    for item in inputs:
        source_url = _clean_text(item.get("source_url"))
        metadata = _as_dict(item.get("metadata"))
        fetch_payload = _as_dict(metadata.get("fetch"))
        if source_url:
            url_summaries.append(
                {
                    "url": source_url,
                    "title": _clean_text(fetch_payload.get("title")),
                    "excerpt": _clean_text(item.get("extracted_text"))[:2000],
                }
            )
    generated = _generate_hypotheses_via_llm(
        question=str(payload.get("question") or ""),
        security_id=_clean_text(payload.get("security_id")) or None,
        urls=urls,
        texts=texts,
        url_summaries=url_summaries,
    )
    generated_hypotheses = list(generated.get("hypotheses") or [])
    generated_mode = str(generated.get("mode") or "")
    updated = 0
    for idx, hypothesis in enumerate(hypotheses):
        candidate = generated_hypotheses[idx] if idx < len(generated_hypotheses) and isinstance(generated_hypotheses[idx], dict) else {}
        repo.update_research_hypothesis(
            str(hypothesis["id"]),
            status="validate",
            thesis_md=str(candidate.get("thesis_md") or hypothesis.get("thesis_md") or ""),
            falsification_md=str(candidate.get("falsification_md") or hypothesis.get("falsification_md") or ""),
            confidence=float(candidate.get("confidence")) if candidate.get("confidence") is not None else hypothesis.get("confidence"),
            metadata_patch={
                "generated_at": _utc_now().isoformat(),
                "processor": "research.generate_hypothesis",
                "mode": generated_mode,
                "llm_summary": str(generated.get("summary") or ""),
                "suggested_stance": candidate.get("stance"),
                "suggested_horizon_days": candidate.get("horizon_days"),
                "validation_plan": candidate.get("validation_plan"),
                "key_metrics": list(candidate.get("key_metrics") or []),
            },
        )
        updated += 1
    hypothesis_lines = []
    for idx, candidate in enumerate(generated_hypotheses[:3], start=1):
        if not isinstance(candidate, dict):
            continue
        metrics = ", ".join(str(item) for item in list(candidate.get("key_metrics") or [])[:6])
        hypothesis_lines.append(
            "\n".join(
                [
                    f"### Hypothesis {idx}",
                    f"- stance: {candidate.get('stance', '-')}",
                    f"- horizon_days: {candidate.get('horizon_days', '-')}",
                    f"- thesis: {candidate.get('thesis_md', '-')}",
                    f"- falsification: {candidate.get('falsification_md', '-')}",
                    f"- validation_plan: {candidate.get('validation_plan', '-')}",
                    f"- key_metrics: {metrics or '-'}",
                ]
            )
        )
    summary_header = str(generated.get("summary") or " / ".join(texts[:3])[:1200] or "No extracted text yet.")
    summary = "\n\n".join([summary_header, *hypothesis_lines]) if hypothesis_lines else summary_header
    artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=hypothesis_ids[0] if hypothesis_ids else None,
            artifact_type="report",
            title="Generated Hypothesis Pack",
            body_md=summary,
            metadata={
                "processor": "research.generate_hypothesis",
                "llm_used": bool(getattr(load_runtime_secrets(), "openai_api_key", None)),
                "url_count": len(urls),
                "mode": generated_mode,
            },
        )
    )
    return {"updated_hypotheses": updated, "artifact_id": artifact_id, "summary": summary}


def _process_critic_review(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None,
            artifact_type="note",
            title="Critic Review",
            body_md=_build_critic_note(hypotheses),
            metadata={"processor": "research.critic_review"},
        )
    )
    return {"artifact_id": artifact_id, "hypothesis_count": len(hypotheses)}


def _process_quant_plan(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    assets = repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses])
    symbol = _select_primary_symbol(assets)
    artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None,
            artifact_type="sql",
            title="Quant Validation SQL",
            code_text=_build_quant_sql(symbol),
            language="sql",
            metadata={"processor": "research.quant_plan", "symbol": symbol},
        )
    )
    repo.enqueue_agent_task(
        task_type="research.artifact_run",
        payload={
            "session_id": session_id,
            "artifact_id": artifact_id,
            "requested_by": "research.quant_plan",
        },
        session_id=session_id,
        assigned_role="artifact",
        dedupe_key=f"{session_id}:artifact_run:{artifact_id}",
    )
    return {"artifact_id": artifact_id, "symbol": symbol, "auto_run_enqueued": True}


def _process_code_generate(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    assets = repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses])
    symbol = _select_primary_symbol(assets)
    artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None,
            artifact_type="python",
            title="Python Analysis Draft",
            code_text=_build_python_template(symbol),
            language="python",
            metadata={"processor": "research.code_generate", "symbol": symbol},
        )
    )
    repo.enqueue_agent_task(
        task_type="research.artifact_run",
        payload={
            "session_id": session_id,
            "artifact_id": artifact_id,
            "requested_by": "research.code_generate",
        },
        session_id=session_id,
        assigned_role="artifact",
        dedupe_key=f"{session_id}:artifact_run:{artifact_id}",
    )
    return {"artifact_id": artifact_id, "symbol": symbol, "auto_run_enqueued": True}


def _process_portfolio_build(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    inputs = repo.fetch_research_external_inputs(session_id)
    urls, _ = _split_urls_and_text(inputs)
    note, weights = _build_portfolio_note(hypotheses)
    artifact_id = repo.insert_research_artifact(
        ResearchArtifactSpec(
            session_id=session_id,
            hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None,
            artifact_type="report",
            title="Portfolio Suggestion",
            body_md=note,
            metadata={"processor": "research.portfolio_build", "weights": weights},
        )
    )
    previous = repo.fetch_latest_chat_message(session_id, role="assistant")
    summary = _build_session_summary(hypotheses, urls)
    repo.append_chat_message(
        session_id=session_id,
        role="assistant",
        content=summary,
        answer_before=str(previous.get("content")) if previous else None,
        answer_after=summary,
        change_reason="research portfolio build completed",
    )
    _send_discord_follow_up_for_session(repo, payload=payload, session_id=session_id, summary=summary)
    return {"artifact_id": artifact_id, "weights": weights}


def _process_artifact_run(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    artifact_id = str(payload.get("artifact_id", "")).strip()
    artifact = repo.fetch_research_artifact(artifact_id)
    if not artifact:
        raise ValueError(f"artifact not found: {artifact_id}")
    artifact_type = str(artifact.get("artifact_type", "")).strip().lower()
    code_text = str(artifact.get("code_text") or "")
    if artifact_type == "sql":
        result = _execute_readonly_sql(repo, code_text)
        run_id = repo.insert_research_artifact_run(
            ResearchArtifactRunSpec(
                artifact_id=artifact_id,
                run_status="success",
                stdout_text=json.dumps({"row_count": result["row_count"]}, ensure_ascii=False),
                result_json=result,
            )
        )
        chart_artifacts = _create_chart_artifacts_from_run(
            repo,
            artifact=artifact,
            result=result,
            run_id=run_id,
            chart_type=str(payload.get("chart_type") or "") or None,
            instruction=str(payload.get("chart_instruction") or "") or None,
        )
        _send_discord_chart_follow_up(
            payload=payload,
            session_id=str(artifact.get("session_id") or ""),
            source_title=str(artifact.get("title") or artifact_id),
            charts=chart_artifacts,
        )
        return {
            "run_id": run_id,
            "row_count": result["row_count"],
            "chart_artifact_ids": [str(item.get("id") or "") for item in chart_artifacts],
        }
    if artifact_type == "python":
        result = _execute_python(code_text)
        run_id = repo.insert_research_artifact_run(
            ResearchArtifactRunSpec(
                artifact_id=artifact_id,
                run_status="success",
                stdout_text=str(result.get("stdout", "")),
                result_json=result,
            )
        )
        chart_artifacts = _create_chart_artifacts_from_run(
            repo,
            artifact=artifact,
            result=result,
            run_id=run_id,
            chart_type=str(payload.get("chart_type") or "") or None,
            instruction=str(payload.get("chart_instruction") or "") or None,
        )
        _send_discord_chart_follow_up(
            payload=payload,
            session_id=str(artifact.get("session_id") or ""),
            source_title=str(artifact.get("title") or artifact_id),
            charts=chart_artifacts,
        )
        return {
            "run_id": run_id,
            "stdout": result.get("stdout", ""),
            "chart_artifact_ids": [str(item.get("id") or "") for item in chart_artifacts],
        }
    run_id = repo.insert_research_artifact_run(
        ResearchArtifactRunSpec(
            artifact_id=artifact_id,
            run_status="failed",
            stderr_text=f"artifact_type_not_supported: {artifact_type}",
            result_json={"artifact_type": artifact_type},
        )
    )
    return {"run_id": run_id, "error": f"unsupported artifact type: {artifact_type}"}


def _process_chart_generate(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    artifact_id = str(payload.get("artifact_id", "")).strip()
    artifact = repo.fetch_research_artifact(artifact_id)
    if not artifact:
        raise ValueError(f"artifact not found: {artifact_id}")
    latest_run = repo.fetch_latest_research_artifact_run(artifact_id)
    if not latest_run:
        raise ValueError(f"latest artifact run not found: {artifact_id}")
    if str(latest_run.get("run_status") or "").strip().lower() != "success":
        raise ValueError(f"latest artifact run is not successful: {artifact_id}")
    result = _as_dict(latest_run.get("result_json"))
    chart_artifacts = _create_chart_artifacts_from_run(
        repo,
        artifact=artifact,
        result=result,
        run_id=str(latest_run.get("id") or ""),
        chart_type=str(payload.get("chart_type") or "") or None,
        instruction=str(payload.get("chart_instruction") or "") or None,
    )
    _send_discord_chart_follow_up(
        payload=payload,
        session_id=str(artifact.get("session_id") or ""),
        source_title=str(artifact.get("title") or artifact_id),
        charts=chart_artifacts,
    )
    return {
        "source_artifact_id": artifact_id,
        "source_run_id": str(latest_run.get("id") or ""),
        "chart_artifact_ids": [str(item.get("id") or "") for item in chart_artifacts],
    }


def _process_validate_outcome(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    requested_ids = [str(item) for item in (payload.get("hypothesis_ids") or [])]
    hypotheses = repo.fetch_research_hypotheses_by_ids(requested_ids) if requested_ids else repo.fetch_research_hypotheses_for_session(session_id)
    assets = repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses])
    assets_by_hypothesis: dict[str, list[dict[str, Any]]] = {}
    for asset in assets:
        assets_by_hypothesis.setdefault(str(asset["hypothesis_id"]), []).append(asset)
    created = 0
    for hypothesis in hypotheses:
        hypothesis_id = str(hypothesis["id"])
        symbol = _select_primary_symbol(assets_by_hypothesis.get(hypothesis_id, []))
        if not symbol:
            continue
        created_at = hypothesis.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        metrics = _resolve_entry_and_returns(repo, symbol, created_at)
        if not metrics:
            repo.update_research_hypothesis(
                hypothesis_id,
                status="watch",
                metadata_patch={"validation_status": "open", "validator": "research.validate_outcome"},
            )
            continue
        label = _label_outcome(str(hypothesis.get("stance", "watch")), metrics.get("ret_5d"))
        repo.insert_research_hypothesis_outcome(
            ResearchHypothesisOutcomeSpec(
                hypothesis_id=hypothesis_id,
                checked_at=_utc_now(),
                ret_1d=metrics.get("ret_1d"),
                ret_5d=metrics.get("ret_5d"),
                ret_20d=metrics.get("ret_20d"),
                mfe=metrics.get("mfe"),
                mae=metrics.get("mae"),
                outcome_label=label,
                summary_md=f"Validated against {metrics.get('security_id')} from {metrics.get('entry_date')}.",
                metadata={"symbol": symbol, "validator": "research.validate_outcome"},
            )
        )
        repo.update_research_hypothesis(
            hypothesis_id,
            status="passed" if label == "hit" else "failed" if label == "miss" else "watch",
            metadata_patch={"validation_status": label, "validated_symbol": symbol},
        )
        created += 1
    return {"validated": created}


def _process_session_summarize(repo: NeonRepository, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    inputs = repo.fetch_research_external_inputs(session_id)
    urls, _ = _split_urls_and_text(inputs)
    previous = repo.fetch_latest_chat_message(session_id, role="assistant")
    summary = _build_session_summary(hypotheses, urls)
    message_id = repo.append_chat_message(
        session_id=session_id,
        role="assistant",
        content=summary,
        answer_before=str(previous.get("content")) if previous else None,
        answer_after=summary,
        change_reason="research session summarized",
    )
    return {"message_id": message_id}


def process_research_task(repo: NeonRepository, task: dict[str, Any]) -> dict[str, Any]:
    payload = _as_dict(task.get("payload"))
    task_type = str(task.get("task_type", "")).strip()
    if task_type == "research.extract_input":
        return _process_extract_input(repo, payload)
    if task_type == "research.generate_hypothesis":
        return _process_generate_hypothesis(repo, payload)
    if task_type == "research.critic_review":
        return _process_critic_review(repo, payload)
    if task_type == "research.quant_plan":
        return _process_quant_plan(repo, payload)
    if task_type == "research.code_generate":
        return _process_code_generate(repo, payload)
    if task_type == "research.portfolio_build":
        return _process_portfolio_build(repo, payload)
    if task_type == "research.artifact_run":
        return _process_artifact_run(repo, payload)
    if task_type == "research.chart_generate":
        return _process_chart_generate(repo, payload)
    if task_type == "research.validate_outcome":
        return _process_validate_outcome(repo, payload)
    if task_type == "research.session_summarize":
        return _process_session_summarize(repo, payload)
    return {"ignored": True, "task_type": task_type}


def run_research_chat_once(limit: int = 20, assigned_role: str | None = None) -> dict[str, int]:
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    tasks = repo.fetch_queued_agent_tasks(limit=limit, task_types=list(SUPPORTED_TASK_TYPES), assigned_role=assigned_role)
    stats = {"queued": len(tasks), "processed": 0, "success": 0, "failed": 0}
    for task in tasks:
        task_id = str(task["id"])
        stats["processed"] += 1
        try:
            repo.mark_agent_task(task_id=task_id, status="running")
            result = process_research_task(repo, task)
            repo.mark_agent_task(task_id=task_id, status="success", result=result, cost_usd=0.0)
            stats["success"] += 1
        except Exception as exc:  # noqa: BLE001
            repo.mark_agent_task(
                task_id=task_id,
                status="failed",
                result={"error": str(exc), "task_type": str(task.get("task_type", ""))},
                cost_usd=0.0,
                error_text=str(exc),
            )
            stats["failed"] += 1
    return stats


def run_research_chat(limit: int | None = None) -> dict[str, int]:
    cfg = load_yaml_config()
    runtime_cfg = _resolve_runtime_cfg(cfg)
    batch_size = max(1, int(limit or runtime_cfg["batch_size"]))
    poll_interval_sec = float(runtime_cfg["poll_interval_sec"])
    while True:
        summary = run_research_chat_once(limit=batch_size)
        print(f"job=research_chat summary={summary}", flush=True)
        time.sleep(poll_interval_sec)
