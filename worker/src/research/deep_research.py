from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Any

from src.llm.openai_client import DEFAULT_OPENAI_MODEL, request_openai_json

DEEP_RESEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rating", "summary", "drivers", "catalysts", "risks"],
    "properties": {
        "rating": {"type": "string", "enum": ["A", "B", "C"]},
        "summary": {"type": "string", "minLength": 1, "maxLength": 1500},
        "drivers": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
        "catalysts": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
        "risks": {"type": "array", "items": {"type": "string"}, "maxItems": 12},
    },
}


@dataclass(frozen=True)
class DeepResearchInput:
    security_id: str
    report_text: str
    source: str = "deep_research"


def _resolve_model(model: str | None) -> str:
    candidate = (model or os.getenv("OPENAI_MODEL", "")).strip()
    return candidate or DEFAULT_OPENAI_MODEL


def _fallback_summary(report_text: str) -> dict[str, Any]:
    text = report_text.strip()
    lines = [line.strip("- ").strip() for line in text.splitlines() if line.strip()]
    summary = "\n".join(lines[:6])[:1200] or "Deep research report imported."

    lowered = text.lower()
    positive_keywords = ("上方修正", "増益", "成長", "改善", "追い風", "strong", "beat", "upgrade")
    negative_keywords = ("下方修正", "減益", "悪化", "逆風", "懸念", "risk", "downgrade")
    pos = sum(1 for kw in positive_keywords if kw in lowered)
    neg = sum(1 for kw in negative_keywords if kw in lowered)
    rating = "B"
    if pos >= neg + 2:
        rating = "A"
    elif neg >= pos + 2:
        rating = "C"

    drivers = [line for line in lines if any(k in line for k in ("要因", "driver", "背景", "改善", "成長"))][:8]
    catalysts = [line for line in lines if any(k in line for k in ("カタリスト", "イベント", "決算", "watch"))][:8]
    risks = [line for line in lines if any(k in line for k in ("リスク", "懸念", "悪化", "停止", "drawdown"))][:8]

    if not drivers:
        drivers = ["業績・需給・外部環境の変化を継続監視する"]
    if not catalysts:
        catalysts = ["次回決算、ガイダンス修正、需給の変化"]
    if not risks:
        risks = ["業績鈍化、流動性悪化、外部環境の逆風"]

    return {
        "rating": rating,
        "summary": summary,
        "drivers": drivers,
        "catalysts": catalysts,
        "risks": risks,
    }


def build_deep_research_snapshot(
    payload: DeepResearchInput,
    *,
    api_key: str | None = None,
    model: str | None = None,
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    key = (api_key or "").strip()
    if not key:
        data = _fallback_summary(payload.report_text)
    else:
        prompt = "\n".join(
            [
                "You are a buy-side research assistant.",
                "Return JSON only.",
                f"Security: {payload.security_id}",
                "Task: Summarize the deep research report into structured investment snapshot.",
                "rating: A/B/C",
                "summary: concise Japanese summary",
                "drivers/catalysts/risks: concise bullet-like strings",
                "",
                "Report:",
                payload.report_text[:12000],
            ]
        )
        data = request_openai_json(
            prompt=prompt,
            api_key=key,
            model=_resolve_model(model),
            timeout_sec=timeout_sec,
            json_schema=DEEP_RESEARCH_SCHEMA,
        )

    return {
        "source": payload.source,
        "rating": str(data.get("rating", "B")).upper(),
        "summary": str(data.get("summary", "")).strip() or "Deep research summary",
        "snapshot": {
            "drivers": list(data.get("drivers", [])),
            "catalysts": list(data.get("catalysts", [])),
            "risks": list(data.get("risks", [])),
            "raw_length": len(payload.report_text),
        },
    }


def parse_deep_research_file_if_configured() -> DeepResearchInput | None:
    report_path = (os.getenv("DEEP_RESEARCH_REPORT_PATH", "") or "").strip()
    security_id = (os.getenv("DEEP_RESEARCH_SECURITY_ID", "") or "").strip()
    if not report_path or not security_id:
        return None

    path = Path(report_path)
    if not path.exists() or not path.is_file():
        return None
    report_text = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not report_text:
        return None
    return DeepResearchInput(security_id=security_id, report_text=report_text)
