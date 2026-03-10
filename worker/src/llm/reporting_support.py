from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd

from src.llm.openai_client import DEFAULT_OPENAI_MODEL
from src.types import CitationItem

SECURITY_CLAIMS: list[tuple[str, str]] = [
    ("C1", "総合スコアは市場内で相対上位"),
    ("C2", "欠損率と流動性を考慮して監視継続"),
    ("C3", "シグナル条件は High かつ Top10"),
]
WEEKLY_SUMMARY_CLAIMS: list[tuple[str, str]] = [("C1", "Top50 weekly ranking has been refreshed with mixed-market constraints")]
ALLOWED_CLAIM_STATUS = {"supported", "hypothesis"}
SECURITY_REPORT_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "body_md", "conclusion", "falsification_conditions", "claims"],
    "properties": {
        "title": {"type": "string", "minLength": 1, "maxLength": 160},
        "body_md": {"type": "string", "minLength": 1, "maxLength": 4000},
        "conclusion": {"type": "string", "minLength": 1, "maxLength": 1000},
        "falsification_conditions": {"type": "string", "minLength": 1, "maxLength": 1000},
        "claims": {"type": "array", "minItems": 3, "maxItems": 3, "items": {"type": "object", "additionalProperties": False, "required": ["claim_id", "status"], "properties": {"claim_id": {"type": "string", "enum": ["C1", "C2", "C3"]}, "status": {"type": "string", "enum": ["supported", "hypothesis"]}}}},
    },
}
WEEKLY_SUMMARY_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "body_md", "conclusion", "falsification_conditions", "claims"],
    "properties": {
        "title": {"type": "string", "minLength": 1, "maxLength": 160},
        "body_md": {"type": "string", "minLength": 1, "maxLength": 2500},
        "conclusion": {"type": "string", "minLength": 1, "maxLength": 1000},
        "falsification_conditions": {"type": "string", "minLength": 1, "maxLength": 1000},
        "claims": {"type": "array", "minItems": 1, "maxItems": 1, "items": {"type": "object", "additionalProperties": False, "required": ["claim_id", "status"], "properties": {"claim_id": {"type": "string", "enum": ["C1"]}, "status": {"type": "string", "enum": ["supported", "hypothesis"]}}}},
    },
}


def _resolve_openai_model(model: str | None) -> str:
    candidate = model if model is not None else os.getenv("OPENAI_MODEL", "")
    return (candidate or "").strip() or DEFAULT_OPENAI_MODEL


def _score_table(row: pd.Series) -> str:
    return "\n".join(
        [
            "| metric | value |",
            "|---|---:|",
            f"| Quality | {row['quality']:.2f} |",
            f"| Growth | {row['growth']:.2f} |",
            f"| Value | {row['value']:.2f} |",
            f"| Momentum | {row['momentum']:.2f} |",
            f"| Catalyst | {row['catalyst']:.2f} |",
            f"| Combined | {row['combined_score']:.2f} |",
            f"| Confidence | {row['confidence']} |",
        ]
    )


def _row_text(row: pd.Series, key: str, default: str = "") -> str:
    value = row.get(key, default)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    text = str(value).strip()
    return text or default


def _security_label(row: pd.Series) -> str:
    security_id = _row_text(row, "security_id", "-")
    ticker = _row_text(row, "ticker")
    name = _row_text(row, "name")
    if ticker and name:
        return f"{ticker} / {name} ({security_id})"
    if name:
        return f"{name} ({security_id})"
    if ticker:
        return f"{ticker} ({security_id})"
    return security_id


def _md_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _remap_citations(raw_citations: list[CitationItem], claim_ids: list[str]) -> list[CitationItem]:
    return [CitationItem(claim_id=claim_id, doc_version_id=citation.doc_version_id, page_ref=citation.page_ref, quote_text=citation.quote_text) for claim_id, citation in zip(claim_ids, raw_citations, strict=False)]


def _build_security_claims(status_by_claim: dict[str, str]) -> list[dict[str, str]]:
    return [{"claim_id": claim_id, "claim_text": claim_text, "status": status_by_claim.get(claim_id, "hypothesis")} for claim_id, claim_text in SECURITY_CLAIMS]


def _build_weekly_summary_claims(status_by_claim: dict[str, str]) -> list[dict[str, str]]:
    return [{"claim_id": claim_id, "claim_text": claim_text, "status": status_by_claim.get(claim_id, "hypothesis")} for claim_id, claim_text in WEEKLY_SUMMARY_CLAIMS]


def build_security_report_prompt(row: pd.Series, as_of: datetime, evidence_citations: list[CitationItem] | None = None) -> str:
    citations = list(evidence_citations or [])
    citation_lines = [f"- doc={c.doc_version_id} page={c.page_ref or '-'} quote={c.quote_text}" for c in citations] or ["(none)"]
    metrics_lines = [
        f"- Quality: {float(row['quality']):.2f}",
        f"- Growth: {float(row['growth']):.2f}",
        f"- Value: {float(row['value']):.2f}",
        f"- Momentum: {float(row['momentum']):.2f}",
        f"- Catalyst: {float(row['catalyst']):.2f}",
        f"- Combined: {float(row['combined_score']):.2f}",
        f"- Confidence: {row['confidence']}",
    ]
    return "\n".join(
        [
            "You are a stock research assistant.",
            "Return JSON only. Do not include markdown code fences.",
            "",
            f"Security: {row['security_id']}",
            f"As-of date: {as_of.date().isoformat()}",
            "",
            "Metrics:",
            *metrics_lines,
            "",
            "Evidence citations:",
            *citation_lines,
            "",
            "Requirements:",
            "- Write concise Japanese text for body_md/conclusion/falsification_conditions.",
            "- claims must include C1/C2/C3 exactly.",
            "- status must be supported or hypothesis.",
            "- If evidence is insufficient, set status to hypothesis.",
            "",
            "Output JSON schema:",
            "{",
            '  "title": "string",',
            '  "body_md": "string",',
            '  "conclusion": "string",',
            '  "falsification_conditions": "string",',
            '  "claims": [',
            '    {"claim_id":"C1","status":"supported|hypothesis"},',
            '    {"claim_id":"C2","status":"supported|hypothesis"},',
            '    {"claim_id":"C3","status":"supported|hypothesis"}',
            "  ]",
            "}",
        ]
    )


def build_weekly_summary_report_prompt(run_id: str, as_of: datetime, top50: pd.DataFrame, events: list[dict[str, str]]) -> str:
    top_lines = [f"- rank={int(row['mixed_rank'])} security={_security_label(row)} market={row['market']} score={float(row['combined_score']):.2f} confidence={row['confidence']}" for _, row in top50.head(10).iterrows()] or ["(none)"]
    event_lines = [f"- importance={event.get('importance', '-')} title={event.get('title', '-')}" for event in events[:10]] or ["(none)"]
    high_med_count = sum(1 for event in events if event.get("importance") in {"high", "medium"})
    return "\n".join(
        [
            "You are a stock research assistant.",
            "Return JSON only. Do not include markdown code fences.",
            "",
            f"Run ID: {run_id}",
            f"As-of date: {as_of.date().isoformat()}",
            f"Top50 count: {len(top50)}",
            f"High/Med events count: {high_med_count}",
            "",
            "Top10 preview:",
            *top_lines,
            "",
            "Recent events preview:",
            *event_lines,
            "",
            "Requirements:",
            "- Write concise Japanese text for body_md/conclusion/falsification_conditions.",
            "- Keep body_md practical with watch points.",
            "- claims must include C1 exactly.",
            "- status must be supported or hypothesis.",
            "- If event citation is not available, status should be hypothesis.",
            "",
            "Output JSON schema:",
            "{",
            '  "title": "string",',
            '  "body_md": "string",',
            '  "conclusion": "string",',
            '  "falsification_conditions": "string",',
            '  "claims": [',
            '    {"claim_id":"C1","status":"supported|hypothesis"}',
            "  ]",
            "}",
        ]
    )


def parse_security_report_llm_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be object")
    normalized: dict[str, Any] = {}
    for key in ["title", "body_md", "conclusion", "falsification_conditions"]:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LLM payload missing required string field: {key}")
        normalized[key] = value.strip()
    status_by_claim = {claim_id: "hypothesis" for claim_id, _ in SECURITY_CLAIMS}
    claims = payload.get("claims")
    if isinstance(claims, list):
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            claim_id = str(claim.get("claim_id", "")).strip().upper()
            status = str(claim.get("status", "")).strip().lower()
            if claim_id in status_by_claim and status in ALLOWED_CLAIM_STATUS:
                status_by_claim[claim_id] = status
    normalized["status_by_claim"] = status_by_claim
    return normalized


def parse_weekly_summary_report_llm_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be object")
    normalized: dict[str, Any] = {}
    for key in ["title", "body_md", "conclusion", "falsification_conditions"]:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LLM payload missing required string field: {key}")
        normalized[key] = value.strip()
    status_by_claim = {claim_id: "hypothesis" for claim_id, _ in WEEKLY_SUMMARY_CLAIMS}
    claims = payload.get("claims")
    if isinstance(claims, list):
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            claim_id = str(claim.get("claim_id", "")).strip().upper()
            status = str(claim.get("status", "")).strip().lower()
            if claim_id in status_by_claim and status in ALLOWED_CLAIM_STATUS:
                status_by_claim[claim_id] = status
    normalized["status_by_claim"] = status_by_claim
    return normalized


__all__ = [
    "ALLOWED_CLAIM_STATUS",
    "SECURITY_CLAIMS",
    "SECURITY_REPORT_JSON_SCHEMA",
    "WEEKLY_SUMMARY_CLAIMS",
    "WEEKLY_SUMMARY_JSON_SCHEMA",
    "_build_security_claims",
    "_build_weekly_summary_claims",
    "_md_cell",
    "_remap_citations",
    "_resolve_openai_model",
    "_row_text",
    "_score_table",
    "_security_label",
    "build_security_report_prompt",
    "build_weekly_summary_report_prompt",
    "parse_security_report_llm_payload",
    "parse_weekly_summary_report_llm_payload",
]
