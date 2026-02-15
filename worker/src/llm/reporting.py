from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Iterable

import pandas as pd

from src.llm.openai_client import DEFAULT_OPENAI_MODEL, request_openai_json
from src.types import CitationItem, ReportItem

SECURITY_CLAIMS: list[tuple[str, str]] = [
    ("C1", "総合スコアは市場内で相対上位"),
    ("C2", "欠損率と流動性を考慮して監視継続"),
    ("C3", "シグナル条件は High かつ Top10"),
]
WEEKLY_SUMMARY_CLAIMS: list[tuple[str, str]] = [
    ("C1", "Top50 weekly ranking has been refreshed with mixed-market constraints"),
]
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
        "claims": {
            "type": "array",
            "minItems": 3,
            "maxItems": 3,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["claim_id", "status"],
                "properties": {
                    "claim_id": {"type": "string", "enum": ["C1", "C2", "C3"]},
                    "status": {"type": "string", "enum": ["supported", "hypothesis"]},
                },
            },
        },
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
        "claims": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["claim_id", "status"],
                "properties": {
                    "claim_id": {"type": "string", "enum": ["C1"]},
                    "status": {"type": "string", "enum": ["supported", "hypothesis"]},
                },
            },
        },
    },
}


def _resolve_openai_model(model: str | None) -> str:
    candidate = model if model is not None else os.getenv("OPENAI_MODEL", "")
    selected = (candidate or "").strip()
    return selected or DEFAULT_OPENAI_MODEL


def _score_table(row: pd.Series) -> str:
    lines = [
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
    return "\n".join(lines)


def _row_text(row: pd.Series, key: str, default: str = "") -> str:
    value = row.get(key, default)
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
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
    remapped: list[CitationItem] = []
    for claim_id, citation in zip(claim_ids, raw_citations, strict=False):
        remapped.append(
            CitationItem(
                claim_id=claim_id,
                doc_version_id=citation.doc_version_id,
                page_ref=citation.page_ref,
                quote_text=citation.quote_text,
            )
        )
    return remapped


def _build_security_claims(status_by_claim: dict[str, str]) -> list[dict[str, str]]:
    claims: list[dict[str, str]] = []
    for claim_id, claim_text in SECURITY_CLAIMS:
        claims.append(
            {
                "claim_id": claim_id,
                "claim_text": claim_text,
                "status": status_by_claim.get(claim_id, "hypothesis"),
            }
        )
    return claims


def _build_weekly_summary_claims(status_by_claim: dict[str, str]) -> list[dict[str, str]]:
    claims: list[dict[str, str]] = []
    for claim_id, claim_text in WEEKLY_SUMMARY_CLAIMS:
        claims.append(
            {
                "claim_id": claim_id,
                "claim_text": claim_text,
                "status": status_by_claim.get(claim_id, "hypothesis"),
            }
        )
    return claims


def build_security_report_prompt(
    row: pd.Series,
    as_of: datetime,
    evidence_citations: list[CitationItem] | None = None,
) -> str:
    citations = list(evidence_citations or [])
    citation_lines = ["(none)"]
    if citations:
        citation_lines = [
            f"- doc={c.doc_version_id} page={c.page_ref or '-'} quote={c.quote_text}"
            for c in citations
        ]

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


def build_weekly_summary_report_prompt(
    run_id: str,
    as_of: datetime,
    top50: pd.DataFrame,
    events: list[dict[str, str]],
) -> str:
    top_preview = top50.head(10)
    high_med_count = sum(1 for e in events if e.get("importance") in {"high", "medium"})
    top_lines = [
        (
            f"- rank={int(row['mixed_rank'])} security={_security_label(row)} "
            f"market={row['market']} score={float(row['combined_score']):.2f} confidence={row['confidence']}"
        )
        for _, row in top_preview.iterrows()
    ]
    if not top_lines:
        top_lines = ["(none)"]

    event_lines = [
        f"- importance={e.get('importance', '-')} title={e.get('title', '-')}"
        for e in events[:10]
    ]
    if not event_lines:
        event_lines = ["(none)"]

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

    text_keys = ["title", "body_md", "conclusion", "falsification_conditions"]
    normalized: dict[str, Any] = {}
    for key in text_keys:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LLM payload missing required string field: {key}")
        normalized[key] = value.strip()

    status_by_claim = {claim_id: "hypothesis" for claim_id, _ in SECURITY_CLAIMS}
    claims = payload.get("claims")
    if isinstance(claims, list):
        for c in claims:
            if not isinstance(c, dict):
                continue
            claim_id = str(c.get("claim_id", "")).strip().upper()
            status = str(c.get("status", "")).strip().lower()
            if claim_id in status_by_claim and status in ALLOWED_CLAIM_STATUS:
                status_by_claim[claim_id] = status
    normalized["status_by_claim"] = status_by_claim
    return normalized


def parse_weekly_summary_report_llm_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be object")

    text_keys = ["title", "body_md", "conclusion", "falsification_conditions"]
    normalized: dict[str, Any] = {}
    for key in text_keys:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LLM payload missing required string field: {key}")
        normalized[key] = value.strip()

    status_by_claim = {claim_id: "hypothesis" for claim_id, _ in WEEKLY_SUMMARY_CLAIMS}
    claims = payload.get("claims")
    if isinstance(claims, list):
        for c in claims:
            if not isinstance(c, dict):
                continue
            claim_id = str(c.get("claim_id", "")).strip().upper()
            status = str(c.get("status", "")).strip().lower()
            if claim_id in status_by_claim and status in ALLOWED_CLAIM_STATUS:
                status_by_claim[claim_id] = status
    normalized["status_by_claim"] = status_by_claim
    return normalized


def generate_security_report_with_llm(
    row: pd.Series,
    as_of: datetime,
    evidence_citations: list[CitationItem] | None = None,
    dcf_markdown: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
    timeout_sec: float | None = None,
    max_output_tokens: int | None = None,
    llm_json_fn: Callable[[str, str, str | None], dict[str, Any]] | None = None,
) -> ReportItem:
    claim_ids = [claim_id for claim_id, _ in SECURITY_CLAIMS]
    raw_citations = list(evidence_citations or [])
    prompt = build_security_report_prompt(row, as_of, raw_citations)
    selected_model = _resolve_openai_model(model)

    if llm_json_fn is None:
        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY is required for LLM report generation")
        request_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "api_key": key,
            "model": selected_model,
            "json_schema": SECURITY_REPORT_JSON_SCHEMA,
        }
        if timeout_sec is not None:
            request_kwargs["timeout_sec"] = float(timeout_sec)
        if max_output_tokens is not None:
            request_kwargs["max_output_tokens"] = int(max_output_tokens)
        llm_payload = request_openai_json(**request_kwargs)
    else:
        llm_payload = llm_json_fn(prompt, selected_model, api_key)

    parsed = parse_security_report_llm_payload(llm_payload)
    citations = _remap_citations(raw_citations, claim_ids)
    cited_claim_ids = {c.claim_id for c in citations}

    status_by_claim = dict(parsed["status_by_claim"])
    for claim_id in claim_ids:
        if status_by_claim.get(claim_id) == "supported" and claim_id not in cited_claim_ids:
            status_by_claim[claim_id] = "hypothesis"

    body_md = parsed["body_md"]
    if dcf_markdown:
        body_md = f"{body_md}\n\n## DCF（Top10のみ）\n{dcf_markdown}"

    return ReportItem(
        report_type="security_full",
        title=parsed["title"],
        body_md=body_md,
        conclusion=parsed["conclusion"],
        falsification_conditions=parsed["falsification_conditions"],
        confidence=row["confidence"],
        security_id=row["security_id"],
        claims=_build_security_claims(status_by_claim),
        citations=citations,
    )


def generate_weekly_summary_report_with_llm(
    run_id: str,
    as_of: datetime,
    top50: pd.DataFrame,
    events: list[dict[str, str]],
    model: str | None = None,
    api_key: str | None = None,
    timeout_sec: float | None = None,
    max_output_tokens: int | None = None,
    llm_json_fn: Callable[[str, str, str | None], dict[str, Any]] | None = None,
) -> ReportItem:
    prompt = build_weekly_summary_report_prompt(run_id, as_of, top50, events)
    selected_model = _resolve_openai_model(model)

    if llm_json_fn is None:
        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY is required for LLM report generation")
        request_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "api_key": key,
            "model": selected_model,
            "json_schema": WEEKLY_SUMMARY_JSON_SCHEMA,
        }
        if timeout_sec is not None:
            request_kwargs["timeout_sec"] = float(timeout_sec)
        if max_output_tokens is not None:
            request_kwargs["max_output_tokens"] = int(max_output_tokens)
        llm_payload = request_openai_json(**request_kwargs)
    else:
        llm_payload = llm_json_fn(prompt, selected_model, api_key)

    parsed = parse_weekly_summary_report_llm_payload(llm_payload)

    citations: list[CitationItem] = []
    if events and events[0].get("doc_version_id"):
        quote_text = str(events[0].get("summary") or events[0].get("title") or "").strip()
        if quote_text:
            citations.append(
                CitationItem(
                    claim_id="C1",
                    doc_version_id=str(events[0]["doc_version_id"]),
                    page_ref="p1",
                    quote_text=quote_text,
                )
            )

    status_by_claim = dict(parsed["status_by_claim"])
    if status_by_claim.get("C1") == "supported" and not citations:
        status_by_claim["C1"] = "hypothesis"

    return ReportItem(
        report_type="weekly_summary",
        title=parsed["title"],
        body_md=parsed["body_md"],
        conclusion=parsed["conclusion"],
        falsification_conditions=parsed["falsification_conditions"],
        confidence="Medium",
        claims=_build_weekly_summary_claims(status_by_claim),
        citations=citations,
    )


def generate_weekly_summary_report(
    run_id: str,
    as_of: datetime,
    top50: pd.DataFrame,
    events: list[dict[str, str]],
) -> ReportItem:
    top_preview = top50.head(10)
    lines = [
        f"週次サマリ（as-of: {as_of.date().isoformat()}）",
        "",
        "## 市況ハイライト",
        f"- 対象銘柄数: {len(top50)}",
        f"- High/Med イベント数: {sum(1 for e in events if e['importance'] in {'high', 'medium'})}",
        "",
        "## Top10 プレビュー",
        "| rank | security | market | score | confidence |",
        "|---:|---|---|---:|---|",
    ]

    for _, row in top_preview.iterrows():
        security = _md_cell(_security_label(row))
        lines.append(
            f"| {int(row['mixed_rank'])} | {security} | {row['market']} | {row['combined_score']:.2f} | {row['confidence']} |"
        )

    lines.append("")
    lines.append("## 次に見るべき3点")
    lines.append("1. High confidence かつ Top10 の一次情報更新")
    lines.append("2. 流動性フラグの変化")
    lines.append("3. バックテスト strict コスト時のドローダウン推移")

    claims = _build_weekly_summary_claims({"C1": "hypothesis"})
    citations: list[CitationItem] = []
    if events and events[0].get("doc_version_id"):
        quote_text = str(events[0].get("summary") or events[0].get("title") or "").strip()
        if quote_text:
            claims = _build_weekly_summary_claims({"C1": "supported"})
            citations.append(
                CitationItem(
                    claim_id="C1",
                    doc_version_id=str(events[0]["doc_version_id"]),
                    page_ref="p1",
                    quote_text=quote_text,
                )
            )

    return ReportItem(
        report_type="weekly_summary",
        title=f"Weekly Summary {as_of.date().isoformat()}",
        body_md="\n".join(lines),
        conclusion="監視集合Top50とシグナル銘柄を更新。High根拠が揃う銘柄を優先。",
        falsification_conditions="主要一次情報の否定、欠損率上昇、流動性低下で結論を再評価。",
        confidence="Medium",
        claims=claims,
        citations=citations,
    )


def generate_security_report(
    row: pd.Series,
    as_of: datetime,
    evidence_citations: list[CitationItem] | None = None,
    dcf_markdown: str | None = None,
) -> ReportItem:
    claim_ids = [claim_id for claim_id, _ in SECURITY_CLAIMS]

    body_lines = [
        f"# {row['security_id']} レポート ({as_of.date().isoformat()})",
        "",
        "## スコア",
        _score_table(row),
        "",
        "## 重要主張",
        "- C1: 総合スコアは市場内で相対上位。",
        "- C2: 現時点の欠損率と流動性から監視継続が妥当。",
        "- C3: シグナル条件は Confidence と順位で判定される。",
        "",
        "## 結論",
        "現時点では監視継続。シグナル点灯時のみエントリー候補。",
        "",
        "## 反証条件",
        "一次情報でガイダンス悪化/重大矛盾が確認された場合、結論を撤回。",
    ]

    if dcf_markdown:
        body_lines.extend(["", "## DCF（Top10のみ）", dcf_markdown])

    raw_citations = list(evidence_citations or [])
    citations = _remap_citations(raw_citations, claim_ids)
    supported_claim_ids = {citation.claim_id for citation in citations}
    status_by_claim = {
        claim_id: ("supported" if claim_id in supported_claim_ids else "hypothesis")
        for claim_id in claim_ids
    }

    return ReportItem(
        report_type="security_full",
        title=f"Security Report {row['security_id']}",
        body_md="\n".join(body_lines),
        conclusion="シグナル点灯まで監視。点灯時はATRルールで執行。",
        falsification_conditions="重要開示で前提が崩れた場合、または欠損率閾値超過で撤退。",
        confidence=row["confidence"],
        security_id=row["security_id"],
        claims=_build_security_claims(status_by_claim),
        citations=citations,
    )


def generate_event_digest_report(as_of: datetime, events: Iterable[dict[str, str]]) -> ReportItem:
    events = list(events)
    high = [e for e in events if e["importance"] == "high"]
    med = [e for e in events if e["importance"] == "medium"]
    low = [e for e in events if e["importance"] == "low"]

    lines = [
        f"# Daily Event Digest ({as_of.strftime('%Y-%m-%d %H:%M:%S')})",
        "",
        f"- High: {len(high)}",
        f"- Medium: {len(med)}",
        f"- Low: {len(low)}",
        "",
        "## High",
    ]

    if not high:
        lines.append("- なし")
    else:
        for e in high:
            lines.append(f"- {e['title']}: {e['summary']} ({e.get('source_url', '-')})")

    lines.append("")
    lines.append("## Medium")
    if not med:
        lines.append("- なし")
    else:
        for e in med[:10]:
            lines.append(f"- {e['title']}: {e['summary']}")

    return ReportItem(
        report_type="event_digest",
        title=f"Daily Event Digest {as_of.date().isoformat()}",
        body_md="\n".join(lines),
        conclusion="High/Medium イベントを監視集合更新の入力に使用。",
        falsification_conditions="一次情報との不一致が判明した場合は再収集。",
        confidence="Medium",
    )
