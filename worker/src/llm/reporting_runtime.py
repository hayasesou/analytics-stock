from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Iterable

import pandas as pd

from src.llm.openai_client import request_openai_json
from src.llm.reporting_support import *
from src.types import CitationItem, ReportItem


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
        request_kwargs: dict[str, Any] = {"prompt": prompt, "api_key": key, "model": selected_model, "json_schema": SECURITY_REPORT_JSON_SCHEMA}
        if timeout_sec is not None:
            request_kwargs["timeout_sec"] = float(timeout_sec)
        if max_output_tokens is not None:
            request_kwargs["max_output_tokens"] = int(max_output_tokens)
        llm_payload = request_openai_json(**request_kwargs)
    else:
        llm_payload = llm_json_fn(prompt, selected_model, api_key)
    parsed = parse_security_report_llm_payload(llm_payload)
    citations = _remap_citations(raw_citations, claim_ids)
    cited_claim_ids = {citation.claim_id for citation in citations}
    status_by_claim = dict(parsed["status_by_claim"])
    for claim_id in claim_ids:
        if status_by_claim.get(claim_id) == "supported" and claim_id not in cited_claim_ids:
            status_by_claim[claim_id] = "hypothesis"
    body_md = f"{parsed['body_md']}\n\n## DCF（Top10のみ）\n{dcf_markdown}" if dcf_markdown else parsed["body_md"]
    return ReportItem(report_type="security_full", title=parsed["title"], body_md=body_md, conclusion=parsed["conclusion"], falsification_conditions=parsed["falsification_conditions"], confidence=row["confidence"], security_id=row["security_id"], claims=_build_security_claims(status_by_claim), citations=citations)


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
        request_kwargs: dict[str, Any] = {"prompt": prompt, "api_key": key, "model": selected_model, "json_schema": WEEKLY_SUMMARY_JSON_SCHEMA}
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
            citations.append(CitationItem(claim_id="C1", doc_version_id=str(events[0]["doc_version_id"]), page_ref="p1", quote_text=quote_text))
    status_by_claim = dict(parsed["status_by_claim"])
    if status_by_claim.get("C1") == "supported" and not citations:
        status_by_claim["C1"] = "hypothesis"
    return ReportItem(report_type="weekly_summary", title=parsed["title"], body_md=parsed["body_md"], conclusion=parsed["conclusion"], falsification_conditions=parsed["falsification_conditions"], confidence="Medium", claims=_build_weekly_summary_claims(status_by_claim), citations=citations)


def generate_weekly_summary_report(run_id: str, as_of: datetime, top50: pd.DataFrame, events: list[dict[str, str]]) -> ReportItem:
    lines = [
        f"週次サマリ（as-of: {as_of.date().isoformat()}）",
        "",
        "## 市況ハイライト",
        f"- 対象銘柄数: {len(top50)}",
        f"- High/Med イベント数: {sum(1 for event in events if event['importance'] in {'high', 'medium'})}",
        "",
        "## Top10 プレビュー",
        "| rank | security | market | score | confidence |",
        "|---:|---|---|---:|---|",
    ]
    for _, row in top50.head(10).iterrows():
        lines.append(f"| {int(row['mixed_rank'])} | {_md_cell(_security_label(row))} | {row['market']} | {row['combined_score']:.2f} | {row['confidence']} |")
    lines.extend(["", "## 次に見るべき3点", "1. High confidence かつ Top10 の一次情報更新", "2. 流動性フラグの変化", "3. バックテスト strict コスト時のドローダウン推移"])
    claims = _build_weekly_summary_claims({"C1": "hypothesis"})
    citations: list[CitationItem] = []
    if events and events[0].get("doc_version_id"):
        quote_text = str(events[0].get("summary") or events[0].get("title") or "").strip()
        if quote_text:
            claims = _build_weekly_summary_claims({"C1": "supported"})
            citations.append(CitationItem(claim_id="C1", doc_version_id=str(events[0]["doc_version_id"]), page_ref="p1", quote_text=quote_text))
    return ReportItem(report_type="weekly_summary", title=f"Weekly Summary {as_of.date().isoformat()}", body_md="\n".join(lines), conclusion="監視集合Top50とシグナル銘柄を更新。High根拠が揃う銘柄を優先。", falsification_conditions="主要一次情報の否定、欠損率上昇、流動性低下で結論を再評価。", confidence="Medium", claims=claims, citations=citations)


def generate_security_report(row: pd.Series, as_of: datetime, evidence_citations: list[CitationItem] | None = None, dcf_markdown: str | None = None) -> ReportItem:
    claim_ids = [claim_id for claim_id, _ in SECURITY_CLAIMS]
    body_lines = [f"# {row['security_id']} レポート ({as_of.date().isoformat()})", "", "## スコア", _score_table(row), "", "## 重要主張", "- C1: 総合スコアは市場内で相対上位。", "- C2: 現時点の欠損率と流動性から監視継続が妥当。", "- C3: シグナル条件は Confidence と順位で判定される。", "", "## 結論", "現時点では監視継続。シグナル点灯時のみエントリー候補。", "", "## 反証条件", "一次情報でガイダンス悪化/重大矛盾が確認された場合、結論を撤回。"]
    if dcf_markdown:
        body_lines.extend(["", "## DCF（Top10のみ）", dcf_markdown])
    raw_citations = list(evidence_citations or [])
    citations = _remap_citations(raw_citations, claim_ids)
    supported_claim_ids = {citation.claim_id for citation in citations}
    status_by_claim = {claim_id: ("supported" if claim_id in supported_claim_ids else "hypothesis") for claim_id in claim_ids}
    return ReportItem(report_type="security_full", title=f"Security Report {row['security_id']}", body_md="\n".join(body_lines), conclusion="シグナル点灯まで監視。点灯時はATRルールで執行。", falsification_conditions="重要開示で前提が崩れた場合、または欠損率閾値超過で撤退。", confidence=row["confidence"], security_id=row["security_id"], claims=_build_security_claims(status_by_claim), citations=citations)


def generate_event_digest_report(as_of: datetime, events: Iterable[dict[str, str]]) -> ReportItem:
    events = list(events)
    high = [event for event in events if event["importance"] == "high"]
    med = [event for event in events if event["importance"] == "medium"]
    low = [event for event in events if event["importance"] == "low"]
    lines = [f"# Daily Event Digest ({as_of.strftime('%Y-%m-%d %H:%M:%S')})", "", f"- High: {len(high)}", f"- Medium: {len(med)}", f"- Low: {len(low)}", "", "## High"]
    lines.extend([f"- {event['title']}: {event['summary']} ({event.get('source_url', '-')})" for event in high] or ["- なし"])
    lines.extend(["", "## Medium"])
    lines.extend([f"- {event['title']}: {event['summary']}" for event in med[:10]] or ["- なし"])
    return ReportItem(report_type="event_digest", title=f"Daily Event Digest {as_of.date().isoformat()}", body_md="\n".join(lines), conclusion="High/Medium イベントを監視集合更新の入力に使用。", falsification_conditions="一次情報との不一致が判明した場合は再収集。", confidence="Medium")


__all__ = [
    "generate_event_digest_report",
    "generate_security_report",
    "generate_security_report_with_llm",
    "generate_weekly_summary_report",
    "generate_weekly_summary_report_with_llm",
]
