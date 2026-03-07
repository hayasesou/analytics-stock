from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
import re
from typing import Any

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import parse_ingest_youtube_command
from src.integrations.youtube import YouTubeClient, extract_video_id, normalize_youtube_url
from src.storage.db import NeonRepository
from src.types import IdeaEvidenceSpec, IdeaSpec


def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(minimum, min(maximum, out))


def _to_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        out = default
    return max(minimum, min(maximum, out))


def _resolve_ingest_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("youtube_ingest", {})
    if not isinstance(root, dict):
        root = {}
    return {
        "enabled": bool(root.get("enabled", True)),
        "request_timeout_sec": _to_float(root.get("request_timeout_sec"), 8.0, 1.0, 60.0),
        "retry_count": _to_int(root.get("retry_count"), 2, 0, 10),
        "max_comments": _to_int(root.get("max_comments"), 20000, 0, 200000),
        "max_comment_pages": _to_int(root.get("max_comment_pages"), 200, 1, 5000),
        "max_comment_duration_sec": _to_float(root.get("max_comment_duration_sec"), 600.0, 1.0, 7200.0),
        "max_claims": _to_int(root.get("max_claims"), 5, 1, 20),
        "min_claim_chars": _to_int(root.get("min_claim_chars"), 20, 5, 400),
        "idea_priority": _to_int(root.get("idea_priority"), 80, 0, 1000),
        "task_priority": _to_int(root.get("task_priority"), 30, 0, 1000),
        "created_by": str(root.get("created_by", "discord-user")).strip() or "discord-user",
    }


def _parse_datetime_or_none(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_url_from_inputs(command: str | None, url: str | None) -> str:
    if url and str(url).strip():
        return str(url).strip()
    if command and str(command).strip():
        parsed = parse_ingest_youtube_command(str(command))
        if parsed:
            return parsed
    raise ValueError("ingest_youtube_url_required")


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).strip())


def _split_sentences(text: str) -> list[str]:
    raw = str(text or "")
    parts = re.split(r"[。！？!?\n]+", raw)
    out: list[str] = []
    for part in parts:
        cleaned = _collapse_whitespace(part)
        if cleaned:
            out.append(cleaned)
    return out


_TICKER_STOPWORDS = {
    "THE",
    "THIS",
    "THAT",
    "WITH",
    "FROM",
    "WILL",
    "YOUR",
    "HAVE",
    "ABOUT",
    "VIDEO",
    "YOUTUBE",
    "COMMENT",
    "SHORTS",
    "NEWS",
    "USD",
}


def _extract_ticker_candidates(text: str, *, max_items: int = 5) -> list[str]:
    candidates: list[str] = []
    for match in re.finditer(r"(?<![A-Z0-9])\$?([A-Z]{1,5})(?![A-Z0-9])", str(text).upper()):
        ticker = match.group(1).strip()
        if not ticker or ticker in _TICKER_STOPWORDS:
            continue
        if ticker not in candidates:
            candidates.append(ticker)
        if len(candidates) >= max_items:
            break
    return candidates


def _build_causal_hypothesis(claim_text: str, tickers: list[str]) -> str:
    lowered = str(claim_text).lower()
    positive_keywords = ["増", "改善", "成長", "上方", "追い風", "expand", "growth", "beat"]
    negative_keywords = ["減", "悪化", "下方", "逆風", "懸念", "decline", "miss", "slowdown"]
    symbol_block = ", ".join(tickers) if tickers else "関連銘柄"

    if any(keyword in lowered for keyword in positive_keywords):
        return f"{symbol_block} の業績・需給改善が続く場合、相対リターンが上振れする仮説。"
    if any(keyword in lowered for keyword in negative_keywords):
        return f"{symbol_block} の業績・需給悪化が続く場合、相対リターンが下振れする仮説。"
    return f"{symbol_block} に対してニュースが需給へ波及し、価格変動を生む仮説。"


def _build_summary(metadata: dict[str, Any], comments: list[dict[str, Any]], transcripts: list[dict[str, Any]]) -> str:
    title = _collapse_whitespace(str(metadata.get("title", "")))
    channel = _collapse_whitespace(str(metadata.get("channel_title", "")))
    description = _collapse_whitespace(str(metadata.get("description", "")))
    comment_lines = [_collapse_whitespace(str((item or {}).get("text", ""))) for item in comments[:3]]
    transcript_lines = [_collapse_whitespace(str((item or {}).get("text", ""))) for item in transcripts[:2]]
    chunks = [
        x
        for x in [
            f"title={title}" if title else "",
            f"channel={channel}" if channel else "",
            f"description={description[:280]}" if description else "",
            f"comments={' | '.join([x for x in comment_lines if x][:3])}" if any(comment_lines) else "",
            f"transcript={' | '.join([x for x in transcript_lines if x][:2])}" if any(transcript_lines) else "",
        ]
        if x
    ]
    return " / ".join(chunks)[:1200]


def _build_source_blob(metadata: dict[str, Any], comments: list[dict[str, Any]], transcripts: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    if metadata:
        lines.append(f"title: {metadata.get('title', '')}")
        lines.append(f"channel: {metadata.get('channel_title', '')}")
        lines.append(f"url: {metadata.get('url', '')}")
        lines.append(f"description: {metadata.get('description', '')}")
    if comments:
        lines.append("comments:")
        for item in comments:
            if not isinstance(item, dict):
                continue
            text = _collapse_whitespace(str(item.get("text", "")))
            if not text:
                continue
            lines.append(f"- {text}")
    if transcripts:
        lines.append("transcript:")
        for item in transcripts:
            if not isinstance(item, dict):
                continue
            text = _collapse_whitespace(str(item.get("text", "")))
            if not text:
                continue
            lines.append(f"- {text}")
    return "\n".join(lines).strip()


def _claim_hash(claim_text: str) -> str:
    normalized = _collapse_whitespace(claim_text).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _extract_claims(
    *,
    metadata: dict[str, Any],
    source_blob: str,
    max_claims: int,
    min_claim_chars: int,
) -> list[dict[str, Any]]:
    title = _collapse_whitespace(str(metadata.get("title", "")))
    description = _collapse_whitespace(str(metadata.get("description", "")))
    base_text = "\n".join([title, description, source_blob]).strip()
    sentences = _split_sentences(base_text)
    if not sentences:
        return []

    seen: set[str] = set()
    claims: list[dict[str, Any]] = []
    global_tickers = _extract_ticker_candidates(f"{title} {description}", max_items=5)
    for sentence in sentences:
        if len(sentence) < max(1, int(min_claim_chars)):
            continue
        normalized = sentence.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        tickers = _extract_ticker_candidates(sentence, max_items=5)
        if not tickers:
            tickers = list(global_tickers)
        claims.append(
            {
                "claim_text": sentence,
                "evidence_excerpt": sentence[:600],
                "ticker_candidates": tickers,
                "causal_hypothesis": _build_causal_hypothesis(sentence, tickers),
                "claim_hash": _claim_hash(sentence),
            }
        )
        if len(claims) >= max_claims:
            break
    return claims


_ARBITRAGE_KEYWORDS = {
    "arb",
    "arbitrage",
    "basis",
    "funding",
    "spread",
    "乖離",
    "裁定",
    "歪み",
    "サヤ",
}


def _detect_venues(text: str) -> list[str]:
    lowered = str(text).lower()
    mapping = [
        ("BINANCE", ["binance"]),
        ("HYPERLIQUID", ["hyperliquid", "hyper liquid"]),
        ("BYBIT", ["bybit"]),
        ("OKX", ["okx"]),
    ]
    out: list[str] = []
    for venue, keys in mapping:
        if any(key in lowered for key in keys):
            out.append(venue)
    return out


def _extract_arb_edges(
    *,
    metadata: dict[str, Any],
    claims: list[dict[str, Any]],
    comments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    title = _collapse_whitespace(str(metadata.get("title", "")))
    description = _collapse_whitespace(str(metadata.get("description", "")))
    venue_hint = _detect_venues(f"{title} {description}")
    edges: list[dict[str, Any]] = []

    for claim in claims:
        claim_text = _collapse_whitespace(str(claim.get("claim_text", "")))
        if not claim_text:
            continue
        lowered = claim_text.lower()
        tickers = [str(x).strip().upper() for x in claim.get("ticker_candidates", []) if str(x).strip()]
        venues = _detect_venues(claim_text)
        if not venues:
            venues = list(venue_hint)

        priority = 0.45
        if any(key in lowered for key in _ARBITRAGE_KEYWORDS):
            priority += 0.25
        if len(venues) >= 2:
            priority += 0.15
        if tickers:
            priority += min(0.10, 0.03 * len(tickers))

        evidence: list[dict[str, Any]] = []
        for item in comments:
            if not isinstance(item, dict):
                continue
            comment_text = _collapse_whitespace(str(item.get("text", "")))
            if not comment_text:
                continue
            comment_lower = comment_text.lower()
            has_keyword = any(key in comment_lower for key in _ARBITRAGE_KEYWORDS)
            has_ticker = any(ticker.lower() in comment_lower for ticker in tickers)
            if not (has_keyword or has_ticker):
                continue
            evidence.append(
                {
                    "comment_id": str(item.get("comment_id", "")).strip() or None,
                    "text": comment_text[:260],
                    "like_count": int(item.get("like_count", 0) or 0),
                }
            )
            if len(evidence) >= 3:
                break

        if evidence:
            priority += min(0.10, 0.03 * len(evidence))

        instrument = f"{tickers[0]}-PERP" if tickers else "UNKNOWN-PERP"
        edge_type = "crypto_arb" if (len(venues) >= 2 or any(key in lowered for key in _ARBITRAGE_KEYWORDS)) else "market_watch"
        edges.append(
            {
                "edge_type": edge_type,
                "venues": venues,
                "instrument": instrument,
                "claim": claim_text,
                "ticker_candidates": tickers,
                "evidence": evidence,
                "data_requirements": ["mid_price", "spread_bps", "funding_rate", "orderbook_top"],
                "validation_plan": [
                    "spread_zscore",
                    "cost_model_with_fees_slippage",
                    "execution_latency_and_fill_rate",
                ],
                "priority": round(min(0.99, max(0.05, priority)), 4),
            }
        )

    edges.sort(key=lambda row: float(row.get("priority", 0.0)), reverse=True)
    return edges


def run_ingest_youtube(
    *,
    command: str | None = None,
    url: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    cfg = load_yaml_config()
    ingest_cfg = _resolve_ingest_cfg(cfg)
    if not bool(ingest_cfg["enabled"]):
        return {"enabled": False, "inserted_ideas": 0, "enqueued_tasks": 0, "deduped_claims": 0}

    raw_url = _parse_url_from_inputs(command, url)
    normalized_url = normalize_youtube_url(raw_url)
    video_id = extract_video_id(normalized_url)
    now_utc = now.astimezone(timezone.utc) if now and now.tzinfo else (now.replace(tzinfo=timezone.utc) if now else datetime.now(timezone.utc))

    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    youtube_api_key = str(os.getenv("YOUTUBE_API_KEY", "")).strip()
    youtube_client = YouTubeClient(
        api_key=youtube_api_key,
        timeout_sec=float(ingest_cfg["request_timeout_sec"]),
        retry_count=int(ingest_cfg["retry_count"]),
    )

    bundle = youtube_client.fetch_video_bundle(
        normalized_url,
        max_comments=int(ingest_cfg["max_comments"]),
        max_comment_pages=int(ingest_cfg["max_comment_pages"]),
        max_comment_duration_sec=float(ingest_cfg["max_comment_duration_sec"]),
    )
    metadata = bundle.get("metadata") if isinstance(bundle.get("metadata"), dict) else {}
    comments = bundle.get("comments") if isinstance(bundle.get("comments"), list) else []
    transcripts = bundle.get("transcript") if isinstance(bundle.get("transcript"), list) else []

    source_blob = _build_source_blob(metadata, comments, transcripts)
    summary = _build_summary(metadata, comments, transcripts)
    extracted_claims = _extract_claims(
        metadata=metadata,
        source_blob=source_blob,
        max_claims=int(ingest_cfg["max_claims"]),
        min_claim_chars=int(ingest_cfg["min_claim_chars"]),
    )
    extracted_edges = _extract_arb_edges(
        metadata=metadata,
        claims=extracted_claims,
        comments=comments,
    )

    existing_hashes = repo.fetch_idea_claim_hashes_by_source_url(
        source_type="youtube",
        source_url=normalized_url,
        limit=5000,
    )
    url_hash = hashlib.sha256(f"url:{normalized_url}".encode("utf-8")).hexdigest()
    if url_hash in existing_hashes:
        return {
            "enabled": True,
            "video_id": video_id,
            "source_url": normalized_url,
            "inserted_ideas": 0,
            "inserted_evidence": 0,
            "enqueued_tasks": 0,
            "deduped_claims": len(extracted_claims),
            "summary": summary,
            "comment_count": len(comments),
            "edge_count": len(extracted_edges),
        }

    evidence_payload = "\n\n".join([summary, source_blob]).strip()
    evidence_sha = hashlib.sha256(evidence_payload.encode("utf-8")).hexdigest()
    published_at = _parse_datetime_or_none(str(metadata.get("published_at", "")).strip())
    doc_version_id = repo.upsert_document_with_version(
        external_doc_id=f"youtube:{video_id}",
        source_system="youtube",
        source_url=normalized_url,
        title=str(metadata.get("title", "")).strip() or f"YouTube {video_id}",
        published_at=published_at,
        retrieved_at=now_utc,
        sha256=evidence_sha,
        mime_type="text/plain",
        r2_object_key=f"youtube/{video_id}/{evidence_sha}.txt",
        r2_text_key=f"youtube/{video_id}/{evidence_sha}.txt",
        page_count=1,
    )

    idea_id = repo.create_idea(
        IdeaSpec(
            source_type="youtube",
            source_url=normalized_url,
            title=(str(metadata.get("title", "")).strip() or f"YouTube {video_id}")[:220],
            raw_text=summary,
            status="new",
            priority=int(ingest_cfg["idea_priority"]),
            created_by=str(ingest_cfg["created_by"]),
            metadata={
                "video_id": video_id,
                "video_title": str(metadata.get("title", "")).strip(),
                "channel_title": str(metadata.get("channel_title", "")).strip(),
                "published_at": metadata.get("published_at"),
                "claim_hash": url_hash,
                "summary": summary,
                "ingested_at": now_utc.isoformat(),
                "command": command,
                "comment_count": len(comments),
                "claim_count": len(extracted_claims),
                "edge_count": len(extracted_edges),
                "claims": extracted_claims,
                "extracted_edges": extracted_edges,
                "storage_unit": "url",
            },
        )
    )

    repo.insert_idea_evidence(
        IdeaEvidenceSpec(
            idea_id=idea_id,
            doc_version_id=doc_version_id,
            excerpt=summary[:1000],
            locator={
                "source_url": normalized_url,
                "video_id": video_id,
                "claim_hash": url_hash,
            },
        )
    )

    repo.enqueue_agent_task(
        task_type="idea_analysis",
        payload={
            "idea_id": idea_id,
            "source_type": "youtube",
            "source_url": normalized_url,
            "video_id": video_id,
            "claim_hash": url_hash,
            "claim_text": summary,
            "ticker_candidates": sorted(
                {
                    ticker
                    for claim in extracted_claims
                    for ticker in [str(x).strip().upper() for x in claim.get("ticker_candidates", []) if str(x).strip()]
                }
            )[:20],
            "causal_hypothesis": "URL-level ingest summary",
            "summary": summary,
            "extracted_edges": extracted_edges,
        },
        priority=int(ingest_cfg["task_priority"]),
    )

    return {
        "enabled": True,
        "video_id": video_id,
        "source_url": normalized_url,
        "idea_id": idea_id,
        "doc_version_id": doc_version_id,
        "inserted_ideas": 1,
        "inserted_evidence": 1,
        "enqueued_tasks": 1,
        "deduped_claims": 0,
        "comment_count": len(comments),
        "edge_count": len(extracted_edges),
        "summary": summary,
    }
