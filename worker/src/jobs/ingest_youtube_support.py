from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any

from src.integrations.discord import parse_ingest_youtube_command


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
    return [cleaned for cleaned in (_collapse_whitespace(part) for part in re.split(r"[。！？!?\n]+", str(text or ""))) if cleaned]


_TICKER_STOPWORDS = {"THE", "THIS", "THAT", "WITH", "FROM", "WILL", "YOUR", "HAVE", "ABOUT", "VIDEO", "YOUTUBE", "COMMENT", "SHORTS", "NEWS", "USD"}
_ARBITRAGE_KEYWORDS = {"arb", "arbitrage", "basis", "funding", "spread", "乖離", "裁定", "歪み", "サヤ"}


def _extract_ticker_candidates(text: str, *, max_items: int = 5) -> list[str]:
    candidates: list[str] = []
    for match in re.finditer(r"(?<![A-Z0-9])\$?([A-Z]{1,5})(?![A-Z0-9])", str(text).upper()):
        ticker = match.group(1).strip()
        if ticker and ticker not in _TICKER_STOPWORDS and ticker not in candidates:
            candidates.append(ticker)
        if len(candidates) >= max_items:
            break
    return candidates


def _build_causal_hypothesis(claim_text: str, tickers: list[str]) -> str:
    lowered = str(claim_text).lower()
    symbol_block = ", ".join(tickers) if tickers else "関連銘柄"
    if any(keyword in lowered for keyword in ["増", "改善", "成長", "上方", "追い風", "expand", "growth", "beat"]):
        return f"{symbol_block} の業績・需給改善が続く場合、相対リターンが上振れする仮説。"
    if any(keyword in lowered for keyword in ["減", "悪化", "下方", "逆風", "懸念", "decline", "miss", "slowdown"]):
        return f"{symbol_block} の業績・需給悪化が続く場合、相対リターンが下振れする仮説。"
    return f"{symbol_block} に対してニュースが需給へ波及し、価格変動を生む仮説。"


def _build_summary(metadata: dict[str, Any], comments: list[dict[str, Any]], transcripts: list[dict[str, Any]]) -> str:
    title = _collapse_whitespace(str(metadata.get("title", "")))
    channel = _collapse_whitespace(str(metadata.get("channel_title", "")))
    description = _collapse_whitespace(str(metadata.get("description", "")))
    comment_lines = [_collapse_whitespace(str((item or {}).get("text", ""))) for item in comments[:3]]
    transcript_lines = [_collapse_whitespace(str((item or {}).get("text", ""))) for item in transcripts[:2]]
    chunks = [x for x in [f"title={title}" if title else "", f"channel={channel}" if channel else "", f"description={description[:280]}" if description else "", f"comments={' | '.join([x for x in comment_lines if x][:3])}" if any(comment_lines) else "", f"transcript={' | '.join([x for x in transcript_lines if x][:2])}" if any(transcript_lines) else ""] if x]
    return " / ".join(chunks)[:1200]


def _build_source_blob(metadata: dict[str, Any], comments: list[dict[str, Any]], transcripts: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    if metadata:
        lines.extend([f"title: {metadata.get('title', '')}", f"channel: {metadata.get('channel_title', '')}", f"url: {metadata.get('url', '')}", f"description: {metadata.get('description', '')}"])
    if comments:
        lines.append("comments:")
        lines.extend([f"- {text}" for text in (_collapse_whitespace(str((item or {}).get('text', ''))) for item in comments) if text])
    if transcripts:
        lines.append("transcript:")
        lines.extend([f"- {text}" for text in (_collapse_whitespace(str((item or {}).get('text', ''))) for item in transcripts) if text])
    return "\n".join(lines).strip()


def _claim_hash(claim_text: str) -> str:
    return hashlib.sha256(_collapse_whitespace(claim_text).lower().encode("utf-8")).hexdigest()


def _extract_claims(*, metadata: dict[str, Any], source_blob: str, max_claims: int, min_claim_chars: int) -> list[dict[str, Any]]:
    base_text = "\n".join([_collapse_whitespace(str(metadata.get("title", ""))), _collapse_whitespace(str(metadata.get("description", ""))), source_blob]).strip()
    sentences = _split_sentences(base_text)
    if not sentences:
        return []
    seen: set[str] = set()
    claims: list[dict[str, Any]] = []
    global_tickers = _extract_ticker_candidates(f"{metadata.get('title', '')} {metadata.get('description', '')}", max_items=5)
    for sentence in sentences:
        if len(sentence) < max(1, int(min_claim_chars)):
            continue
        normalized = sentence.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        tickers = _extract_ticker_candidates(sentence, max_items=5) or list(global_tickers)
        claims.append({"claim_text": sentence, "evidence_excerpt": sentence[:600], "ticker_candidates": tickers, "causal_hypothesis": _build_causal_hypothesis(sentence, tickers), "claim_hash": _claim_hash(sentence)})
        if len(claims) >= max_claims:
            break
    return claims


def _detect_venues(text: str) -> list[str]:
    lowered = str(text).lower()
    mapping = [("BINANCE", ["binance"]), ("HYPERLIQUID", ["hyperliquid", "hyper liquid"]), ("BYBIT", ["bybit"]), ("OKX", ["okx"])]
    return [venue for venue, keys in mapping if any(key in lowered for key in keys)]


def _extract_arb_edges(*, metadata: dict[str, Any], claims: list[dict[str, Any]], comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    title = _collapse_whitespace(str(metadata.get("title", "")))
    description = _collapse_whitespace(str(metadata.get("description", "")))
    venue_hint = _detect_venues(f"{title} {description}")
    edges: list[dict[str, Any]] = []
    for claim in claims:
        claim_text = _collapse_whitespace(str(claim.get("claim_text", "")))
        if not claim_text:
            continue
        lowered = claim_text.lower()
        tickers = [str(item).strip().upper() for item in claim.get("ticker_candidates", []) if str(item).strip()]
        venues = _detect_venues(claim_text) or list(venue_hint)
        priority = 0.45
        if any(key in lowered for key in _ARBITRAGE_KEYWORDS):
            priority += 0.25
        if len(venues) >= 2:
            priority += 0.15
        if tickers:
            priority += min(0.10, 0.03 * len(tickers))
        evidence = []
        for item in comments:
            if not isinstance(item, dict):
                continue
            comment_text = _collapse_whitespace(str(item.get("text", "")))
            if not comment_text:
                continue
            comment_lower = comment_text.lower()
            if not (any(key in comment_lower for key in _ARBITRAGE_KEYWORDS) or any(ticker.lower() in comment_lower for ticker in tickers)):
                continue
            evidence.append({"comment_id": str(item.get("comment_id", "")).strip() or None, "text": comment_text[:260], "like_count": int(item.get("like_count", 0) or 0)})
            if len(evidence) >= 3:
                break
        if evidence:
            priority += min(0.10, 0.03 * len(evidence))
        edges.append(
            {
                "edge_type": "crypto_arb" if (len(venues) >= 2 or any(key in lowered for key in _ARBITRAGE_KEYWORDS)) else "market_watch",
                "venues": venues,
                "instrument": f"{tickers[0]}-PERP" if tickers else "UNKNOWN-PERP",
                "claim": claim_text,
                "ticker_candidates": tickers,
                "evidence": evidence,
                "data_requirements": ["mid_price", "spread_bps", "funding_rate", "orderbook_top"],
                "validation_plan": ["spread_zscore", "cost_model_with_fees_slippage", "execution_latency_and_fill_rate"],
                "priority": round(min(0.99, max(0.05, priority)), 4),
            }
        )
    edges.sort(key=lambda row: float(row.get("priority", 0.0)), reverse=True)
    return edges


__all__ = [
    "_build_source_blob",
    "_build_summary",
    "_claim_hash",
    "_collapse_whitespace",
    "_detect_venues",
    "_extract_arb_edges",
    "_extract_claims",
    "_extract_ticker_candidates",
    "_parse_datetime_or_none",
    "_parse_url_from_inputs",
    "_resolve_ingest_cfg",
    "_split_sentences",
    "_to_float",
    "_to_int",
]
