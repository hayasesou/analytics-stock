from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from typing import Any

from src.integrations.youtube import extract_video_id, normalize_youtube_url
from src.jobs.ingest_youtube_support import (
    _build_source_blob,
    _build_summary,
    _extract_arb_edges,
    _extract_claims,
    _parse_datetime_or_none,
    _parse_url_from_inputs,
    _resolve_ingest_cfg,
)
from src.types import IdeaEvidenceSpec, IdeaSpec


def run_ingest_youtube_impl(
    *,
    command: str | None = None,
    url: str | None = None,
    now: datetime | None = None,
    load_yaml_config_fn,
    load_runtime_secrets_fn,
    NeonRepository_cls,
    YouTubeClient_cls,
) -> dict[str, Any]:
    cfg = load_yaml_config_fn()
    ingest_cfg = _resolve_ingest_cfg(cfg)
    if not bool(ingest_cfg["enabled"]):
        return {"enabled": False, "inserted_ideas": 0, "enqueued_tasks": 0, "deduped_claims": 0}
    normalized_url = normalize_youtube_url(_parse_url_from_inputs(command, url))
    video_id = extract_video_id(normalized_url)
    now_utc = now.astimezone(timezone.utc) if now and now.tzinfo else (now.replace(tzinfo=timezone.utc) if now else datetime.now(timezone.utc))
    repo = NeonRepository_cls(load_runtime_secrets_fn().database_url)
    youtube_client = YouTubeClient_cls(api_key=str(os.getenv("YOUTUBE_API_KEY", "")).strip(), timeout_sec=float(ingest_cfg["request_timeout_sec"]), retry_count=int(ingest_cfg["retry_count"]))
    bundle = youtube_client.fetch_video_bundle(normalized_url, max_comments=int(ingest_cfg["max_comments"]), max_comment_pages=int(ingest_cfg["max_comment_pages"]), max_comment_duration_sec=float(ingest_cfg["max_comment_duration_sec"]))
    metadata = bundle.get("metadata") if isinstance(bundle.get("metadata"), dict) else {}
    comments = bundle.get("comments") if isinstance(bundle.get("comments"), list) else []
    transcripts = bundle.get("transcript") if isinstance(bundle.get("transcript"), list) else []
    source_blob = _build_source_blob(metadata, comments, transcripts)
    summary = _build_summary(metadata, comments, transcripts)
    extracted_claims = _extract_claims(metadata=metadata, source_blob=source_blob, max_claims=int(ingest_cfg["max_claims"]), min_claim_chars=int(ingest_cfg["min_claim_chars"]))
    extracted_edges = _extract_arb_edges(metadata=metadata, claims=extracted_claims, comments=comments)
    existing_hashes = repo.fetch_idea_claim_hashes_by_source_url(source_type="youtube", source_url=normalized_url, limit=5000)
    url_hash = hashlib.sha256(f"url:{normalized_url}".encode("utf-8")).hexdigest()
    if url_hash in existing_hashes:
        return {"enabled": True, "video_id": video_id, "source_url": normalized_url, "inserted_ideas": 0, "inserted_evidence": 0, "enqueued_tasks": 0, "deduped_claims": len(extracted_claims), "summary": summary, "comment_count": len(comments), "edge_count": len(extracted_edges)}
    evidence_payload = "\n\n".join([summary, source_blob]).strip()
    evidence_sha = hashlib.sha256(evidence_payload.encode("utf-8")).hexdigest()
    doc_version_id = repo.upsert_document_with_version(external_doc_id=f"youtube:{video_id}", source_system="youtube", source_url=normalized_url, title=str(metadata.get("title", "")).strip() or f"YouTube {video_id}", published_at=_parse_datetime_or_none(str(metadata.get("published_at", "")).strip()), retrieved_at=now_utc, sha256=evidence_sha, mime_type="text/plain", r2_object_key=f"youtube/{video_id}/{evidence_sha}.txt", r2_text_key=f"youtube/{video_id}/{evidence_sha}.txt", page_count=1)
    idea_id = repo.create_idea(IdeaSpec(source_type="youtube", source_url=normalized_url, title=(str(metadata.get("title", "")).strip() or f"YouTube {video_id}")[:220], raw_text=summary, status="new", priority=int(ingest_cfg["idea_priority"]), created_by=str(ingest_cfg["created_by"]), metadata={"video_id": video_id, "video_title": str(metadata.get("title", "")).strip(), "channel_title": str(metadata.get("channel_title", "")).strip(), "published_at": metadata.get("published_at"), "claim_hash": url_hash, "summary": summary, "ingested_at": now_utc.isoformat(), "command": command, "comment_count": len(comments), "claim_count": len(extracted_claims), "edge_count": len(extracted_edges), "claims": extracted_claims, "extracted_edges": extracted_edges, "storage_unit": "url"}))
    repo.insert_idea_evidence(IdeaEvidenceSpec(idea_id=idea_id, doc_version_id=doc_version_id, excerpt=summary[:1000], locator={"source_url": normalized_url, "video_id": video_id, "claim_hash": url_hash}))
    repo.enqueue_agent_task(task_type="idea_analysis", payload={"idea_id": idea_id, "source_type": "youtube", "source_url": normalized_url, "video_id": video_id, "claim_hash": url_hash, "claim_text": summary, "ticker_candidates": sorted({ticker for claim in extracted_claims for ticker in [str(item).strip().upper() for item in claim.get("ticker_candidates", []) if str(item).strip()]})[:20], "causal_hypothesis": "URL-level ingest summary", "summary": summary, "extracted_edges": extracted_edges}, priority=int(ingest_cfg["task_priority"]))
    return {"enabled": True, "video_id": video_id, "source_url": normalized_url, "idea_id": idea_id, "doc_version_id": doc_version_id, "inserted_ideas": 1, "inserted_evidence": 1, "enqueued_tasks": 1, "deduped_claims": 0, "comment_count": len(comments), "edge_count": len(extracted_edges), "summary": summary}


__all__ = ["run_ingest_youtube_impl"]
