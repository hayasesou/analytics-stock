from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ResearchExternalInput:
    session_id: str
    source_type: str
    raw_text: str | None = None
    extracted_text: str | None = None
    source_url: str | None = None
    message_id: str | None = None
    quality_grade: str | None = None
    extraction_status: str = "queued"
    user_comment: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchHypothesisAssetSpec:
    asset_class: str
    security_id: str | None = None
    symbol_text: str | None = None
    weight_hint: float | None = None
    confidence: float | None = None


@dataclass
class ResearchHypothesisSpec:
    session_id: str
    stance: str
    horizon_days: int
    thesis_md: str
    falsification_md: str
    external_input_id: str | None = None
    parent_message_id: str | None = None
    confidence: float | None = None
    status: str = "draft"
    is_favorite: bool = False
    version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
    assets: list[ResearchHypothesisAssetSpec] = field(default_factory=list)


@dataclass
class ResearchArtifactSpec:
    session_id: str
    artifact_type: str
    title: str
    hypothesis_id: str | None = None
    body_md: str | None = None
    code_text: str | None = None
    language: str | None = None
    is_favorite: bool = False
    created_by_task_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchArtifactRunSpec:
    artifact_id: str
    run_status: str
    stdout_text: str | None = None
    stderr_text: str | None = None
    result_json: dict[str, Any] = field(default_factory=dict)
    output_r2_key: str | None = None


@dataclass
class ResearchHypothesisOutcomeSpec:
    hypothesis_id: str
    checked_at: datetime
    outcome_label: str
    ret_1d: float | None = None
    ret_5d: float | None = None
    ret_20d: float | None = None
    mfe: float | None = None
    mae: float | None = None
    summary_md: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
