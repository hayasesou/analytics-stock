from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class EdgeRisk:
    liquidity_score: float | None = None
    min_liquidity_score: float | None = None
    liquidation_distance_pct: float | None = None
    min_liquidation_distance_pct: float | None = None
    delta_neutral_ok: bool | None = None
    delta_neutral_reason: str | None = None
    missing_ratio: float | None = None
    primary_source_count: int | None = None
    has_major_contradiction: bool | None = None
    status: str | None = None
    eval_type: str | None = None
    sharpe: float | None = None
    max_dd: float | None = None
    cagr: float | None = None
    entry_block_reason: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_mapping(value: Any) -> "EdgeRisk":
        if isinstance(value, EdgeRisk):
            return value
        if not isinstance(value, dict):
            return EdgeRisk()
        aliases = {"neutral_ok": "delta_neutral_ok", "neutral_reason": "delta_neutral_reason"}
        normalized = dict(value)
        for source_key, target_key in aliases.items():
            if target_key not in normalized and source_key in normalized:
                normalized[target_key] = normalized.get(source_key)
        known_keys = {
            "liquidity_score", "min_liquidity_score", "liquidation_distance_pct", "min_liquidation_distance_pct",
            "delta_neutral_ok", "delta_neutral_reason", "missing_ratio", "primary_source_count",
            "has_major_contradiction", "status", "eval_type", "sharpe", "max_dd", "cagr", "entry_block_reason", "extra",
        }
        extra_payload = {key: item for key, item in normalized.items() if key not in known_keys}
        raw_extra = normalized.get("extra")
        if isinstance(raw_extra, dict):
            extra_payload = {**raw_extra, **extra_payload}

        def _to_optional_float(raw: Any) -> float | None:
            if raw is None:
                return None
            try:
                return float(raw)
            except (TypeError, ValueError):
                return None

        def _to_optional_int(raw: Any) -> int | None:
            if raw is None:
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        def _to_optional_bool(raw: Any) -> bool | None:
            if raw is None:
                return None
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in {"true", "1", "yes", "on"}:
                return True
            if text in {"false", "0", "no", "off"}:
                return False
            return None

        return EdgeRisk(
            liquidity_score=_to_optional_float(normalized.get("liquidity_score")),
            min_liquidity_score=_to_optional_float(normalized.get("min_liquidity_score")),
            liquidation_distance_pct=_to_optional_float(normalized.get("liquidation_distance_pct")),
            min_liquidation_distance_pct=_to_optional_float(normalized.get("min_liquidation_distance_pct")),
            delta_neutral_ok=_to_optional_bool(normalized.get("delta_neutral_ok")),
            delta_neutral_reason=str(normalized.get("delta_neutral_reason")) if normalized.get("delta_neutral_reason") is not None else None,
            missing_ratio=_to_optional_float(normalized.get("missing_ratio")),
            primary_source_count=_to_optional_int(normalized.get("primary_source_count")),
            has_major_contradiction=_to_optional_bool(normalized.get("has_major_contradiction")),
            status=str(normalized.get("status")) if normalized.get("status") is not None else None,
            eval_type=str(normalized.get("eval_type")) if normalized.get("eval_type") is not None else None,
            sharpe=_to_optional_float(normalized.get("sharpe")),
            max_dd=_to_optional_float(normalized.get("max_dd")),
            cagr=_to_optional_float(normalized.get("cagr")),
            entry_block_reason=str(normalized.get("entry_block_reason")) if normalized.get("entry_block_reason") is not None else None,
            extra=extra_payload,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "liquidity_score": self.liquidity_score,
            "min_liquidity_score": self.min_liquidity_score,
            "liquidation_distance_pct": self.liquidation_distance_pct,
            "min_liquidation_distance_pct": self.min_liquidation_distance_pct,
            "delta_neutral_ok": self.delta_neutral_ok,
            "delta_neutral_reason": self.delta_neutral_reason,
            "missing_ratio": self.missing_ratio,
            "primary_source_count": self.primary_source_count,
            "has_major_contradiction": self.has_major_contradiction,
            "status": self.status,
            "eval_type": self.eval_type,
            "sharpe": self.sharpe,
            "max_dd": self.max_dd,
            "cagr": self.cagr,
            "entry_block_reason": self.entry_block_reason,
            "extra": dict(self.extra),
        }


@dataclass
class EdgeState:
    strategy_name: str
    market_scope: str
    symbol: str
    observed_at: datetime
    edge_score: float
    strategy_version_id: str | None = None
    expected_net_edge: float | None = None
    distance_to_entry: float | None = None
    expected_net_edge_bps: float | None = None
    distance_to_entry_bps: float | None = None
    confidence: float | None = None
    risk_json: EdgeRisk | dict[str, Any] = field(default_factory=EdgeRisk)
    risk: EdgeRisk | dict[str, Any] = field(default_factory=EdgeRisk)
    explain: str | None = None
    market_regime: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class IdeaSpec:
    source_type: str
    title: str
    raw_text: str
    source_url: str | None = None
    status: str = "new"
    priority: int = 100
    created_by: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class IdeaEvidenceSpec:
    idea_id: str
    doc_version_id: str
    excerpt: str
    locator: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentSpec:
    idea_id: str
    hypothesis: str
    strategy_version_id: str | None = None
    eval_status: str = "queued"
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)


@dataclass
class LessonSpec:
    idea_id: str
    lesson_type: str
    summary: str
    experiment_id: str | None = None
    reusable_checklist: dict[str, Any] = field(default_factory=dict)
