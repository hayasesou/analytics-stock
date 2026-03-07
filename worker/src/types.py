from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class Security:
    security_id: str
    market: str
    ticker: str
    name: str
    sector: str | None = None
    industry: str | None = None
    currency: str = "JPY"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventItem:
    event_type: str
    importance: str
    event_time: datetime
    title: str
    summary: str
    source_url: str | None = None
    security_id: str | None = None
    doc_version_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CitationItem:
    claim_id: str
    doc_version_id: str
    page_ref: str | None
    quote_text: str


@dataclass
class ReportItem:
    report_type: str
    title: str
    body_md: str
    conclusion: str
    falsification_conditions: str
    confidence: str | None
    security_id: str | None = None
    claims: list[dict[str, str]] = field(default_factory=list)
    citations: list[CitationItem] = field(default_factory=list)


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


@dataclass
class BacktestTrade:
    security_id: str
    market: str
    entry_date: date
    entry_price: float
    exit_date: date
    exit_price: float
    quantity: float
    gross_pnl: float
    net_pnl: float
    cost: float
    exit_reason: str


@dataclass
class BacktestResult:
    cost_profile: str
    metrics: dict[str, float]
    equity_curve: list[dict[str, Any]]
    trades: list[BacktestTrade]


@dataclass
class StrategySpec:
    name: str
    asset_scope: str
    status: str
    description: str | None = None


@dataclass
class StrategyVersionSpec:
    strategy_name: str
    version: int
    spec: dict[str, Any]
    code_artifact_key: str | None = None
    sha256: str | None = None
    created_by: str | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    is_active: bool = False


@dataclass
class StrategyEvaluation:
    strategy_version_id: str
    eval_type: str
    period_start: date
    period_end: date
    metrics: dict[str, Any]
    artifacts: dict[str, Any] = field(default_factory=dict)


@dataclass
class PortfolioSpec:
    name: str
    base_currency: str
    broker_map: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderIntent:
    portfolio_id: str
    as_of: datetime
    target_positions: list[dict[str, Any]]
    status: str
    reason: str | None = None
    risk_checks: dict[str, Any] = field(default_factory=dict)
    strategy_version_id: str | None = None
    approved_at: datetime | None = None
    approved_by: str | None = None


@dataclass
class OrderRecord:
    broker: str
    symbol: str
    instrument_type: str
    side: str
    order_type: str
    qty: float
    status: str
    idempotency_key: str
    account_id: str | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    time_in_force: str = "DAY"
    broker_order_id: str | None = None
    submitted_at: datetime | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    intent_id: str | None = None


@dataclass
class FillRecord:
    order_id: str
    fill_time: datetime
    qty: float
    price: float
    fee: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class PositionRecord:
    portfolio_id: str
    symbol: str
    instrument_type: str
    qty: float
    avg_price: float | None = None
    last_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None


@dataclass
class RiskSnapshot:
    portfolio_id: str
    as_of: datetime
    equity: float
    drawdown: float
    state: str
    sharpe_20d: float | None = None
    gross_exposure: float | None = None
    net_exposure: float | None = None
    triggers: dict[str, Any] = field(default_factory=dict)


@dataclass
class FundamentalSnapshot:
    security_id: str
    as_of_date: date
    rating: str
    summary: str
    snapshot: dict[str, Any]
    source: str = "llm"
    confidence: str | None = None
    created_by: str | None = None


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

        aliases = {
            "neutral_ok": "delta_neutral_ok",
            "neutral_reason": "delta_neutral_reason",
        }
        normalized = dict(value)
        for source_key, target_key in aliases.items():
            if target_key not in normalized and source_key in normalized:
                normalized[target_key] = normalized.get(source_key)

        known_keys = {
            "liquidity_score",
            "min_liquidity_score",
            "liquidation_distance_pct",
            "min_liquidation_distance_pct",
            "delta_neutral_ok",
            "delta_neutral_reason",
            "missing_ratio",
            "primary_source_count",
            "has_major_contradiction",
            "status",
            "eval_type",
            "sharpe",
            "max_dd",
            "cagr",
            "entry_block_reason",
            "extra",
        }
        extra_payload = {
            key: item
            for key, item in normalized.items()
            if key not in known_keys
        }

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
            delta_neutral_reason=(
                str(normalized.get("delta_neutral_reason"))
                if normalized.get("delta_neutral_reason") is not None
                else None
            ),
            missing_ratio=_to_optional_float(normalized.get("missing_ratio")),
            primary_source_count=_to_optional_int(normalized.get("primary_source_count")),
            has_major_contradiction=_to_optional_bool(normalized.get("has_major_contradiction")),
            status=str(normalized.get("status")) if normalized.get("status") is not None else None,
            eval_type=str(normalized.get("eval_type")) if normalized.get("eval_type") is not None else None,
            sharpe=_to_optional_float(normalized.get("sharpe")),
            max_dd=_to_optional_float(normalized.get("max_dd")),
            cagr=_to_optional_float(normalized.get("cagr")),
            entry_block_reason=(
                str(normalized.get("entry_block_reason"))
                if normalized.get("entry_block_reason") is not None
                else None
            ),
            extra=extra_payload,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
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
        return payload


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


@dataclass
class StrategyRiskEvent:
    strategy_version_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    triggered_at: datetime | None = None


@dataclass
class StrategyRiskSnapshot:
    strategy_version_id: str
    as_of: datetime
    drawdown: float | None
    sharpe_20d: float | None
    state: str
    trigger_flags: dict[str, Any] = field(default_factory=dict)
    cooldown_until: datetime | None = None


@dataclass
class StrategyLifecycleReview:
    strategy_id: str
    strategy_version_id: str | None
    action: str
    from_status: str
    to_status: str
    live_candidate: bool = False
    reason: str | None = None
    recheck_condition: str | None = None
    recheck_after: date | None = None
    acted_by: str = "system"
    acted_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CryptoMarketSnapshot:
    exchange: str
    symbol: str
    market_type: str
    observed_at: datetime
    best_bid: float | None = None
    best_ask: float | None = None
    mid: float | None = None
    spread_bps: float | None = None
    funding_rate: float | None = None
    open_interest: float | None = None
    mark_price: float | None = None
    index_price: float | None = None
    basis_bps: float | None = None
    source_mode: str = "rest"
    latency_ms: float | None = None
    data_quality: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class CryptoDataQualitySnapshot:
    exchange: str
    symbol: str
    market_type: str
    window_start: datetime
    window_end: datetime
    sample_count: int
    missing_count: int
    missing_ratio: float
    latency_p95_ms: float | None = None
    ws_failover_count: int = 0
    eligible_for_edge: bool = True
    details: dict[str, Any] = field(default_factory=dict)
