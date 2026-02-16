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
