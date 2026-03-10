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
