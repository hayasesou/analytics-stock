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
