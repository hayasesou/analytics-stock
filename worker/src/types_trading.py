from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


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
