from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


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
