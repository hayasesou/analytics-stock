from __future__ import annotations

from src.storage.db_trading_execution import NeonRepositoryTradingExecutionMixin
from src.storage.db_trading_fundamentals import NeonRepositoryTradingFundamentalsMixin
from src.storage.db_trading_risk import NeonRepositoryTradingRiskMixin


class NeonRepositoryTradingMixin(
    NeonRepositoryTradingRiskMixin,
    NeonRepositoryTradingExecutionMixin,
    NeonRepositoryTradingFundamentalsMixin,
):
    pass
