from __future__ import annotations

from src.storage.db_base import NeonRepositoryBase
from src.storage.db_chat import NeonRepositoryChatMixin
from src.storage.db_ideas import NeonRepositoryIdeasMixin
from src.storage.db_ingest import NeonRepositoryIngestMixin
from src.storage.db_portfolio_queries import NeonRepositoryPortfolioQueriesMixin
from src.storage.db_read_queries import NeonRepositoryReadQueriesMixin
from src.storage.db_reports import NeonRepositoryReportsMixin
from src.storage.db_research_write import NeonRepositoryResearchWriteMixin
from src.storage.db_scores import NeonRepositoryScoresMixin
from src.storage.db_strategy import NeonRepositoryStrategyMixin
from src.storage.db_trading import NeonRepositoryTradingMixin


class NeonRepository(
    NeonRepositoryBase,
    NeonRepositoryIngestMixin,
    NeonRepositoryScoresMixin,
    NeonRepositoryReportsMixin,
    NeonRepositoryStrategyMixin,
    NeonRepositoryIdeasMixin,
    NeonRepositoryTradingMixin,
    NeonRepositoryResearchWriteMixin,
    NeonRepositoryChatMixin,
    NeonRepositoryPortfolioQueriesMixin,
    NeonRepositoryReadQueriesMixin,
):
    pass
