from __future__ import annotations

from src.storage.db_reports_backtest import NeonRepositoryReportBacktestMixin
from src.storage.db_reports_documents import NeonRepositoryReportDocumentsMixin
from src.storage.db_reports_read import NeonRepositoryReportReadMixin
from src.storage.db_reports_write import NeonRepositoryReportWriteMixin


class NeonRepositoryReportsMixin(
    NeonRepositoryReportDocumentsMixin,
    NeonRepositoryReportReadMixin,
    NeonRepositoryReportWriteMixin,
    NeonRepositoryReportBacktestMixin,
):
    pass
