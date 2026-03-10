from __future__ import annotations

from src.storage.db_ingest_crypto import NeonRepositoryIngestCryptoMixin
from src.storage.db_ingest_lifecycle import NeonRepositoryIngestLifecycleMixin
from src.storage.db_ingest_market import NeonRepositoryIngestMarketMixin


class NeonRepositoryIngestMixin(
    NeonRepositoryIngestLifecycleMixin,
    NeonRepositoryIngestMarketMixin,
    NeonRepositoryIngestCryptoMixin,
):
    pass
