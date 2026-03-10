from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import requests

from src.config import RuntimeSecrets
from src.data.provider_events import ProviderEventsMixin
from src.data.provider_master import ProviderMasterMixin
from src.data.provider_prices import ProviderPriceMixin
from src.integrations.edinet import EdinetClient
from src.integrations.jquants import JQuantsClient
from src.integrations.massive import MassiveClient
from src.integrations.sec import SecEdgarClient


@dataclass
class HybridDataProvider(ProviderMasterMixin, ProviderPriceMixin, ProviderEventsMixin):
    secrets: RuntimeSecrets
    seed: int = 42
    allow_mock_price_fallback: bool = False

    def _rng(self) -> np.random.Generator:
        return np.random.default_rng(self.seed)

    def _make_jquants_client(self):
        return JQuantsClient(
            api_key=self.secrets.jquants_api_key,
            email=self.secrets.jquants_email,
            password=self.secrets.jquants_password,
        )

    def _make_massive_client(self):
        return MassiveClient(self.secrets.massive_api_key)

    def _make_sec_client(self):
        return SecEdgarClient(self.secrets.sec_user_agent)

    def _make_edinet_client(self):
        return EdinetClient(self.secrets.edinet_api_key)

    def _http_get(self, url: str, timeout: int = 20):
        return requests.get(url, timeout=timeout)
