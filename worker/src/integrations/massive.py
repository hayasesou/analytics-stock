from __future__ import annotations

import requests


class MassiveClient:
    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def available(self) -> bool:
        return bool(self.api_key)

    def fetch(self, endpoint: str, params: dict | None = None) -> dict:
        if not self.available():
            raise RuntimeError("Massive API key is not set")
        headers = {"Authorization": f"Bearer {self.api_key}"}
        resp = requests.get(endpoint, headers=headers, params=params or {}, timeout=10)
        resp.raise_for_status()
        return resp.json()
