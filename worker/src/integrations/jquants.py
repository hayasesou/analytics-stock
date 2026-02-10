from __future__ import annotations

import requests


class JQuantsClient:
    def __init__(self, email: str | None, password: str | None):
        self.email = email
        self.password = password

    def available(self) -> bool:
        return bool(self.email and self.password)

    def fetch(self, endpoint: str, params: dict | None = None) -> dict:
        if not self.available():
            raise RuntimeError("J-Quants credentials are not set")
        # Placeholder for real implementation.
        resp = requests.get(endpoint, params=params or {}, timeout=10)
        resp.raise_for_status()
        return resp.json()
