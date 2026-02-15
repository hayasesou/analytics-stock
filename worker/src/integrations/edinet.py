from __future__ import annotations

import requests


class EdinetClient:
    DOCUMENTS_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def available(self) -> bool:
        return bool(self.api_key)

    def fetch_documents(self, endpoint: str, params: dict | None = None) -> dict:
        if not self.available():
            raise RuntimeError("EDINET API key is not set")
        query = dict(params or {})
        query["Subscription-Key"] = self.api_key
        resp = requests.get(endpoint, params=query, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def fetch_documents_list(self, date_yyyy_mm_dd: str) -> list[dict]:
        payload = self.fetch_documents(
            self.DOCUMENTS_URL,
            params={
                "date": date_yyyy_mm_dd,
                "type": 2,
            },
        )
        rows = payload.get("results")
        if not isinstance(rows, list):
            raise RuntimeError("EDINET documents response missing results[]")
        return [r for r in rows if isinstance(r, dict)]
