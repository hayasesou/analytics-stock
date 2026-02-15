from __future__ import annotations

import requests


class SecEdgarClient:
    COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

    def __init__(self, user_agent: str):
        self.user_agent = user_agent

    def fetch(self, url: str) -> dict:
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def fetch_company_tickers_exchange(self) -> list[dict[str, str]]:
        payload = self.fetch(self.COMPANY_TICKERS_EXCHANGE_URL)
        fields = payload.get("fields")
        data = payload.get("data")
        if not isinstance(fields, list) or not isinstance(data, list):
            raise RuntimeError("SEC company_tickers_exchange.json schema mismatch")

        normalized_fields = [str(f) for f in fields]
        rows: list[dict[str, str]] = []
        for item in data:
            if not isinstance(item, list):
                continue
            mapped = {
                normalized_fields[i]: str(item[i]) if i < len(item) and item[i] is not None else ""
                for i in range(len(normalized_fields))
            }
            rows.append(mapped)
        return rows
