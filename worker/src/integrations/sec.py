from __future__ import annotations

import requests


class SecEdgarClient:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent

    def fetch(self, url: str) -> dict:
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
