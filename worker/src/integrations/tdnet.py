from __future__ import annotations

import re

import requests


class TdnetScraper:
    def fetch_html(self, url: str) -> str:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.text

    def extract_links(self, html: str) -> list[str]:
        return re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE)
