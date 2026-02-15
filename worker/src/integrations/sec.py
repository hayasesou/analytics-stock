from __future__ import annotations

from html import unescape
import re

import requests
from xml.etree import ElementTree


class SecEdgarClient:
    COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
    CURRENT_FILINGS_ATOM_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

    _ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
    _TAG_RE = re.compile(r"<[^>]+>")
    _WS_RE = re.compile(r"\s+")

    def __init__(self, user_agent: str):
        self.user_agent = user_agent

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(url, headers=headers, params=params or {}, timeout=10)
        resp.raise_for_status()
        return resp

    def fetch(self, url: str) -> dict:
        resp = self._get(url)
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

    @classmethod
    def _compact_text(cls, text: str) -> str:
        without_tags = cls._TAG_RE.sub(" ", unescape(text or ""))
        return cls._WS_RE.sub(" ", without_tags).strip()

    @staticmethod
    def _extract_form_type(title: str, category_term: str) -> str:
        term = (category_term or "").strip()
        if term:
            return term
        text = (title or "").strip()
        if " - " in text:
            return text.split(" - ", 1)[0].strip()
        return text

    @staticmethod
    def _extract_company_name(title: str) -> str:
        text = (title or "").strip()
        if " - " in text:
            text = text.split(" - ", 1)[1].strip()
        # Drop trailing CIK / role markers in SEC title
        text = re.sub(r"\s+\(\d{4,}\).*$", "", text).strip()
        return text

    def fetch_current_filings(self, count: int = 100) -> list[dict[str, str]]:
        resp = self._get(
            self.CURRENT_FILINGS_ATOM_URL,
            params={"action": "getcurrent", "count": int(max(1, min(count, 100))), "output": "atom"},
        )
        root = ElementTree.fromstring(resp.text)
        rows: list[dict[str, str]] = []
        for entry in root.findall("atom:entry", self._ATOM_NS):
            title = (entry.findtext("atom:title", "", self._ATOM_NS) or "").strip()
            updated = (entry.findtext("atom:updated", "", self._ATOM_NS) or "").strip()
            summary_raw = (entry.findtext("atom:summary", "", self._ATOM_NS) or "").strip()

            link = ""
            link_node = entry.find("atom:link[@rel='alternate']", self._ATOM_NS)
            if link_node is None:
                link_node = entry.find("atom:link", self._ATOM_NS)
            if link_node is not None:
                link = str(link_node.attrib.get("href", "")).strip()

            category_term = ""
            category_node = entry.find("atom:category", self._ATOM_NS)
            if category_node is not None:
                category_term = str(category_node.attrib.get("term", "")).strip()

            rows.append(
                {
                    "title": title,
                    "updated": updated,
                    "form_type": self._extract_form_type(title, category_term),
                    "company_name": self._extract_company_name(title),
                    "summary": self._compact_text(summary_raw),
                    "source_url": link,
                }
            )
        return rows
