from __future__ import annotations

import re

import numpy as np

from src.types import Security

JP_UNIVERSE_LIMIT = 60
US_UNIVERSE_LIMIT = 40
JP_COMMON_MARKET_KEYWORDS = ("プライム", "スタンダード", "グロース", "内国株式", "Prime", "Standard", "Growth")
JP_EXCLUDE_NAME_KEYWORDS = ("ETF", "ETN", "REIT", "投資証券", "インデックス", "指数")
US_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")


class ProviderMasterMixin:
    def load_securities(self, as_of_date) -> list[Security]:  # noqa: ARG002, ANN001
        jp = self._load_jp_securities_live()
        if jp:
            print(f"[provider] jp_master source=jquants count={len(jp)}", flush=True)
        else:
            print("[provider] jp_master source=mock reason=live_unavailable", flush=True)
            jp = self._build_mock_jp_securities()

        us = self._load_us_securities_live()
        if us:
            source = str(us[0].metadata.get("source", "live"))
            print(f"[provider] us_master source={source} count={len(us)}", flush=True)
        else:
            print("[provider] us_master source=mock reason=live_unavailable", flush=True)
            us = self._build_mock_us_securities()
        return jp + us

    def _build_mock_jp_securities(self) -> list[Security]:
        rng = self._rng()
        return [
            Security(
                security_id=f"JP:{code:04d}",
                market="JP",
                ticker=f"{code:04d}",
                name=f"JP Corp {code:04d}",
                sector=rng.choice(["Tech", "Industrial", "Finance", "Health"]),
                currency="JPY",
                metadata={"source": "mock"},
            )
            for code in range(1300, 1360)
        ]

    def _build_mock_us_securities(self) -> list[Security]:
        rng = self._rng()
        return [
            Security(
                security_id=f"US:{idx}",
                market="US",
                ticker=f"US{idx}",
                name=f"US Holdings {idx}",
                sector=rng.choice(["Technology", "Healthcare", "Financials", "Consumer"]),
                currency="USD",
                metadata={"source": "mock"},
            )
            for idx in range(1, 121)
        ]

    @staticmethod
    def _normalize_jp_code(raw_code: object) -> str | None:
        code = str(raw_code or "").strip()
        if not code:
            return None
        if re.fullmatch(r"\d{4}", code):
            return code
        if re.fullmatch(r"\d{5}", code) and code.endswith("0"):
            return code[:4]
        return None

    @staticmethod
    def _is_jp_common_market(market_name: object) -> bool:
        name = str(market_name or "").strip()
        if not name:
            return True
        return any(keyword in name for keyword in JP_COMMON_MARKET_KEYWORDS)

    @staticmethod
    def _is_jp_excluded_name(name: str) -> bool:
        upper_name = name.upper()
        return any(keyword in upper_name for keyword in JP_EXCLUDE_NAME_KEYWORDS)

    def _load_jp_securities_live(self) -> list[Security]:
        client = self._make_jquants_client()
        if not client.available():
            return []
        try:
            rows = client.fetch_listed_info()
        except Exception as exc:  # noqa: BLE001
            print(f"[provider] jp_master_error source=jquants error={exc}", flush=True)
            return []

        by_code: dict[str, Security] = {}
        for row in rows:
            code = self._normalize_jp_code(row.get("Code"))
            if not code or code in by_code:
                continue
            market_name = str(row.get("MarketCodeName") or row.get("MktNm") or "").strip() or None
            if not self._is_jp_common_market(market_name):
                continue
            name = str(
                row.get("CompanyName")
                or row.get("CoName")
                or row.get("CompanyNameEnglish")
                or row.get("CoNameEn")
                or ""
            ).strip()
            if not name or self._is_jp_excluded_name(name):
                continue

            sector = (
                str(
                    row.get("Sector33CodeName")
                    or row.get("S33Nm")
                    or row.get("Sector17CodeName")
                    or row.get("S17Nm")
                    or ""
                ).strip()
                or None
            )
            market_code = str(row.get("MarketCode") or row.get("Mkt") or "").strip() or None
            by_code[code] = Security(
                security_id=f"JP:{code}",
                market="JP",
                ticker=code,
                name=name,
                sector=sector,
                currency="JPY",
                metadata={"source": "jquants", "market_code": market_code, "market_name": market_name},
            )

        return [by_code[code] for code in sorted(by_code.keys())[:JP_UNIVERSE_LIMIT]]

    @staticmethod
    def _build_us_securities_from_massive_rows(rows: list[dict]) -> list[Security]:
        by_ticker: dict[str, Security] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            if not ticker or not name or not US_TICKER_RE.fullmatch(ticker) or ticker in by_ticker:
                continue
            market_cap = row.get("market_cap")
            try:
                market_cap_value = float(market_cap) if market_cap is not None else 0.0
            except (TypeError, ValueError):
                market_cap_value = 0.0
            by_ticker[ticker] = Security(
                security_id="",
                market="US",
                ticker=ticker,
                name=name,
                sector=str(row.get("sic_description") or "").strip() or None,
                currency="USD",
                metadata={
                    "source": "massive",
                    "exchange": str(row.get("primary_exchange") or "").strip() or None,
                    "market_cap": market_cap_value,
                },
            )

        ordered = sorted(
            by_ticker.values(),
            key=lambda sec: (-float(sec.metadata.get("market_cap", 0.0) or 0.0), sec.ticker),
        )
        return [
            Security(
                security_id=f"US:{sec.ticker}",
                market=sec.market,
                ticker=sec.ticker,
                name=sec.name,
                sector=sec.sector,
                currency=sec.currency,
                metadata=sec.metadata,
            )
            for sec in ordered[:US_UNIVERSE_LIMIT]
        ]

    @staticmethod
    def _build_us_securities_from_sec_rows(rows: list[dict[str, str]]) -> list[Security]:
        allowed_exchanges = {"NASDAQ", "NYSE", "NYSE AMERICAN", "NYSEAMERICAN", "NYSE MKT", "NYSEMKT"}
        by_ticker: dict[str, Security] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            exchange = str(row.get("exchange") or "").strip().upper()
            if not ticker or not name or ticker in by_ticker or not US_TICKER_RE.fullmatch(ticker):
                continue
            if exchange and exchange not in allowed_exchanges:
                continue
            by_ticker[ticker] = Security(
                security_id="",
                market="US",
                ticker=ticker,
                name=name,
                currency="USD",
                metadata={"source": "sec", "exchange": exchange or None},
            )

        return [
            Security(
                security_id=f"US:{ticker}",
                market=sec.market,
                ticker=sec.ticker,
                name=sec.name,
                sector=sec.sector,
                currency=sec.currency,
                metadata=sec.metadata,
            )
            for ticker, sec in sorted(by_ticker.items())[:US_UNIVERSE_LIMIT]
        ]

    def _load_us_securities_live(self) -> list[Security]:
        massive_client = self._make_massive_client()
        if massive_client.available():
            try:
                payload = massive_client.fetch(
                    "https://api.polygon.io/v3/reference/tickers",
                    params={
                        "market": "stocks",
                        "locale": "us",
                        "type": "CS",
                        "active": "true",
                        "sort": "ticker",
                        "order": "asc",
                        "limit": 1000,
                    },
                )
                rows = payload.get("results")
                if isinstance(rows, list):
                    securities = self._build_us_securities_from_massive_rows(rows)
                    if securities:
                        return securities
            except Exception as exc:  # noqa: BLE001
                print(f"[provider] us_master_error source=massive error={exc}", flush=True)

        sec_client = self._make_sec_client()
        try:
            rows = sec_client.fetch_company_tickers_exchange()
            securities = self._build_us_securities_from_sec_rows(rows)
            if securities:
                return securities
        except Exception as exc:  # noqa: BLE001
            print(f"[provider] us_master_error source=sec error={exc}", flush=True)
        return []
