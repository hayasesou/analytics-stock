from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
import uuid

import numpy as np
import pandas as pd

from src.config import RuntimeSecrets
from src.integrations.edinet import EdinetClient
from src.integrations.jquants import JQuantsClient
from src.integrations.massive import MassiveClient
from src.integrations.sec import SecEdgarClient
from src.types import EventItem, Security

JP_UNIVERSE_LIMIT = 60
US_UNIVERSE_LIMIT = 120
JP_COMMON_MARKET_KEYWORDS = ("プライム", "スタンダード", "グロース", "内国株式", "Prime", "Standard", "Growth")
US_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")
SEC_HIGH_FORMS = {"8-K", "6-K"}
SEC_MEDIUM_FORMS = {"10-K", "10-Q", "20-F", "DEF 14A"}
EDINET_HIGH_KEYWORDS = ("臨時報告書", "大量保有", "公開買付", "自己株券買付")
EDINET_MEDIUM_KEYWORDS = ("有価証券報告書", "四半期報告書", "半期報告書", "決算")


@dataclass
class HybridDataProvider:
    secrets: RuntimeSecrets
    seed: int = 42

    def _rng(self) -> np.random.Generator:
        return np.random.default_rng(self.seed)

    def load_securities(self, as_of_date: datetime) -> list[Security]:
        """
        Prefer live master data (JP: J-Quants, US: Massive/SEC).
        If unavailable, fall back to deterministic mock universe so that
        weekly pipeline can still run.
        """
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
                security_id=f"US:{i}",
                market="US",
                ticker=f"US{i}",
                name=f"US Holdings {i}",
                sector=rng.choice(["Technology", "Healthcare", "Financials", "Consumer"]),
                currency="USD",
                metadata={"source": "mock"},
            )
            for i in range(1, 121)
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

    def _load_jp_securities_live(self) -> list[Security]:
        client = JQuantsClient(
            api_key=self.secrets.jquants_api_key,
            email=self.secrets.jquants_email,
            password=self.secrets.jquants_password,
        )
        if not client.available():
            return []
        try:
            rows = client.fetch_listed_info()
        except Exception as exc:
            print(f"[provider] jp_master_error source=jquants error={exc}", flush=True)
            return []

        by_code: dict[str, Security] = {}
        for row in rows:
            code = self._normalize_jp_code(row.get("Code"))
            if not code:
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
            if not name:
                continue
            if code in by_code:
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
                metadata={
                    "source": "jquants",
                    "market_code": market_code,
                    "market_name": market_name,
                },
            )

        if not by_code:
            return []

        ordered_codes = sorted(by_code.keys())
        return [by_code[c] for c in ordered_codes[:JP_UNIVERSE_LIMIT]]

    @staticmethod
    def _build_us_securities_from_massive_rows(rows: list[dict]) -> list[Security]:
        by_ticker: dict[str, Security] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            if not ticker or not name:
                continue
            if not US_TICKER_RE.fullmatch(ticker):
                continue
            if ticker in by_ticker:
                continue

            exchange = str(row.get("primary_exchange") or "").strip() or None
            sector = str(row.get("sic_description") or "").strip() or None
            by_ticker[ticker] = Security(
                security_id="",
                market="US",
                ticker=ticker,
                name=name,
                sector=sector,
                currency="USD",
                metadata={
                    "source": "massive",
                    "exchange": exchange,
                },
            )

        securities: list[Security] = []
        for ticker in sorted(by_ticker.keys())[:US_UNIVERSE_LIMIT]:
            sec = by_ticker[ticker]
            securities.append(
                Security(
                    security_id=f"US:{ticker}",
                    market=sec.market,
                    ticker=sec.ticker,
                    name=sec.name,
                    sector=sec.sector,
                    currency=sec.currency,
                    metadata=sec.metadata,
                )
            )
        return securities

    @staticmethod
    def _build_us_securities_from_sec_rows(rows: list[dict[str, str]]) -> list[Security]:
        allowed_exchanges = {"NASDAQ", "NYSE", "NYSE AMERICAN", "NYSEAMERICAN", "NYSE MKT", "NYSEMKT"}
        by_ticker: dict[str, Security] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            exchange = str(row.get("exchange") or "").strip().upper()
            if not ticker or not name:
                continue
            if exchange and exchange not in allowed_exchanges:
                continue
            if not US_TICKER_RE.fullmatch(ticker):
                continue
            if ticker in by_ticker:
                continue

            by_ticker[ticker] = Security(
                security_id="",
                market="US",
                ticker=ticker,
                name=name,
                currency="USD",
                metadata={
                    "source": "sec",
                    "exchange": exchange or None,
                },
            )

        securities: list[Security] = []
        for ticker in sorted(by_ticker.keys())[:US_UNIVERSE_LIMIT]:
            sec = by_ticker[ticker]
            securities.append(
                Security(
                    security_id=f"US:{ticker}",
                    market=sec.market,
                    ticker=sec.ticker,
                    name=sec.name,
                    sector=sec.sector,
                    currency=sec.currency,
                    metadata=sec.metadata,
                )
            )
        return securities

    def _load_us_securities_live(self) -> list[Security]:
        massive_client = MassiveClient(self.secrets.massive_api_key)
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
            except Exception as exc:
                print(f"[provider] us_master_error source=massive error={exc}", flush=True)

        sec_client = SecEdgarClient(self.secrets.sec_user_agent)
        try:
            rows = sec_client.fetch_company_tickers_exchange()
            securities = self._build_us_securities_from_sec_rows(rows)
            if securities:
                return securities
        except Exception as exc:
            print(f"[provider] us_master_error source=sec error={exc}", flush=True)

        return []

    def load_price_history(
        self,
        securities: list[Security],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        rng = self._rng()
        days = pd.bdate_range(start=start_date.date(), end=end_date.date(), freq="C")
        rows: list[dict[str, object]] = []

        for sec in securities:
            drift = 0.0003 if sec.market == "JP" else 0.0004
            vol = 0.015 if sec.market == "JP" else 0.018
            start_price = rng.uniform(400, 5000) if sec.market == "JP" else rng.uniform(20, 300)
            px = start_price
            for d in days:
                r = rng.normal(drift, vol)
                close = max(0.5, px * (1.0 + r))
                high = close * (1.0 + abs(rng.normal(0, vol / 2)))
                low = close * (1.0 - abs(rng.normal(0, vol / 2)))
                open_ = (close + px) / 2
                volume = int(rng.integers(50_000, 8_000_000))
                adjustment_factor = 1.0
                if rng.random() < 0.0007:
                    # 擬似分割イベント
                    split_ratio = rng.choice([0.5, 2.0])
                    adjustment_factor = split_ratio
                    px = close / split_ratio
                else:
                    px = close
                rows.append(
                    {
                        "security_id": sec.security_id,
                        "market": sec.market,
                        "trade_date": d.date(),
                        "open_raw": float(open_),
                        "high_raw": float(max(high, close, open_)),
                        "low_raw": float(min(low, close, open_)),
                        "close_raw": float(close),
                        "volume": volume,
                        "adjusted_close": float(close),
                        "adjustment_factor": float(adjustment_factor),
                        "source": "mock",
                    }
                )

        return pd.DataFrame(rows)

    def load_usdjpy(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        rng = self._rng()
        days = pd.bdate_range(start=start_date.date(), end=end_date.date(), freq="C")
        rate = 145.0
        rows: list[dict[str, object]] = []
        for d in days:
            rate *= 1.0 + rng.normal(0.00005, 0.002)
            rows.append(
                {
                    "pair": "USDJPY",
                    "trade_date": d.date(),
                    "rate": float(rate),
                    "source": "mock_fred",
                }
            )
        return pd.DataFrame(rows)

    def load_recent_events(self, now: datetime, hours: int = 24) -> list[EventItem]:
        hours = max(1, int(hours))
        since = now - timedelta(hours=hours)

        sec_events = self._load_recent_events_sec(since=since, now=now)
        edinet_events = self._load_recent_events_edinet(since=since, now=now)
        live_events = sec_events + edinet_events
        if live_events:
            deduped: dict[tuple[str, str], EventItem] = {}
            for event in live_events:
                source = str(event.source_url or event.title)
                key = (source, event.event_time.isoformat())
                existing = deduped.get(key)
                if existing is None or event.event_time > existing.event_time:
                    deduped[key] = event

            events = sorted(deduped.values(), key=lambda x: x.event_time, reverse=True)
            print(
                f"[provider] recent_events source=live count={len(events)} sec={len(sec_events)} edinet={len(edinet_events)}",
                flush=True,
            )
            return events

        print(
            f"[provider] recent_events source=live count=0 sec={len(sec_events)} edinet={len(edinet_events)}",
            flush=True,
        )
        return []

    @staticmethod
    def _event_doc_version_id(source: str, event_time: datetime) -> str:
        src = str(source or "").strip() or "event"
        raw = f"{src}:{event_time.isoformat()}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))

    @staticmethod
    def _parse_event_time(raw: object) -> datetime | None:
        if isinstance(raw, datetime):
            parsed = raw
        else:
            text = str(raw or "").strip()
            if not text:
                return None
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        parsed = datetime.strptime(text, fmt)  # noqa: DTZ007
                        break
                    except ValueError:
                        continue
                else:
                    return None
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed

    @staticmethod
    def _importance_from_sec_form(form_type: str) -> str:
        form = str(form_type or "").strip().upper()
        if form in SEC_HIGH_FORMS:
            return "high"
        if form in SEC_MEDIUM_FORMS:
            return "medium"
        return "low"

    @staticmethod
    def _importance_from_edinet_description(description: str) -> str:
        desc = str(description or "").strip()
        if any(keyword in desc for keyword in EDINET_HIGH_KEYWORDS):
            return "high"
        if any(keyword in desc for keyword in EDINET_MEDIUM_KEYWORDS):
            return "medium"
        return "low"

    def _load_recent_events_sec(self, since: datetime, now: datetime) -> list[EventItem]:
        sec_client = SecEdgarClient(self.secrets.sec_user_agent)
        try:
            rows = sec_client.fetch_current_filings(count=100)
        except Exception as exc:
            print(f"[provider] recent_events_error source=sec error={exc}", flush=True)
            return []

        events: list[EventItem] = []
        for row in rows:
            event_time = self._parse_event_time(row.get("updated"))
            if event_time is None:
                continue
            if event_time < since or event_time > (now + timedelta(hours=2)):
                continue

            form_type = str(row.get("form_type") or "").strip().upper()
            company_name = str(row.get("company_name") or "").strip()
            source_url = str(row.get("source_url") or "").strip() or None
            title = f"{form_type}: {company_name}" if form_type and company_name else str(row.get("title") or "").strip()
            if not title:
                continue
            summary = str(row.get("summary") or "").strip() or "SEC filing update."
            doc_version_id = self._event_doc_version_id(source_url or title, event_time)

            events.append(
                EventItem(
                    event_type="filing",
                    importance=self._importance_from_sec_form(form_type),
                    event_time=event_time,
                    title=title,
                    summary=summary,
                    source_url=source_url,
                    security_id=None,
                    doc_version_id=doc_version_id,
                    metadata={
                        "source": "sec",
                        "form_type": form_type or None,
                    },
                )
            )
        return events

    def _load_recent_events_edinet(self, since: datetime, now: datetime) -> list[EventItem]:
        client = EdinetClient(self.secrets.edinet_api_key)
        if not client.available():
            return []

        target_dates = sorted({since.date(), now.date()})
        events: list[EventItem] = []
        for target_date in target_dates:
            try:
                rows = client.fetch_documents_list(target_date.isoformat())
            except Exception as exc:
                print(
                    f"[provider] recent_events_error source=edinet date={target_date.isoformat()} error={exc}",
                    flush=True,
                )
                continue

            for row in rows:
                doc_id = str(row.get("docID") or row.get("docId") or "").strip()
                submit_time_raw = row.get("submitDateTime") or row.get("submitDate")
                event_time = self._parse_event_time(submit_time_raw)
                if event_time is None:
                    continue
                if event_time < since or event_time > (now + timedelta(hours=2)):
                    continue

                filer_name = str(row.get("filerName") or row.get("submitterName") or "").strip()
                doc_desc = str(row.get("docDescription") or row.get("description") or "").strip()
                if not filer_name and not doc_desc:
                    continue

                title = " - ".join(part for part in [filer_name, doc_desc] if part)
                summary = doc_desc or "EDINET filing update."
                source_url = (
                    f"https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx?DocID={doc_id}"
                    if doc_id
                    else None
                )
                doc_version_id = self._event_doc_version_id(source_url or title, event_time)

                events.append(
                    EventItem(
                        event_type="filing",
                        importance=self._importance_from_edinet_description(doc_desc),
                        event_time=event_time,
                        title=title,
                        summary=summary,
                        source_url=source_url,
                        security_id=None,
                        doc_version_id=doc_version_id,
                        metadata={
                            "source": "edinet",
                            "doc_id": doc_id or None,
                            "form_code": str(row.get("formCode") or "").strip() or None,
                        },
                    )
                )
        return events
