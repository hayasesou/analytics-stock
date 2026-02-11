from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import uuid

import numpy as np
import pandas as pd

from src.config import RuntimeSecrets
from src.types import EventItem, Security


@dataclass
class HybridDataProvider:
    secrets: RuntimeSecrets
    seed: int = 42

    def _rng(self) -> np.random.Generator:
        return np.random.default_rng(self.seed)

    def load_securities(self, as_of_date: datetime) -> list[Security]:
        """
        MVP baseline:
        - API keys が揃わない間も weekly pipeline を検証できるよう、
          日米の疑似ユニバースを返す。
        """
        rng = self._rng()
        jp = [
            Security(
                security_id=f"JP:{code:04d}",
                market="JP",
                ticker=f"{code:04d}",
                name=f"JP Corp {code:04d}",
                sector=rng.choice(["Tech", "Industrial", "Finance", "Health"]),
                currency="JPY",
            )
            for code in range(1300, 1360)
        ]
        us = [
            Security(
                security_id=f"US:{i}",
                market="US",
                ticker=f"US{i}",
                name=f"US Holdings {i}",
                sector=rng.choice(["Technology", "Healthcare", "Financials", "Consumer"]),
                currency="USD",
            )
            for i in range(1, 121)
        ]
        return jp + us

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
        rng = self._rng()
        event_types = ["earning", "guidance", "filing", "news"]
        importance_levels = ["high", "medium", "low"]
        count = int(rng.integers(8, 20))
        events: list[EventItem] = []
        for i in range(count):
            delta_h = int(rng.integers(0, hours))
            event_time = now - timedelta(hours=delta_h)
            importance = rng.choice(importance_levels, p=[0.25, 0.45, 0.30])
            event_kind = str(rng.choice(event_types))
            title = f"Event {i + 1}: {event_kind}"
            summary = "Mock event generated for baseline operation. Replace with TDnet/EDINET/SEC ingestion."
            source_url = f"https://example.com/event/{i + 1}"
            doc_version_id = self._mock_doc_version_id(source_url, event_time)
            events.append(
                EventItem(
                    event_type=event_kind,
                    importance=str(importance),
                    event_time=event_time,
                    title=title,
                    summary=summary,
                    source_url=source_url,
                    security_id=None,
                    doc_version_id=doc_version_id,
                )
            )
        events.sort(key=lambda x: x.event_time, reverse=True)
        return events

    @staticmethod
    def _mock_doc_version_id(source_url: str, event_time: datetime) -> str:
        raw = f"{source_url}:{event_time.isoformat()}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))
