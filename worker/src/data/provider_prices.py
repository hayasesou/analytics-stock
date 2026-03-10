from __future__ import annotations

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from src.types import Security


class ProviderPriceMixin:
    def load_price_history(
        self,
        securities: list[Security],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        if not securities:
            return pd.DataFrame()

        live = self._load_price_history_live(securities=securities, start_date=start_date, end_date=end_date)
        covered = set(live["security_id"].dropna().unique()) if not live.empty else set()
        missing = [security for security in securities if security.security_id not in covered]

        if missing and self.allow_mock_price_fallback:
            print(
                f"[provider] prices_fallback source=mock missing_securities={len(missing)} covered={len(covered)}",
                flush=True,
            )
            mocked = self._build_mock_price_history(missing, start_date=start_date, end_date=end_date)
            if live.empty:
                return mocked
            return pd.concat([live, mocked], ignore_index=True).sort_values(["security_id", "trade_date"]).reset_index(drop=True)

        if missing:
            print(
                f"[provider] prices_missing source=live_only missing_securities={len(missing)} covered={len(covered)}",
                flush=True,
            )
        return live.sort_values(["security_id", "trade_date"]).reset_index(drop=True)

    def _build_mock_price_history(
        self,
        securities: list[Security],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        rng = self._rng()
        days = pd.bdate_range(start=start_date.date(), end=end_date.date(), freq="C")
        rows: list[dict[str, object]] = []
        for security in securities:
            drift = 0.0003 if security.market == "JP" else 0.0004
            vol = 0.015 if security.market == "JP" else 0.018
            px = rng.uniform(400, 5000) if security.market == "JP" else rng.uniform(20, 300)
            for trade_day in days:
                ret = rng.normal(drift, vol)
                close = max(0.5, px * (1.0 + ret))
                high = close * (1.0 + abs(rng.normal(0, vol / 2)))
                low = close * (1.0 - abs(rng.normal(0, vol / 2)))
                open_raw = (close + px) / 2
                adjustment_factor = 1.0
                if rng.random() < 0.0007:
                    adjustment_factor = rng.choice([0.5, 2.0])
                    px = close / adjustment_factor
                else:
                    px = close
                rows.append(
                    {
                        "security_id": security.security_id,
                        "market": security.market,
                        "trade_date": trade_day.date(),
                        "open_raw": float(open_raw),
                        "high_raw": float(max(high, close, open_raw)),
                        "low_raw": float(min(low, close, open_raw)),
                        "close_raw": float(close),
                        "volume": int(rng.integers(50_000, 8_000_000)),
                        "adjusted_close": float(close),
                        "adjustment_factor": float(adjustment_factor),
                        "source": "mock",
                    }
                )
        return pd.DataFrame(rows)

    @staticmethod
    def _normalize_daily_frame(df: pd.DataFrame) -> pd.DataFrame:
        required_cols = {
            "security_id",
            "market",
            "trade_date",
            "open_raw",
            "high_raw",
            "low_raw",
            "close_raw",
            "volume",
            "adjusted_close",
            "adjustment_factor",
            "source",
        }
        if df.empty:
            return pd.DataFrame(columns=sorted(required_cols))
        for col in required_cols:
            if col not in df.columns:
                df[col] = np.nan
        normalized = df[list(required_cols)].copy()
        normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.date
        return normalized

    def _fetch_us_prices_massive(self, security: Security, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        client = self._make_massive_client()
        if not client.available():
            return pd.DataFrame()
        endpoint = (
            "https://api.polygon.io/v2/aggs/ticker/"
            f"{str(security.ticker).strip().upper()}/range/1/day/{start_date.date().isoformat()}/{end_date.date().isoformat()}"
        )
        payload = client.fetch(endpoint, params={"adjusted": "true", "sort": "asc", "limit": 50000})
        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return pd.DataFrame()
        rows: list[dict[str, object]] = []
        for item in results:
            if not isinstance(item, dict) or item.get("t") is None:
                continue
            trade_date = datetime.utcfromtimestamp(float(item["t"]) / 1000.0).date()
            rows.append(
                {
                    "security_id": security.security_id,
                    "market": security.market,
                    "trade_date": trade_date,
                    "open_raw": float(item.get("o") or 0.0),
                    "high_raw": float(item.get("h") or 0.0),
                    "low_raw": float(item.get("l") or 0.0),
                    "close_raw": float(item.get("c") or 0.0),
                    "volume": int(float(item.get("v") or 0.0)),
                    "adjusted_close": float(item.get("c") or 0.0),
                    "adjustment_factor": 1.0,
                    "source": "massive",
                }
            )
        return pd.DataFrame(rows)

    def _fetch_us_prices_stooq(self, security: Security, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        ticker = str(security.ticker).strip().lower()
        if not ticker:
            return pd.DataFrame()
        candidates = [f"{ticker}.us", f"{ticker.replace('.', '-')}.us", f"{ticker.replace('.', '')}.us"]
        seen: set[str] = set()
        for symbol in candidates:
            if symbol in seen:
                continue
            seen.add(symbol)
            resp = self._http_get(f"https://stooq.com/q/d/l/?s={symbol}&i=d", timeout=20)
            resp.raise_for_status()
            text = resp.text.strip()
            if not text or "No data" in text:
                continue
            raw = pd.read_csv(StringIO(text))
            if raw.empty or "Date" not in raw.columns:
                continue
            raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce")
            raw = raw.dropna(subset=["Date"]).copy()
            raw = raw[(raw["Date"] >= pd.Timestamp(start_date.date())) & (raw["Date"] <= pd.Timestamp(end_date.date()))]
            if raw.empty:
                continue
            out = pd.DataFrame(
                {
                    "security_id": security.security_id,
                    "market": security.market,
                    "trade_date": raw["Date"].dt.date,
                    "open_raw": pd.to_numeric(raw.get("Open"), errors="coerce"),
                    "high_raw": pd.to_numeric(raw.get("High"), errors="coerce"),
                    "low_raw": pd.to_numeric(raw.get("Low"), errors="coerce"),
                    "close_raw": pd.to_numeric(raw.get("Close"), errors="coerce"),
                    "volume": pd.to_numeric(raw.get("Volume"), errors="coerce").fillna(0).astype(int),
                    "adjusted_close": pd.to_numeric(raw.get("Close"), errors="coerce"),
                    "adjustment_factor": 1.0,
                    "source": "stooq_us",
                }
            )
            out = out.dropna(subset=["open_raw", "high_raw", "low_raw", "close_raw"])
            if not out.empty:
                return out.reset_index(drop=True)
        return pd.DataFrame()

    def _fetch_jp_prices_stooq(self, security: Security, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        ticker = str(security.ticker).strip().lower()
        if not ticker:
            return pd.DataFrame()
        resp = self._http_get(f"https://stooq.com/q/d/l/?s={ticker}.jp&i=d", timeout=20)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "No data" in text:
            return pd.DataFrame()
        raw = pd.read_csv(StringIO(text))
        if raw.empty or "Date" not in raw.columns:
            return pd.DataFrame()
        raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce")
        raw = raw.dropna(subset=["Date"]).copy()
        raw = raw[(raw["Date"] >= pd.Timestamp(start_date.date())) & (raw["Date"] <= pd.Timestamp(end_date.date()))]
        if raw.empty:
            return pd.DataFrame()
        out = pd.DataFrame(
            {
                "security_id": security.security_id,
                "market": security.market,
                "trade_date": raw["Date"].dt.date,
                "open_raw": pd.to_numeric(raw.get("Open"), errors="coerce"),
                "high_raw": pd.to_numeric(raw.get("High"), errors="coerce"),
                "low_raw": pd.to_numeric(raw.get("Low"), errors="coerce"),
                "close_raw": pd.to_numeric(raw.get("Close"), errors="coerce"),
                "volume": pd.to_numeric(raw.get("Volume"), errors="coerce").fillna(0).astype(int),
                "adjusted_close": pd.to_numeric(raw.get("Close"), errors="coerce"),
                "adjustment_factor": 1.0,
                "source": "stooq",
            }
        )
        return out.dropna(subset=["open_raw", "high_raw", "low_raw", "close_raw"]).reset_index(drop=True)

    def _fetch_jp_prices_jquants(self, security: Security, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        client = self._make_jquants_client()
        if not client.v2_available():
            return pd.DataFrame()
        rows = client.fetch_eq_bars_daily(
            code=str(security.ticker),
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
        )
        if not rows:
            return pd.DataFrame()
        raw = pd.DataFrame(rows)
        if raw.empty or "Date" not in raw.columns:
            return pd.DataFrame()
        raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce")
        raw = raw.dropna(subset=["Date"]).copy()
        raw = raw[(raw["Date"] >= pd.Timestamp(start_date.date())) & (raw["Date"] <= pd.Timestamp(end_date.date()))]
        if raw.empty:
            return pd.DataFrame()
        out = pd.DataFrame(
            {
                "security_id": security.security_id,
                "market": security.market,
                "trade_date": raw["Date"].dt.date,
                "open_raw": pd.to_numeric(raw.get("O"), errors="coerce"),
                "high_raw": pd.to_numeric(raw.get("H"), errors="coerce"),
                "low_raw": pd.to_numeric(raw.get("L"), errors="coerce"),
                "close_raw": pd.to_numeric(raw.get("C"), errors="coerce"),
                "volume": pd.to_numeric(raw.get("Vo"), errors="coerce").fillna(0).astype(int),
                "adjusted_close": pd.to_numeric(raw.get("AdjC"), errors="coerce"),
                "adjustment_factor": pd.to_numeric(raw.get("AdjFactor"), errors="coerce").fillna(1.0),
                "source": "jquants",
            }
        )
        out["adjusted_close"] = out["adjusted_close"].fillna(out["close_raw"])
        return out.dropna(subset=["open_raw", "high_raw", "low_raw", "close_raw"]).reset_index(drop=True)

    def _load_price_history_live(self, securities: list[Security], start_date: datetime, end_date: datetime) -> pd.DataFrame:
        us = [security for security in securities if security.market == "US"]
        jp = [security for security in securities if security.market == "JP"]
        frames: list[pd.DataFrame] = []
        us_success = 0
        jp_success = 0

        for security in jp:
            try:
                df = self._fetch_jp_prices_jquants(security, start_date, end_date)
                if df.empty:
                    df = self._fetch_jp_prices_stooq(security, start_date, end_date)
            except Exception:  # noqa: BLE001
                df = pd.DataFrame()
            if not df.empty:
                jp_success += 1
                frames.append(df)

        for security in us:
            try:
                df = self._fetch_us_prices_stooq(security, start_date, end_date)
                if df.empty:
                    df = self._fetch_us_prices_massive(security, start_date, end_date)
            except Exception:  # noqa: BLE001
                df = pd.DataFrame()
            if not df.empty:
                us_success += 1
                frames.append(df)

        if not frames:
            print("[provider] prices_live source=none count=0", flush=True)
            return pd.DataFrame()
        merged = self._normalize_daily_frame(pd.concat(frames, ignore_index=True))
        print(
            f"[provider] prices_live source=jquants+stooq+massive rows={len(merged)} us_ok={us_success}/{len(us)} jp_ok={jp_success}/{len(jp)}",
            flush=True,
        )
        return merged

    def load_usdjpy(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        rng = self._rng()
        days = pd.bdate_range(start=start_date.date(), end=end_date.date(), freq="C")
        rate = 145.0
        rows: list[dict[str, object]] = []
        for trade_day in days:
            rate *= 1.0 + rng.normal(0.00005, 0.002)
            rows.append({"pair": "USDJPY", "trade_date": trade_day.date(), "rate": float(rate), "source": "mock_fred"})
        return pd.DataFrame(rows)
