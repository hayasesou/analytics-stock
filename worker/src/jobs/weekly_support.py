from __future__ import annotations

from datetime import date, timedelta
import os
from time import perf_counter
from typing import Any

import pandas as pd

from src.types import CitationItem, Security

EVIDENCE_LOOKBACK_DAYS = 30
SECURITY_REPORT_CITATION_LIMIT = 3
SIGNAL_DIAGNOSTIC_LOOKBACK_DAYS = 730
SIGNAL_DIAGNOSTIC_HORIZONS = (5, 20, 60)
LLM_SECURITY_REPORTS_ENABLED_ENV = "LLM_SECURITY_REPORTS_ENABLED"
LLM_WEEKLY_SUMMARY_ENABLED_ENV = "LLM_WEEKLY_SUMMARY_ENABLED"
OPENAI_MODEL_ENV = "OPENAI_MODEL"
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
LLM_SECURITY_REPORT_TIMEOUT_SEC_ENV = "LLM_SECURITY_REPORT_TIMEOUT_SEC"
LLM_WEEKLY_SUMMARY_TIMEOUT_SEC_ENV = "LLM_WEEKLY_SUMMARY_TIMEOUT_SEC"
LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS_ENV = "LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS"
LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS_ENV = "LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS"
LLM_SECURITY_REPORT_MAX_CALLS_ENV = "LLM_SECURITY_REPORT_MAX_CALLS"
LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES_ENV = "LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES"
LLM_SECURITY_REPORT_BUDGET_SEC_ENV = "LLM_SECURITY_REPORT_BUDGET_SEC"
DEFAULT_LLM_SECURITY_REPORT_TIMEOUT_SEC = 12.0
DEFAULT_LLM_WEEKLY_SUMMARY_TIMEOUT_SEC = 12.0
DEFAULT_LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS = 750
DEFAULT_LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS = 600
DEFAULT_LLM_SECURITY_REPORT_MAX_CALLS = 20
DEFAULT_LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES = 3
DEFAULT_LLM_SECURITY_REPORT_BUDGET_SEC = 180.0


def _to_security_frame(securities) -> pd.DataFrame:  # noqa: ANN001
    return pd.DataFrame(
        [
            {
                "security_id": security.security_id,
                "market": security.market,
                "ticker": security.ticker,
                "name": security.name,
                "sector": security.sector,
                "industry": security.industry,
                "currency": security.currency,
            }
            for security in securities
        ]
    )


def _log_stage(stage: str, stage_started: float, run_started: float, extra: str | None = None) -> float:
    now = perf_counter()
    suffix = f" {extra}" if extra else ""
    print(f"[weekly] stage={stage} elapsed_sec={now - stage_started:.2f} total_sec={now - run_started:.2f}{suffix}", flush=True)
    return now


def _remap_citations(citations: list[CitationItem], claim_ids: list[str]) -> list[CitationItem]:
    return [
        CitationItem(
            claim_id=claim_id,
            doc_version_id=citation.doc_version_id,
            page_ref=citation.page_ref,
            quote_text=citation.quote_text,
        )
        for claim_id, citation in zip(claim_ids, citations, strict=False)
    ]


def _compute_signal_diagnostics(
    prices: pd.DataFrame,
    signal_history: pd.DataFrame,
    horizons: tuple[int, ...] = SIGNAL_DIAGNOSTIC_HORIZONS,
) -> list[dict[str, float | int | None]]:
    returns_by_horizon: dict[int, list[float]] = {horizon: [] for horizon in horizons}
    if not prices.empty and not signal_history.empty:
        prepared = prices.copy()
        prepared["trade_date"] = pd.to_datetime(prepared["trade_date"])
        price_map: dict[str, tuple[pd.Index, pd.Series]] = {}
        for security_id, group in prepared.groupby("security_id"):
            ordered = group.sort_values("trade_date")
            price_map[str(security_id)] = (pd.Index(ordered["trade_date"].to_numpy()), ordered["close_raw"].astype(float).reset_index(drop=True))
        for _, row in signal_history.iterrows():
            security_id = str(row["security_id"])
            if security_id not in price_map:
                continue
            trade_dates, close_series = price_map[security_id]
            entry_idx = int(trade_dates.searchsorted(pd.to_datetime(row["as_of_date"]), side="right"))
            if entry_idx >= len(close_series):
                continue
            entry_price = float(close_series.iloc[entry_idx])
            if entry_price <= 0:
                continue
            for horizon in horizons:
                exit_idx = entry_idx + int(horizon)
                if exit_idx < len(close_series):
                    returns_by_horizon[horizon].append((float(close_series.iloc[exit_idx]) / entry_price) - 1.0)
    diagnostics: list[dict[str, float | int | None]] = []
    for horizon in horizons:
        samples = pd.Series(returns_by_horizon[horizon], dtype=float)
        diagnostics.append(
            {
                "horizon_days": int(horizon),
                "hit_rate": float((samples > 0).mean()) if not samples.empty else 0.0,
                "median_return": float(samples.median()) if not samples.empty else None,
                "p10_return": float(samples.quantile(0.10)) if not samples.empty else None,
                "p90_return": float(samples.quantile(0.90)) if not samples.empty else None,
                "sample_size": int(samples.size),
            }
        )
    return diagnostics


def _resolve_openai_model(model: str | None) -> str:
    return (model or "").strip() or DEFAULT_OPENAI_MODEL


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(int(raw), minimum)
    except ValueError:
        print(f"[weekly] invalid_int_env name={name} value={raw!r}; fallback={default}", flush=True)
        return default


def _env_float(name: str, default: float, minimum: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(float(raw), minimum)
    except ValueError:
        print(f"[weekly] invalid_float_env name={name} value={raw!r}; fallback={default}", flush=True)
        return default


def _resolve_weekly_data_quality_policy(cfg: dict[str, Any]) -> dict[str, Any]:
    dq_root = cfg.get("data_quality", {})
    if not isinstance(dq_root, dict):
        dq_root = {}
    weekly_cfg = dq_root.get("weekly", {})
    if not isinstance(weekly_cfg, dict):
        weekly_cfg = {}
    try:
        lookback_days = max(1, int(weekly_cfg.get("lookback_days", 14)))
    except (TypeError, ValueError):
        lookback_days = 14
    min_cov_raw = weekly_cfg.get("min_coverage_ratio", {"JP": 0.8, "US": 0.8})
    if not isinstance(min_cov_raw, dict):
        min_cov_raw = {"JP": 0.8, "US": 0.8}
    min_coverage_ratio: dict[str, float] = {}
    for market, ratio in min_cov_raw.items():
        try:
            min_coverage_ratio[str(market).strip().upper()] = max(0.0, min(1.0, float(ratio)))
        except (TypeError, ValueError):
            continue
    if not min_coverage_ratio:
        min_coverage_ratio = {"JP": 0.8, "US": 0.8}
    return {"enabled": bool(weekly_cfg.get("enabled", True)), "lookback_days": lookback_days, "min_coverage_ratio": min_coverage_ratio}


def _compute_market_price_coverage(
    securities: list[Security],
    prices: pd.DataFrame,
    as_of_date: date,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    by_market_expected: dict[str, set[str]] = {}
    for security in securities:
        market = str(security.market).strip().upper()
        by_market_expected.setdefault(market, set()).add(str(security.security_id))
    if prices.empty:
        return {market: {"total": len(expected), "covered": 0, "coverage_ratio": 0.0 if expected else 1.0, "latest_trade_date": None} for market, expected in by_market_expected.items()}
    df = prices.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["market"] = df["market"].astype(str).str.upper()
    recent = df[df["trade_date"] >= (as_of_date - timedelta(days=max(1, int(lookback_days))))].copy()
    out: dict[str, dict[str, Any]] = {}
    for market, expected in by_market_expected.items():
        market_recent = recent[recent["market"] == market]
        covered_ids = set(market_recent["security_id"].astype(str).tolist()) & expected
        latest_trade_date = market_recent["trade_date"].max() if not market_recent.empty else None
        if isinstance(latest_trade_date, pd.Timestamp):
            latest_trade_date = latest_trade_date.date()
        out[market] = {
            "total": len(expected),
            "covered": len(covered_ids),
            "coverage_ratio": (len(covered_ids) / len(expected)) if expected else 1.0,
            "latest_trade_date": latest_trade_date.isoformat() if latest_trade_date else None,
        }
    return out


def _enforce_weekly_data_quality(policy: dict[str, Any], coverage: dict[str, dict[str, Any]]) -> None:
    if not bool(policy.get("enabled", True)):
        return
    thresholds = policy.get("min_coverage_ratio", {})
    if not isinstance(thresholds, dict):
        return
    breaches = []
    for market, min_ratio_raw in thresholds.items():
        metrics = coverage.get(str(market).strip().upper())
        if not metrics:
            continue
        total = int(metrics.get("total", 0) or 0)
        if total <= 0:
            continue
        ratio = float(metrics.get("coverage_ratio", 0.0) or 0.0)
        min_ratio = float(min_ratio_raw)
        if ratio < min_ratio:
            breaches.append(f"{str(market).strip().upper()}:{ratio:.3f}<{min_ratio:.3f}")
    if breaches:
        raise RuntimeError(f"weekly data quality gate failed: {', '.join(breaches)}")


__all__ = [
    "DEFAULT_LLM_SECURITY_REPORT_BUDGET_SEC",
    "DEFAULT_LLM_SECURITY_REPORT_MAX_CALLS",
    "DEFAULT_LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES",
    "DEFAULT_LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS",
    "DEFAULT_LLM_SECURITY_REPORT_TIMEOUT_SEC",
    "DEFAULT_LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS",
    "DEFAULT_LLM_WEEKLY_SUMMARY_TIMEOUT_SEC",
    "EVIDENCE_LOOKBACK_DAYS",
    "LLM_SECURITY_REPORTS_ENABLED_ENV",
    "LLM_SECURITY_REPORT_BUDGET_SEC_ENV",
    "LLM_SECURITY_REPORT_MAX_CALLS_ENV",
    "LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES_ENV",
    "LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS_ENV",
    "LLM_SECURITY_REPORT_TIMEOUT_SEC_ENV",
    "LLM_WEEKLY_SUMMARY_ENABLED_ENV",
    "LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS_ENV",
    "LLM_WEEKLY_SUMMARY_TIMEOUT_SEC_ENV",
    "OPENAI_MODEL_ENV",
    "SECURITY_REPORT_CITATION_LIMIT",
    "SIGNAL_DIAGNOSTIC_HORIZONS",
    "SIGNAL_DIAGNOSTIC_LOOKBACK_DAYS",
    "_compute_market_price_coverage",
    "_compute_signal_diagnostics",
    "_enforce_weekly_data_quality",
    "_env_float",
    "_env_int",
    "_log_stage",
    "_remap_citations",
    "_resolve_openai_model",
    "_resolve_weekly_data_quality_policy",
    "_to_security_frame",
]
