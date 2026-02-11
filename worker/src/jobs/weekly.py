from __future__ import annotations

from datetime import datetime, timedelta
import os
from time import perf_counter
import traceback

import pandas as pd

from src.analytics.backtest import run_backtest, serialize_backtest_results
from src.analytics.dcf import render_dcf_markdown, run_dcf_top10
from src.analytics.features import compute_layer0_features
from src.analytics.ranking import build_top50
from src.analytics.scoring import score_securities
from src.analytics.signal import generate_b_mode_signals
from src.config import load_runtime_secrets, load_yaml_config
from src.data.provider import HybridDataProvider
from src.integrations.discord import DiscordNotifier
from src.llm.reporting import (
    generate_security_report,
    generate_security_report_with_llm,
    generate_weekly_summary_report_with_llm,
    generate_weekly_summary_report,
)
from src.storage.db import NeonRepository
from src.storage.r2 import R2Storage
from src.types import CitationItem, ReportItem

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


def _to_security_frame(securities) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": s.security_id,
                "market": s.market,
                "ticker": s.ticker,
                "name": s.name,
                "sector": s.sector,
                "industry": s.industry,
                "currency": s.currency,
            }
            for s in securities
        ]
    )


def _log_stage(stage: str, stage_started: float, run_started: float, extra: str | None = None) -> float:
    now = perf_counter()
    suffix = f" {extra}" if extra else ""
    print(
        f"[weekly] stage={stage} elapsed_sec={now - stage_started:.2f} total_sec={now - run_started:.2f}{suffix}",
        flush=True,
    )
    return now


def _remap_citations(citations: list[CitationItem], claim_ids: list[str]) -> list[CitationItem]:
    remapped: list[CitationItem] = []
    for claim_id, citation in zip(claim_ids, citations, strict=False):
        remapped.append(
            CitationItem(
                claim_id=claim_id,
                doc_version_id=citation.doc_version_id,
                page_ref=citation.page_ref,
                quote_text=citation.quote_text,
            )
        )
    return remapped


def _compute_signal_diagnostics(
    prices: pd.DataFrame,
    signal_history: pd.DataFrame,
    horizons: tuple[int, ...] = SIGNAL_DIAGNOSTIC_HORIZONS,
) -> list[dict[str, float | int | None]]:
    returns_by_horizon: dict[int, list[float]] = {h: [] for h in horizons}
    if not prices.empty and not signal_history.empty:
        prepared = prices.copy()
        prepared["trade_date"] = pd.to_datetime(prepared["trade_date"])
        price_map: dict[str, tuple[pd.Index, pd.Series]] = {}
        for security_id, group in prepared.groupby("security_id"):
            ordered = group.sort_values("trade_date")
            price_map[str(security_id)] = (
                pd.Index(ordered["trade_date"].to_numpy()),
                ordered["close_raw"].astype(float).reset_index(drop=True),
            )

        for _, row in signal_history.iterrows():
            security_id = str(row["security_id"])
            if security_id not in price_map:
                continue
            trade_dates, close_series = price_map[security_id]
            signal_ts = pd.to_datetime(row["as_of_date"])
            entry_idx = int(trade_dates.searchsorted(signal_ts, side="right"))
            if entry_idx >= len(close_series):
                continue

            entry_price = float(close_series.iloc[entry_idx])
            if entry_price <= 0:
                continue

            for horizon in horizons:
                exit_idx = entry_idx + int(horizon)
                if exit_idx >= len(close_series):
                    continue
                exit_price = float(close_series.iloc[exit_idx])
                returns_by_horizon[horizon].append((exit_price / entry_price) - 1.0)

    diagnostics: list[dict[str, float | int | None]] = []
    for horizon in horizons:
        samples = pd.Series(returns_by_horizon[horizon], dtype=float)
        if samples.empty:
            diagnostics.append(
                {
                    "horizon_days": int(horizon),
                    "hit_rate": 0.0,
                    "median_return": None,
                    "p10_return": None,
                    "p90_return": None,
                    "sample_size": 0,
                }
            )
            continue

        diagnostics.append(
            {
                "horizon_days": int(horizon),
                "hit_rate": float((samples > 0).mean()),
                "median_return": float(samples.median()),
                "p10_return": float(samples.quantile(0.10)),
                "p90_return": float(samples.quantile(0.90)),
                "sample_size": int(samples.size),
            }
        )

    return diagnostics


def _resolve_openai_model(model: str | None) -> str:
    selected = (model or "").strip()
    return selected or DEFAULT_OPENAI_MODEL


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"[weekly] invalid_int_env name={name} value={raw!r}; fallback={default}", flush=True)
        return default
    return max(value, minimum)


def _env_float(name: str, default: float, minimum: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        print(f"[weekly] invalid_float_env name={name} value={raw!r}; fallback={default}", flush=True)
        return default
    return max(value, minimum)


def run_weekly() -> str:
    run_started = perf_counter()
    print("[weekly] start preparing job", flush=True)

    cfg = load_yaml_config()
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    provider = HybridDataProvider(secrets)
    notifier = DiscordNotifier(secrets.discord_webhook_url)
    r2 = R2Storage(
        endpoint_url=secrets.r2_endpoint,
        access_key_id=secrets.r2_access_key_id,
        secret_access_key=secrets.r2_secret_access_key,
        bucket_evidence=secrets.r2_bucket_evidence,
        bucket_data=secrets.r2_bucket_data,
    )

    create_run_started = perf_counter()
    print("[weekly] stage=create_run_start", flush=True)
    run_id = repo.create_run("weekly", str(cfg.get("version", "1.1")), metadata={"baseline": True})
    print(
        f"[weekly] stage=create_run elapsed_sec={perf_counter() - create_run_started:.2f} total_sec={perf_counter() - run_started:.2f}",
        flush=True,
    )

    try:
        stage_started = run_started
        print(f"[weekly] start run_id={run_id}", flush=True)

        now = datetime.now()
        start = now - timedelta(days=365 * 5)

        securities = provider.load_securities(now)
        sec_df = _to_security_frame(securities)
        stage_started = _log_stage("load_securities", stage_started, run_started, extra=f"count={len(securities)}")

        sec_map = repo.upsert_securities(securities)
        jp_map = {k: v for k, v in sec_map.items() if k.startswith("JP:")}
        us_map = {k: v for k, v in sec_map.items() if k.startswith("US:")}
        repo.upsert_universe_membership(jp_map, "jp_tse_common", now.date(), "worker")
        repo.upsert_universe_membership(us_map, "us_sp500", now.date(), "worker")
        stage_started = _log_stage("upsert_universe", stage_started, run_started, extra=f"mapped={len(sec_map)}")

        prices = provider.load_price_history(securities, start, now)
        stage_started = _log_stage("load_price_history", stage_started, run_started, extra=f"rows={len(prices)}")

        repo.upsert_prices(prices, sec_map)
        stage_started = _log_stage("upsert_prices", stage_started, run_started, extra=f"rows={len(prices)}")

        fx_df = provider.load_usdjpy(start, now)
        repo.upsert_fx(fx_df)
        stage_started = _log_stage("upsert_fx", stage_started, run_started, extra=f"rows={len(fx_df)}")

        features = compute_layer0_features(prices)
        evidence_stats = repo.get_evidence_stats(
            features["security_id"].tolist(),
            lookback_days=EVIDENCE_LOOKBACK_DAYS,
        )
        scores = score_securities(features, cfg, evidence_stats)
        scores = scores.merge(sec_df, on=["security_id", "market"], how="left")
        stage_started = _log_stage("score", stage_started, run_started, extra=f"rows={len(scores)}")

        top_cfg = cfg["ranking"]
        top50 = build_top50(
            scores,
            top_n=int(top_cfg["top_n"]),
            jp_min=int(top_cfg["hard_min"]["jp"]),
            us_min=int(top_cfg["hard_min"]["us"]),
        )
        stage_started = _log_stage("build_top50", stage_started, run_started, extra=f"rows={len(top50)}")

        risk_alert_mode = False
        signals = generate_b_mode_signals(top50, as_of_date=now.date(), risk_alert_mode=risk_alert_mode)
        stage_started = _log_stage("generate_signals", stage_started, run_started, extra=f"rows={len(signals)}")

        repo.insert_scores(run_id, scores, sec_map)
        repo.insert_top50(run_id, top50, sec_map)
        repo.insert_signals(run_id, signals, sec_map)
        stage_started = _log_stage("persist_scores_top50_signals", stage_started, run_started)

        signal_history = repo.fetch_signals_for_diagnostics(
            as_of_date=now.date(),
            lookback_days=SIGNAL_DIAGNOSTIC_LOOKBACK_DAYS,
        )
        signal_diagnostics = _compute_signal_diagnostics(
            prices,
            signal_history,
            horizons=SIGNAL_DIAGNOSTIC_HORIZONS,
        )
        repo.upsert_signal_diagnostics_weekly(run_id, signal_diagnostics)
        stage_started = _log_stage(
            "persist_signal_diagnostics",
            stage_started,
            run_started,
            extra=f"horizons={len(signal_diagnostics)}",
        )

        latest_prices = prices.sort_values("trade_date").groupby("security_id").tail(1)
        fx_latest = float(fx_df.sort_values("trade_date").iloc[-1]["rate"])
        dcf_df = run_dcf_top10(top50, latest_prices, fx_latest, cfg)
        stage_started = _log_stage("run_dcf_top10", stage_started, run_started, extra=f"rows={len(dcf_df)}")

        weekly_events = provider.load_recent_events(now=now, hours=24 * 7)
        weekly_event_dicts = [
            {
                "event_type": e.event_type,
                "importance": e.importance,
                "event_time": e.event_time.isoformat(),
                "title": e.title,
                "summary": e.summary,
                "source_url": e.source_url,
                "doc_version_id": e.doc_version_id,
            }
            for e in weekly_events
        ]
        stage_started = _log_stage("load_events", stage_started, run_started, extra=f"count={len(weekly_events)}")

        llm_model = _resolve_openai_model(os.getenv(OPENAI_MODEL_ENV, ""))
        llm_weekly_summary_enabled = (
            os.getenv(LLM_WEEKLY_SUMMARY_ENABLED_ENV, "0") == "1"
            and bool(secrets.openai_api_key)
        )
        llm_security_reports_enabled = (
            os.getenv(LLM_SECURITY_REPORTS_ENABLED_ENV, "0") == "1"
            and bool(secrets.openai_api_key)
        )
        llm_security_timeout_sec = _env_float(
            LLM_SECURITY_REPORT_TIMEOUT_SEC_ENV,
            DEFAULT_LLM_SECURITY_REPORT_TIMEOUT_SEC,
            minimum=3.0,
        )
        llm_weekly_summary_timeout_sec = _env_float(
            LLM_WEEKLY_SUMMARY_TIMEOUT_SEC_ENV,
            DEFAULT_LLM_WEEKLY_SUMMARY_TIMEOUT_SEC,
            minimum=3.0,
        )
        llm_security_max_output_tokens = _env_int(
            LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS_ENV,
            DEFAULT_LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS,
            minimum=100,
        )
        llm_weekly_summary_max_output_tokens = _env_int(
            LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS_ENV,
            DEFAULT_LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS,
            minimum=100,
        )
        llm_security_max_calls = _env_int(
            LLM_SECURITY_REPORT_MAX_CALLS_ENV,
            DEFAULT_LLM_SECURITY_REPORT_MAX_CALLS,
            minimum=1,
        )
        llm_security_max_consecutive_failures = _env_int(
            LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES_ENV,
            DEFAULT_LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES,
            minimum=1,
        )
        llm_security_budget_sec = _env_float(
            LLM_SECURITY_REPORT_BUDGET_SEC_ENV,
            DEFAULT_LLM_SECURITY_REPORT_BUDGET_SEC,
            minimum=10.0,
        )

        if llm_weekly_summary_enabled:
            try:
                summary_report = generate_weekly_summary_report_with_llm(
                    run_id=run_id,
                    as_of=now,
                    top50=top50,
                    events=weekly_event_dicts,
                    model=llm_model,
                    api_key=secrets.openai_api_key,
                    timeout_sec=llm_weekly_summary_timeout_sec,
                    max_output_tokens=llm_weekly_summary_max_output_tokens,
                )
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[weekly] llm_weekly_summary_failed run_id={run_id} error={exc}; fallback=template",
                    flush=True,
                )
                summary_report = generate_weekly_summary_report(run_id, now, top50, weekly_event_dicts)
        else:
            summary_report = generate_weekly_summary_report(run_id, now, top50, weekly_event_dicts)

        reports_to_insert: list[ReportItem] = [summary_report]
        citations_by_security = repo.get_recent_citations_by_security(
            top50["security_id"].tolist(),
            lookback_days=EVIDENCE_LOOKBACK_DAYS,
            per_security_limit=SECURITY_REPORT_CITATION_LIMIT,
        )

        top10_ids = set(top50.sort_values("mixed_rank").head(10)["security_id"])
        llm_security_reports_runtime_enabled = llm_security_reports_enabled
        llm_security_calls = 0
        llm_security_consecutive_failures = 0
        llm_security_window_started = perf_counter()
        for _, row in top50.iterrows():
            security_citations = citations_by_security.get(row["security_id"], [])
            dcf_md = render_dcf_markdown(dcf_df, row["security_id"]) if row["security_id"] in top10_ids else None
            llm_security_elapsed = perf_counter() - llm_security_window_started
            if llm_security_reports_runtime_enabled and llm_security_calls >= llm_security_max_calls:
                llm_security_reports_runtime_enabled = False
                print(
                    f"[weekly] llm_security_reports_disabled reason=max_calls calls={llm_security_calls} threshold={llm_security_max_calls}",
                    flush=True,
                )
            if llm_security_reports_runtime_enabled and llm_security_elapsed >= llm_security_budget_sec:
                llm_security_reports_runtime_enabled = False
                print(
                    f"[weekly] llm_security_reports_disabled reason=budget elapsed_sec={llm_security_elapsed:.2f} budget_sec={llm_security_budget_sec:.2f}",
                    flush=True,
                )
            if (
                llm_security_reports_runtime_enabled
                and llm_security_consecutive_failures >= llm_security_max_consecutive_failures
            ):
                llm_security_reports_runtime_enabled = False
                print(
                    f"[weekly] llm_security_reports_disabled reason=circuit_breaker failures={llm_security_consecutive_failures}",
                    flush=True,
                )

            if llm_security_reports_runtime_enabled:
                llm_security_calls += 1
                try:
                    sec_report = generate_security_report_with_llm(
                        row,
                        now,
                        evidence_citations=security_citations,
                        dcf_markdown=dcf_md,
                        model=llm_model,
                        api_key=secrets.openai_api_key,
                        timeout_sec=llm_security_timeout_sec,
                        max_output_tokens=llm_security_max_output_tokens,
                    )
                    llm_security_consecutive_failures = 0
                except Exception as exc:  # noqa: BLE001
                    llm_security_consecutive_failures += 1
                    print(
                        f"[weekly] llm_security_report_failed security={row['security_id']} error={exc} consecutive_failures={llm_security_consecutive_failures}; fallback=template",
                        flush=True,
                    )
                    if llm_security_consecutive_failures >= llm_security_max_consecutive_failures:
                        llm_security_reports_runtime_enabled = False
                        print(
                            f"[weekly] llm_security_reports_disabled reason=circuit_breaker failures={llm_security_consecutive_failures}",
                            flush=True,
                        )
                    sec_report = generate_security_report(
                        row,
                        now,
                        evidence_citations=security_citations,
                        dcf_markdown=dcf_md,
                    )
            else:
                sec_report = generate_security_report(
                    row,
                    now,
                    evidence_citations=security_citations,
                    dcf_markdown=dcf_md,
                )
            reports_to_insert.append(sec_report)

            if row["security_id"] in top10_ids:
                claim_id = "C1"
                dcf_citations = _remap_citations(security_citations, [claim_id])
                dcf_report = ReportItem(
                    report_type="dcf",
                    title=f"DCF Report {row['security_id']}",
                    body_md=dcf_md or "DCF unavailable",
                    conclusion="感度分析レンジ内で現在価格との乖離を監視。",
                    falsification_conditions="WACC/g 前提の変化またはマージン前提崩壊時に再計算。",
                    confidence="Medium",
                    security_id=row["security_id"],
                    claims=[
                        {
                            "claim_id": claim_id,
                            "claim_text": "DCF sensitivity grid was evaluated",
                            "status": "supported" if dcf_citations else "hypothesis",
                        }
                    ],
                    citations=dcf_citations,
                )
                reports_to_insert.append(dcf_report)

        stage_started = _log_stage("build_reports", stage_started, run_started, extra=f"count={len(reports_to_insert)}")
        repo.insert_reports_bulk(run_id, reports_to_insert, sec_map, batch_size=10)
        stage_started = _log_stage("persist_reports", stage_started, run_started, extra=f"count={len(reports_to_insert)}")

        bt_results = run_backtest(prices, signals, cfg)
        stage_started = _log_stage("run_backtest", stage_started, run_started, extra=f"profiles={len(bt_results)}")
        if bt_results:
            period_start = pd.to_datetime(prices["trade_date"]).min().date()
            period_end = pd.to_datetime(prices["trade_date"]).max().date()
            backtest_run_id = repo.create_backtest_run(
                run_id=run_id,
                as_of_date=now.date(),
                period_start=period_start,
                period_end=period_end,
                common_period_start=period_start,
                common_period_end=period_end,
            )
            repo.insert_backtest_results(backtest_run_id, bt_results, sec_map)
            stage_started = _log_stage("persist_backtest", stage_started, run_started)

        if r2.available():
            prefix = f"weekly/{now.date().isoformat()}"
            r2.put_parquet(f"{prefix}/prices_daily.parquet", prices)
            r2.put_json(f"{prefix}/top50.json", {"run_id": run_id, "rows": top50.to_dict(orient="records")})
            r2.put_json(f"{prefix}/signals.json", {"run_id": run_id, "rows": signals.to_dict(orient="records")})
            r2.put_json(f"{prefix}/backtest.json", serialize_backtest_results(bt_results))
            if not dcf_df.empty:
                r2.put_parquet(f"{prefix}/dcf_top10.parquet", dcf_df)
            stage_started = _log_stage("upload_r2", stage_started, run_started)

        base_url = os.getenv("WEB_BASE_URL", "https://example.vercel.app")
        notifier.send_weekly_links(base_url, now)
        stage_started = _log_stage("notify_discord", stage_started, run_started)

        repo.finish_run(
            run_id,
            "success",
            metadata={
                "securities": len(securities),
                "top50": len(top50),
                "signals": int(signals["entry_allowed"].sum()) if not signals.empty else 0,
                "backtest_profiles": len(bt_results),
                "signal_diagnostics": signal_diagnostics,
                "llm_security_calls": llm_security_calls,
                "llm_security_failures": llm_security_consecutive_failures,
                "llm_security_runtime_enabled_end": llm_security_reports_runtime_enabled,
            },
        )
        _log_stage("finish_run", stage_started, run_started)
    except Exception as exc:  # noqa: BLE001
        repo.finish_run(
            run_id,
            "failed",
            metadata={"error": str(exc), "trace": traceback.format_exc()[-8000:]},
        )
        raise

    return run_id
