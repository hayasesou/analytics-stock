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
from src.jobs.weekly_support import *
from src.storage.r2 import R2Storage
from src.types import ReportItem


def run_weekly_impl(
    *,
    load_yaml_config_fn,
    load_runtime_secrets_fn,
    NeonRepository_cls,
    HybridDataProvider_cls,
    DiscordNotifier_cls,
    generate_security_report_fn,
    generate_security_report_with_llm_fn,
    generate_weekly_summary_report_fn,
    generate_weekly_summary_report_with_llm_fn,
) -> str:
    run_started = perf_counter()
    print("[weekly] start preparing job", flush=True)
    cfg = load_yaml_config_fn()
    secrets = load_runtime_secrets_fn()
    repo = NeonRepository_cls(secrets.database_url)
    provider = HybridDataProvider_cls(secrets)
    notifier = DiscordNotifier_cls(secrets.discord_webhook_url)
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
    print(f"[weekly] stage=create_run elapsed_sec={perf_counter() - create_run_started:.2f} total_sec={perf_counter() - run_started:.2f}", flush=True)
    try:
        stage_started = run_started
        print(f"[weekly] start run_id={run_id}", flush=True)
        now = datetime.now()
        start = now - timedelta(days=365 * 5)
        securities = provider.load_securities(now)
        sec_df = _to_security_frame(securities)
        stage_started = _log_stage("load_securities", stage_started, run_started, extra=f"count={len(securities)}")
        sec_map = repo.upsert_securities(securities)
        jp_map = {key: value for key, value in sec_map.items() if key.startswith("JP:")}
        us_map = {key: value for key, value in sec_map.items() if key.startswith("US:")}
        repo.upsert_universe_membership(jp_map, "jp_tse_common", now.date(), "worker")
        repo.upsert_universe_membership(us_map, "us_sp500", now.date(), "worker")
        stage_started = _log_stage("upsert_universe", stage_started, run_started, extra=f"mapped={len(sec_map)}")
        prices = provider.load_price_history(securities, start, now)
        stage_started = _log_stage("load_price_history", stage_started, run_started, extra=f"rows={len(prices)}")
        dq_policy = _resolve_weekly_data_quality_policy(cfg)
        dq_coverage = _compute_market_price_coverage(securities=securities, prices=prices, as_of_date=now.date(), lookback_days=int(dq_policy["lookback_days"]))
        _enforce_weekly_data_quality(dq_policy, dq_coverage)
        stage_started = _log_stage("data_quality", stage_started, run_started, extra=", ".join([f"{market}={int(values.get('covered', 0))}/{int(values.get('total', 0))}({float(values.get('coverage_ratio', 0.0)):.2f})" for market, values in sorted(dq_coverage.items())]))
        repo.delete_prices_range(list(sec_map.values()), start.date(), now.date())
        repo.upsert_prices(prices, sec_map)
        stage_started = _log_stage("upsert_prices", stage_started, run_started, extra=f"rows={len(prices)}")
        fx_df = provider.load_usdjpy(start, now)
        repo.upsert_fx(fx_df)
        stage_started = _log_stage("upsert_fx", stage_started, run_started, extra=f"rows={len(fx_df)}")
        features = compute_layer0_features(prices)
        evidence_stats = repo.get_evidence_stats(features["security_id"].tolist(), lookback_days=EVIDENCE_LOOKBACK_DAYS)
        scores = score_securities(features, cfg, evidence_stats).merge(sec_df, on=["security_id", "market"], how="left")
        stage_started = _log_stage("score", stage_started, run_started, extra=f"rows={len(scores)}")
        top_cfg = cfg["ranking"]
        top50 = build_top50(scores, top_n=int(top_cfg["top_n"]), jp_min=int(top_cfg["hard_min"]["jp"]), us_min=int(top_cfg["hard_min"]["us"]))
        stage_started = _log_stage("build_top50", stage_started, run_started, extra=f"rows={len(top50)}")
        signals = generate_b_mode_signals(top50, as_of_date=now.date(), risk_alert_mode=False)
        stage_started = _log_stage("generate_signals", stage_started, run_started, extra=f"rows={len(signals)}")
        repo.insert_scores(run_id, scores, sec_map)
        repo.insert_top50(run_id, top50, sec_map)
        repo.insert_signals(run_id, signals, sec_map)
        stage_started = _log_stage("persist_scores_top50_signals", stage_started, run_started)
        signal_history = repo.fetch_signals_for_diagnostics(as_of_date=now.date(), lookback_days=SIGNAL_DIAGNOSTIC_LOOKBACK_DAYS)
        signal_diagnostics = _compute_signal_diagnostics(prices, signal_history, horizons=SIGNAL_DIAGNOSTIC_HORIZONS)
        repo.upsert_signal_diagnostics_weekly(run_id, signal_diagnostics)
        stage_started = _log_stage("persist_signal_diagnostics", stage_started, run_started, extra=f"horizons={len(signal_diagnostics)}")
        latest_prices = prices.sort_values("trade_date").groupby("security_id").tail(1)
        dcf_df = run_dcf_top10(top50, latest_prices, float(fx_df.sort_values("trade_date").iloc[-1]["rate"]), cfg)
        stage_started = _log_stage("run_dcf_top10", stage_started, run_started, extra=f"rows={len(dcf_df)}")
        weekly_events = provider.load_recent_events(now=now, hours=24 * 7)
        weekly_event_dicts = [{"event_type": event.event_type, "importance": event.importance, "event_time": event.event_time.isoformat(), "title": event.title, "summary": event.summary, "source_url": event.source_url, "doc_version_id": event.doc_version_id} for event in weekly_events]
        stage_started = _log_stage("load_events", stage_started, run_started, extra=f"count={len(weekly_events)}")
        llm_model = _resolve_openai_model(os.getenv(OPENAI_MODEL_ENV, ""))
        llm_weekly_summary_enabled = os.getenv(LLM_WEEKLY_SUMMARY_ENABLED_ENV, "0") == "1" and bool(secrets.openai_api_key)
        llm_security_reports_enabled = os.getenv(LLM_SECURITY_REPORTS_ENABLED_ENV, "0") == "1" and bool(secrets.openai_api_key)
        llm_security_timeout_sec = _env_float(LLM_SECURITY_REPORT_TIMEOUT_SEC_ENV, DEFAULT_LLM_SECURITY_REPORT_TIMEOUT_SEC, minimum=3.0)
        llm_weekly_summary_timeout_sec = _env_float(LLM_WEEKLY_SUMMARY_TIMEOUT_SEC_ENV, DEFAULT_LLM_WEEKLY_SUMMARY_TIMEOUT_SEC, minimum=3.0)
        llm_security_max_output_tokens = _env_int(LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS_ENV, DEFAULT_LLM_SECURITY_REPORT_MAX_OUTPUT_TOKENS, minimum=100)
        llm_weekly_summary_max_output_tokens = _env_int(LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS_ENV, DEFAULT_LLM_WEEKLY_SUMMARY_MAX_OUTPUT_TOKENS, minimum=100)
        llm_security_max_calls = _env_int(LLM_SECURITY_REPORT_MAX_CALLS_ENV, DEFAULT_LLM_SECURITY_REPORT_MAX_CALLS, minimum=1)
        llm_security_max_consecutive_failures = _env_int(LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES_ENV, DEFAULT_LLM_SECURITY_REPORT_MAX_CONSECUTIVE_FAILURES, minimum=1)
        llm_security_budget_sec = _env_float(LLM_SECURITY_REPORT_BUDGET_SEC_ENV, DEFAULT_LLM_SECURITY_REPORT_BUDGET_SEC, minimum=10.0)
        if llm_weekly_summary_enabled:
            try:
                summary_report = generate_weekly_summary_report_with_llm_fn(run_id=run_id, as_of=now, top50=top50, events=weekly_event_dicts, model=llm_model, api_key=secrets.openai_api_key, timeout_sec=llm_weekly_summary_timeout_sec, max_output_tokens=llm_weekly_summary_max_output_tokens)
            except Exception as exc:  # noqa: BLE001
                print(f"[weekly] llm_weekly_summary_failed run_id={run_id} error={exc}; fallback=template", flush=True)
                summary_report = generate_weekly_summary_report_fn(run_id, now, top50, weekly_event_dicts)
        else:
            summary_report = generate_weekly_summary_report_fn(run_id, now, top50, weekly_event_dicts)
        reports_to_insert: list[ReportItem] = [summary_report]
        citations_by_security = repo.get_recent_citations_by_security(top50["security_id"].tolist(), lookback_days=EVIDENCE_LOOKBACK_DAYS, per_security_limit=SECURITY_REPORT_CITATION_LIMIT)
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
                print(f"[weekly] llm_security_reports_disabled reason=max_calls calls={llm_security_calls} threshold={llm_security_max_calls}", flush=True)
            if llm_security_reports_runtime_enabled and llm_security_elapsed >= llm_security_budget_sec:
                llm_security_reports_runtime_enabled = False
                print(f"[weekly] llm_security_reports_disabled reason=budget elapsed_sec={llm_security_elapsed:.2f} budget_sec={llm_security_budget_sec:.2f}", flush=True)
            if llm_security_reports_runtime_enabled and llm_security_consecutive_failures >= llm_security_max_consecutive_failures:
                llm_security_reports_runtime_enabled = False
                print(f"[weekly] llm_security_reports_disabled reason=circuit_breaker failures={llm_security_consecutive_failures}", flush=True)
            if llm_security_reports_runtime_enabled:
                llm_security_calls += 1
                try:
                    sec_report = generate_security_report_with_llm_fn(row, now, evidence_citations=security_citations, dcf_markdown=dcf_md, model=llm_model, api_key=secrets.openai_api_key, timeout_sec=llm_security_timeout_sec, max_output_tokens=llm_security_max_output_tokens)
                    llm_security_consecutive_failures = 0
                except Exception as exc:  # noqa: BLE001
                    llm_security_consecutive_failures += 1
                    print(f"[weekly] llm_security_report_failed security={row['security_id']} error={exc} consecutive_failures={llm_security_consecutive_failures}; fallback=template", flush=True)
                    if llm_security_consecutive_failures >= llm_security_max_consecutive_failures:
                        llm_security_reports_runtime_enabled = False
                        print(f"[weekly] llm_security_reports_disabled reason=circuit_breaker failures={llm_security_consecutive_failures}", flush=True)
                    sec_report = generate_security_report_fn(row, now, evidence_citations=security_citations, dcf_markdown=dcf_md)
            else:
                sec_report = generate_security_report_fn(row, now, evidence_citations=security_citations, dcf_markdown=dcf_md)
            reports_to_insert.append(sec_report)
            if row["security_id"] in top10_ids:
                claim_id = "C1"
                dcf_citations = _remap_citations(security_citations, [claim_id])
                reports_to_insert.append(
                    ReportItem(
                        report_type="dcf",
                        title=f"DCF Report {row['security_id']}",
                        body_md=dcf_md or "DCF unavailable",
                        conclusion="感度分析レンジ内で現在価格との乖離を監視。",
                        falsification_conditions="WACC/g 前提の変化またはマージン前提崩壊時に再計算。",
                        confidence="Medium",
                        security_id=row["security_id"],
                        claims=[{"claim_id": claim_id, "claim_text": "DCF sensitivity grid was evaluated", "status": "supported" if dcf_citations else "hypothesis"}],
                        citations=dcf_citations,
                    )
                )
        stage_started = _log_stage("build_reports", stage_started, run_started, extra=f"count={len(reports_to_insert)}")
        repo.insert_reports_bulk(run_id, reports_to_insert, sec_map, batch_size=10)
        stage_started = _log_stage("persist_reports", stage_started, run_started, extra=f"count={len(reports_to_insert)}")
        bt_results = run_backtest(prices, signals, cfg)
        stage_started = _log_stage("run_backtest", stage_started, run_started, extra=f"profiles={len(bt_results)}")
        if bt_results:
            period_start = pd.to_datetime(prices["trade_date"]).min().date()
            period_end = pd.to_datetime(prices["trade_date"]).max().date()
            backtest_run_id = repo.create_backtest_run(run_id=run_id, as_of_date=now.date(), period_start=period_start, period_end=period_end, common_period_start=period_start, common_period_end=period_end)
            repo.insert_backtest_results(backtest_run_id, bt_results, sec_map)
            stage_started = _log_stage("persist_backtest", stage_started, run_started)
        if r2.available():
            prefix = f"weekly/{now.date().isoformat()}"
            r2.put_parquet(f"{prefix}/prices_daily.parquet", prices)
            r2.put_json(f"{prefix}/top50.json", {"run_id": run_id, "rows": top50.to_dict(orient='records')})
            r2.put_json(f"{prefix}/signals.json", {"run_id": run_id, "rows": signals.to_dict(orient='records')})
            r2.put_json(f"{prefix}/backtest.json", serialize_backtest_results(bt_results))
            if not dcf_df.empty:
                r2.put_parquet(f"{prefix}/dcf_top10.parquet", dcf_df)
            stage_started = _log_stage("upload_r2", stage_started, run_started)
        notifier.send_weekly_links(os.getenv("WEB_BASE_URL", "https://example.vercel.app"), now)
        stage_started = _log_stage("notify_discord", stage_started, run_started)
        repo.finish_run(run_id, "success", metadata={"securities": len(securities), "top50": len(top50), "signals": int(signals["entry_allowed"].sum()) if not signals.empty else 0, "backtest_profiles": len(bt_results), "signal_diagnostics": signal_diagnostics, "llm_security_calls": llm_security_calls, "llm_security_failures": llm_security_consecutive_failures, "llm_security_runtime_enabled_end": llm_security_reports_runtime_enabled, "data_quality_policy": dq_policy, "data_quality_coverage": dq_coverage})
        _log_stage("finish_run", stage_started, run_started)
    except Exception as exc:  # noqa: BLE001
        repo.finish_run(run_id, "failed", metadata={"error": str(exc), "trace": traceback.format_exc()[-8000:]})
        raise
    return run_id


__all__ = ["run_weekly_impl"]
