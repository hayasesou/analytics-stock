from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import os
import traceback

import numpy as np
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
from src.llm.reporting import generate_security_report, generate_weekly_summary_report
from src.storage.db import NeonRepository
from src.storage.r2 import R2Storage
from src.types import CitationItem, ReportItem


def _mock_evidence_stats(security_ids: list[str]) -> pd.DataFrame:
    rows = []
    for sid in security_ids:
        h = int(hashlib.sha256(sid.encode("utf-8")).hexdigest(), 16)
        rows.append(
            {
                "security_id": sid,
                "primary_source_count": 2 if h % 4 != 0 else 1,
                "has_key_numbers": h % 5 != 0,
                "has_major_contradiction": h % 29 == 0,
                "catalyst_bonus": 0.15 if h % 7 == 0 else 0.0,
            }
        )
    return pd.DataFrame(rows)


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


def run_weekly() -> str:
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

    run_id = repo.create_run("weekly", str(cfg.get("version", "1.1")), metadata={"baseline": True})

    try:
        now = datetime.now()
        start = now - timedelta(days=365 * 5)

        securities = provider.load_securities(now)
        sec_df = _to_security_frame(securities)

        sec_map = repo.upsert_securities(securities)
        jp_map = {k: v for k, v in sec_map.items() if k.startswith("JP:")}
        us_map = {k: v for k, v in sec_map.items() if k.startswith("US:")}
        repo.upsert_universe_membership(jp_map, "jp_tse_common", now.date(), "worker")
        repo.upsert_universe_membership(us_map, "us_sp500", now.date(), "worker")

        prices = provider.load_price_history(securities, start, now)
        repo.upsert_prices(prices, sec_map)

        fx_df = provider.load_usdjpy(start, now)
        repo.upsert_fx(fx_df)

        features = compute_layer0_features(prices)
        evidence_stats = _mock_evidence_stats(features["security_id"].tolist())
        scores = score_securities(features, cfg, evidence_stats)
        scores = scores.merge(sec_df, on=["security_id", "market"], how="left")

        top_cfg = cfg["ranking"]
        top50 = build_top50(
            scores,
            top_n=int(top_cfg["top_n"]),
            jp_min=int(top_cfg["hard_min"]["jp"]),
            us_min=int(top_cfg["hard_min"]["us"]),
        )

        risk_alert_mode = False
        signals = generate_b_mode_signals(top50, as_of_date=now.date(), risk_alert_mode=risk_alert_mode)

        repo.insert_scores(run_id, scores, sec_map)
        repo.insert_top50(run_id, top50, sec_map)
        repo.insert_signals(run_id, signals, sec_map)

        latest_prices = prices.sort_values("trade_date").groupby("security_id").tail(1)
        fx_latest = float(fx_df.sort_values("trade_date").iloc[-1]["rate"])
        dcf_df = run_dcf_top10(top50, latest_prices, fx_latest, cfg)

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

        summary_report = generate_weekly_summary_report(run_id, now, top50, weekly_event_dicts)
        repo.insert_report(run_id, summary_report, sec_map)

        top10_ids = set(top50.sort_values("mixed_rank").head(10)["security_id"])
        for _, row in top50.iterrows():
            evidence = provider.build_mock_evidence(row["security_id"], now)
            dcf_md = render_dcf_markdown(dcf_df, row["security_id"]) if row["security_id"] in top10_ids else None
            sec_report = generate_security_report(row, now, evidence, dcf_markdown=dcf_md)
            repo.insert_report(run_id, sec_report, sec_map)

            if row["security_id"] in top10_ids:
                claim_id = "C1"
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
                            "status": "supported",
                        }
                    ],
                    citations=[
                        CitationItem(
                            claim_id=claim_id,
                            doc_version_id=evidence["doc_version_id"],
                            page_ref=evidence["page_ref"],
                            quote_text=evidence["quote_text"],
                        )
                    ],
                )
                repo.insert_report(run_id, dcf_report, sec_map)

        bt_results = run_backtest(prices, signals, cfg)
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

        if r2.available():
            prefix = f"weekly/{now.date().isoformat()}"
            r2.put_parquet(f"{prefix}/prices_daily.parquet", prices)
            r2.put_json(f"{prefix}/top50.json", {"run_id": run_id, "rows": top50.to_dict(orient="records")})
            r2.put_json(f"{prefix}/signals.json", {"run_id": run_id, "rows": signals.to_dict(orient="records")})
            r2.put_json(f"{prefix}/backtest.json", serialize_backtest_results(bt_results))
            if not dcf_df.empty:
                r2.put_parquet(f"{prefix}/dcf_top10.parquet", dcf_df)

        base_url = os.getenv("WEB_BASE_URL", "https://example.vercel.app")
        notifier.send_weekly_links(base_url, now)

        repo.finish_run(
            run_id,
            "success",
            metadata={
                "securities": len(securities),
                "top50": len(top50),
                "signals": int(signals["entry_allowed"].sum()) if not signals.empty else 0,
                "backtest_profiles": len(bt_results),
            },
        )
    except Exception as exc:  # noqa: BLE001
        repo.finish_run(
            run_id,
            "failed",
            metadata={"error": str(exc), "trace": traceback.format_exc()[-8000:]},
        )
        raise

    return run_id
