from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import traceback
from typing import Any

from src.analytics.validation import resolve_validation_policy, run_walk_forward_validation
from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.discord import DiscordNotifier
from src.research import (
    build_deep_research_snapshot,
    compute_fundamental_rating,
    parse_deep_research_file_if_configured,
)
from src.storage.db import NeonRepository
from src.storage.r2 import R2Storage
from src.types import (
    FundamentalSnapshot,
    StrategyEvaluation,
    StrategyLifecycleReview,
    StrategySpec,
    StrategyVersionSpec,
)

DEFAULT_AGENT_TASK_TYPES = [
    "strategy_design",
    "coding",
    "feature_engineering",
    "risk_evaluation",
    "orchestration",
]


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_rating_set(values: Any, default: list[str]) -> set[str]:
    if not isinstance(values, list):
        values = list(default)
    normalized = {str(v).strip().upper() for v in values if str(v).strip()}
    if not normalized:
        normalized = {str(v).strip().upper() for v in default}
    return normalized


def _strategy_status_for_rating(
    rating: str,
    overlay_cfg: dict[str, Any],
) -> str:
    if not bool(overlay_cfg.get("enabled", True)):
        return "candidate"
    allow_ratings = _normalize_rating_set(
        overlay_cfg.get("screening_allow_ratings"),
        default=["A", "B"],
    )
    candidate_status = str(overlay_cfg.get("screening_pass_status", "candidate"))
    blocked_status = str(overlay_cfg.get("screening_block_status", "draft"))
    return candidate_status if rating in allow_ratings else blocked_status


def _build_strategy_spec(row: dict[str, Any], as_of: date) -> dict[str, Any]:
    market = str(row.get("market", "JP"))
    security_id = str(row.get("security_id", "UNKNOWN"))
    asset_scope = "JP_EQ" if market == "JP" else "US_EQ"
    return {
        "name": f"sf-{security_id.lower().replace(':', '-')}-v1",
        "asset_scope": asset_scope,
        "as_of_date": as_of.isoformat(),
        "universe": {"security_id": security_id, "market": market},
        "signal": {
            "horizon": "5D",
            "features": [
                "ret_5d",
                "ret_20d",
                "vol_20d",
                "dollar_volume_20d",
                "missing_ratio",
                "fundamental_rating",
            ],
            "model": {"type": "ensemble", "params": {"alpha": 0.5, "beta": 0.5}},
        },
        "risk": {
            "max_drawdown_breach": -0.03,
            "min_sharpe_20d": 0.0,
        },
        "evaluation": {
            "metrics": ["sharpe", "max_dd", "cagr"],
            "gates": {
                "sharpe_min": 0.5,
                "max_dd_min": -0.2,
            },
        },
    }


def _build_eval_metrics(row: dict[str, Any]) -> dict[str, float]:
    combined = float(row.get("combined_score") or 0.0)
    edge = float(row.get("edge_score") or 0.0)
    sharpe = (combined / 100.0) * 1.6 + (edge / 100.0) * 0.4
    max_dd = -0.35 + (combined / 100.0) * 0.20
    cagr = (combined / 100.0) * 0.22
    return {
        "sharpe": round(sharpe, 4),
        "max_dd": round(max_dd, 4),
        "cagr": round(cagr, 4),
    }


def _extract_primary_validation_metrics(validation_result: dict[str, Any]) -> dict[str, Any]:
    gate = validation_result.get("gate") or {}
    summary = validation_result.get("summary") or {}
    primary_profile = str(gate.get("primary_cost_profile", "strict"))
    primary = summary.get(primary_profile) or {}
    return {
        "validation_passed": bool(gate.get("passed", False)),
        "validation_primary_profile": primary_profile,
        "validation_fold_count": int(primary.get("fold_count") or 0),
        "validation_total_trades": int(primary.get("total_trades") or 0),
        "validation_mean_sharpe": primary.get("mean_sharpe"),
        "validation_median_sharpe": primary.get("median_sharpe"),
        "validation_worst_max_dd": primary.get("worst_max_dd"),
        "validation_mean_cagr": primary.get("mean_cagr"),
        "validation_fail_reasons": list(gate.get("reasons") or []),
    }


def _resolve_lifecycle_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    sf_cfg = cfg.get("strategy_factory", {})
    if not isinstance(sf_cfg, dict):
        sf_cfg = {}
    lifecycle_cfg = sf_cfg.get("lifecycle", {})
    if not isinstance(lifecycle_cfg, dict):
        lifecycle_cfg = {}
    gate_cfg = lifecycle_cfg.get("live_candidate_gate", {})
    if not isinstance(gate_cfg, dict):
        gate_cfg = {}
    req_cfg = lifecycle_cfg.get("paper_requirements", {})
    if not isinstance(req_cfg, dict):
        req_cfg = {}
    crypto_req = req_cfg.get("crypto", {})
    if not isinstance(crypto_req, dict):
        crypto_req = {}
    equities_req = req_cfg.get("equities", {})
    if not isinstance(equities_req, dict):
        equities_req = {}
    return {
        "enabled": bool(lifecycle_cfg.get("enabled", True)),
        "lookback_days": max(30, int(lifecycle_cfg.get("evaluation_lookback_days", 365))),
        "auto_promote_candidate_to_paper": bool(lifecycle_cfg.get("auto_promote_candidate_to_paper", True)),
        "live_candidate_gate": {
            "max_drawdown_breach": float(gate_cfg.get("max_drawdown_breach", -0.03)),
            "min_sharpe_20d": float(gate_cfg.get("min_sharpe_20d", 0.0)),
        },
        "paper_requirements": {
            "crypto": {
                "min_days": max(1, int(crypto_req.get("min_days", 14))),
                "min_round_trips": max(1, int(crypto_req.get("min_round_trips", 50))),
            },
            "equities": {
                "min_days": max(1, int(equities_req.get("min_days", 60))),
                "min_round_trips": max(1, int(equities_req.get("min_round_trips", 10))),
            },
        },
    }


def _paper_requirements_for_scope(asset_scope: str, lifecycle_cfg: dict[str, Any]) -> dict[str, int]:
    req_cfg = lifecycle_cfg.get("paper_requirements", {})
    if not isinstance(req_cfg, dict):
        req_cfg = {}
    if asset_scope == "CRYPTO":
        raw = req_cfg.get("crypto", {})
    else:
        raw = req_cfg.get("equities", {})
    if not isinstance(raw, dict):
        raw = {}
    return {
        "min_days": int(raw.get("min_days", 60)),
        "min_round_trips": int(raw.get("min_round_trips", 10)),
    }


def _run_strategy_lifecycle(
    repo: NeonRepository,
    cfg: dict[str, Any],
    today: date,
    run_id: str,
) -> dict[str, Any]:
    lifecycle_cfg = _resolve_lifecycle_cfg(cfg)
    if not bool(lifecycle_cfg.get("enabled", True)):
        return {"enabled": False}

    lookback_days = int(lifecycle_cfg.get("lookback_days", 365))
    rows = repo.fetch_strategies_for_lifecycle(
        statuses=["candidate", "approved", "paper"],
        limit=500,
    )
    summary = {
        "enabled": True,
        "evaluated": len(rows),
        "promoted_to_paper": 0,
        "marked_live_candidate": 0,
        "demoted_live_candidate": 0,
    }

    for row in rows:
        strategy_id = str(row.get("strategy_id", "")).strip()
        strategy_version_id = str(row.get("strategy_version_id", "")).strip()
        if not strategy_id or not strategy_version_id:
            continue

        current_status = str(row.get("status", "draft")).strip().lower()
        current_live_candidate = bool(row.get("live_candidate", False))
        asset_scope = str(row.get("asset_scope", "MIXED")).strip().upper()

        paper_metrics = repo.fetch_strategy_paper_metrics(
            strategy_version_id=strategy_version_id,
            lookback_days=lookback_days,
        )
        paper_days = int(paper_metrics.get("paper_days") or 0)
        round_trips = int(paper_metrics.get("round_trips") or 0)
        max_drawdown = _to_float_or_none(paper_metrics.get("max_drawdown"))
        sharpe_20d = _to_float_or_none(paper_metrics.get("sharpe_20d"))
        first_intent_at = paper_metrics.get("first_intent_at")
        last_intent_at = paper_metrics.get("last_intent_at")

        req = _paper_requirements_for_scope(asset_scope=asset_scope, lifecycle_cfg=lifecycle_cfg)
        days_ok = paper_days >= int(req["min_days"])
        trades_ok = round_trips >= int(req["min_round_trips"])

        gate = lifecycle_cfg.get("live_candidate_gate", {})
        max_drawdown_breach = float(gate.get("max_drawdown_breach", -0.03))
        min_sharpe_20d = float(gate.get("min_sharpe_20d", 0.0))
        drawdown_ok = bool(max_drawdown is not None and max_drawdown > max_drawdown_breach)
        sharpe_ok = bool(sharpe_20d is not None and sharpe_20d >= min_sharpe_20d)
        risk_ok = drawdown_ok and sharpe_ok
        should_be_live_candidate = days_ok and trades_ok and risk_ok

        period_start = today
        if isinstance(first_intent_at, datetime):
            period_start = first_intent_at.date()
        period_end = today
        if isinstance(last_intent_at, datetime):
            period_end = last_intent_at.date()

        repo.insert_strategy_evaluation(
            StrategyEvaluation(
                strategy_version_id=strategy_version_id,
                eval_type="paper",
                period_start=period_start,
                period_end=period_end,
                metrics={
                    "paper_days": paper_days,
                    "round_trips": round_trips,
                    "max_drawdown": max_drawdown,
                    "sharpe_20d": sharpe_20d,
                    "days_ok": days_ok,
                    "round_trips_ok": trades_ok,
                    "drawdown_ok": drawdown_ok,
                    "sharpe_ok": sharpe_ok,
                    "risk_ok": risk_ok,
                    "live_candidate": should_be_live_candidate,
                    "min_days": req["min_days"],
                    "min_round_trips": req["min_round_trips"],
                    "max_drawdown_breach": max_drawdown_breach,
                    "min_sharpe_20d": min_sharpe_20d,
                },
                artifacts={
                    "run_id": run_id,
                    "lifecycle_policy": lifecycle_cfg,
                },
            )
        )

        if current_status == "candidate" and bool(lifecycle_cfg.get("auto_promote_candidate_to_paper", True)):
            repo.update_strategy_lifecycle_state(
                strategy_id=strategy_id,
                status="paper",
                live_candidate=False,
            )
            repo.insert_strategy_lifecycle_review(
                StrategyLifecycleReview(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    action="promote_paper",
                    from_status="candidate",
                    to_status="paper",
                    live_candidate=False,
                    acted_by="research-loop",
                    metadata={
                        "run_id": run_id,
                        "paper_days": paper_days,
                        "round_trips": round_trips,
                    },
                )
            )
            summary["promoted_to_paper"] += 1
            current_status = "paper"
            current_live_candidate = False

        if current_status not in {"paper", "approved"}:
            continue

        if should_be_live_candidate and not current_live_candidate:
            repo.update_strategy_lifecycle_state(
                strategy_id=strategy_id,
                status=current_status,
                live_candidate=True,
            )
            repo.insert_strategy_lifecycle_review(
                StrategyLifecycleReview(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    action="mark_live_candidate",
                    from_status=current_status,
                    to_status=current_status,
                    live_candidate=True,
                    acted_by="research-loop",
                    metadata={
                        "run_id": run_id,
                        "paper_days": paper_days,
                        "round_trips": round_trips,
                        "max_drawdown": max_drawdown,
                        "sharpe_20d": sharpe_20d,
                    },
                )
            )
            summary["marked_live_candidate"] += 1
        elif (not should_be_live_candidate) and current_live_candidate:
            unmet: list[str] = []
            if not days_ok:
                unmet.append("paper_days")
            if not trades_ok:
                unmet.append("round_trips")
            if not drawdown_ok:
                unmet.append("drawdown")
            if not sharpe_ok:
                unmet.append("sharpe_20d")
            repo.update_strategy_lifecycle_state(
                strategy_id=strategy_id,
                status=current_status,
                live_candidate=False,
            )
            repo.insert_strategy_lifecycle_review(
                StrategyLifecycleReview(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    action="demote_live_candidate",
                    from_status=current_status,
                    to_status=current_status,
                    live_candidate=False,
                    reason="paper_gate_unmet",
                    recheck_condition="all_paper_gates_pass",
                    acted_by="research-loop",
                    metadata={
                        "run_id": run_id,
                        "unmet": unmet,
                        "paper_days": paper_days,
                        "round_trips": round_trips,
                        "max_drawdown": max_drawdown,
                        "sharpe_20d": sharpe_20d,
                    },
                )
            )
            summary["demoted_live_candidate"] += 1

    return summary
def run_research(limit: int | None = None) -> str:
    cfg = load_yaml_config()
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)
    notifier = DiscordNotifier(getattr(secrets, "discord_webhook_url", None))
    r2 = R2Storage(
        endpoint_url=getattr(secrets, "r2_endpoint", None),
        access_key_id=getattr(secrets, "r2_access_key_id", None),
        secret_access_key=getattr(secrets, "r2_secret_access_key", None),
        bucket_evidence=getattr(secrets, "r2_bucket_evidence", None),
        bucket_data=getattr(secrets, "r2_bucket_data", None),
    )

    run_id = repo.create_run(
        "research",
        str(cfg.get("version", "1.1")),
        metadata={"pipeline": "strategy_factory"},
    )
    try:
        sf_cfg = cfg.get("strategy_factory", {})
        max_parallel = int(sf_cfg.get("max_parallel_tasks", 5))
        candidate_limit = int(limit if limit is not None else sf_cfg.get("candidate_limit", 20))
        agent_task_types = sf_cfg.get("agent_roles", DEFAULT_AGENT_TASK_TYPES)
        if not isinstance(agent_task_types, list) or not agent_task_types:
            agent_task_types = list(DEFAULT_AGENT_TASK_TYPES)
        overlay_cfg = sf_cfg.get("fundamental_overlay", {})
        if not isinstance(overlay_cfg, dict):
            overlay_cfg = {}
        validation_policy = resolve_validation_policy(cfg)
        candidates = repo.fetch_latest_weekly_candidates(limit=candidate_limit)
        processed = 0
        today = date.today()
        validation_lookback_days = int(validation_policy.get("lookback_days", 365))

        for row in candidates:
            processed += 1
            security_id = str(row.get("security_id"))
            market = str(row.get("market", "JP"))
            asset_scope = "JP_EQ" if market == "JP" else "US_EQ"
            rating = compute_fundamental_rating(
                combined_score=float(row.get("combined_score") or 0.0),
                confidence=str(row.get("confidence") or "Low"),
                missing_ratio=float(row.get("missing_ratio") or 1.0),
                has_major_contradiction=bool(row.get("has_major_contradiction") or False),
                primary_source_count=int(row.get("primary_source_count") or 0),
            )
            base_status = _strategy_status_for_rating(rating, overlay_cfg)

            validation_result: dict[str, Any] | None = None
            if bool(validation_policy.get("enabled", True)):
                lookback_days = int(validation_policy.get("lookback_days", 900))
                price_history = repo.fetch_price_history_for_security(
                    security_id=security_id,
                    start_date=today - timedelta(days=lookback_days),
                    end_date=today,
                )
                validation_result = run_walk_forward_validation(
                    prices=price_history,
                    security_id=security_id,
                    market=market,
                    config=cfg,
                    policy=validation_policy,
                )
                eval_metrics: dict[str, Any] = _extract_primary_validation_metrics(validation_result)
            else:
                eval_metrics = _build_eval_metrics(row)
                eval_metrics["validation_passed"] = True
                eval_metrics["validation_fail_reasons"] = []

            strategy_status = base_status
            candidate_status = str(overlay_cfg.get("screening_pass_status", "candidate"))
            blocked_status = str(overlay_cfg.get("screening_block_status", "draft"))
            if strategy_status == candidate_status and not bool(eval_metrics.get("validation_passed", False)):
                strategy_status = blocked_status

            strategy_name = f"sf-{security_id.lower().replace(':', '-')}"
            strategy_id = repo.upsert_strategy(
                StrategySpec(
                    name=strategy_name,
                    asset_scope=asset_scope,
                    status=strategy_status,
                    description=f"Generated by research loop for {security_id}",
                )
            )
            spec = _build_strategy_spec(row, today)
            strategy_version_id = repo.upsert_strategy_version(
                StrategyVersionSpec(
                    strategy_name=strategy_name,
                    version=1,
                    spec=spec,
                    created_by="research-loop",
                    is_active=False,
                )
            )
            repo.insert_strategy_evaluation(
                StrategyEvaluation(
                    strategy_version_id=strategy_version_id,
                    eval_type="robust_backtest" if bool(validation_policy.get("enabled", True)) else "quick_backtest",
                    period_start=today - timedelta(days=validation_lookback_days),
                    period_end=today,
                    metrics=eval_metrics,
                    artifacts={
                        "strategy_id": strategy_id,
                        "run_id": run_id,
                        "validation": validation_result,
                    },
                )
            )

            snapshot_payload = {
                "drivers": [
                    "score_combined_trend",
                    "edge_score",
                    "confidence_gate",
                ],
                "catalysts": ["weekly_rebalance", "evidence_update"],
                "risks": ["drawdown_gate", "liquidity_flag"],
                "combined_score": float(row.get("combined_score") or 0.0),
                "edge_score": float(row.get("edge_score") or 0.0),
                "screening_status": strategy_status,
                "validation": validation_result or {"enabled": False},
            }
            repo.upsert_fundamental_snapshot(
                FundamentalSnapshot(
                    security_id=security_id,
                    as_of_date=today,
                    source="research_loop",
                    rating=rating,
                    confidence=str(row.get("confidence") or "Low"),
                    summary=f"{security_id} rating={rating} combined={float(row.get('combined_score') or 0.0):.2f}",
                    snapshot=snapshot_payload,
                    created_by="research-loop",
                ),
            )

            if strategy_status == candidate_status:
                for idx, task_type in enumerate(agent_task_types):
                    repo.enqueue_agent_task(
                        task_type=task_type,
                        payload={
                            "run_id": run_id,
                            "strategy_name": strategy_name,
                            "strategy_version_id": strategy_version_id,
                            "security_id": security_id,
                            "market": market,
                            "combined_score": float(row.get("combined_score") or 0.0),
                            "agent_role": task_type,
                        },
                        priority=10 + idx,
                    )

            if processed >= max_parallel:
                break

        deep_input = parse_deep_research_file_if_configured()
        deep_saved = 0
        if deep_input:
            retrieved_at = datetime.now()
            report_sha = hashlib.sha256(deep_input.report_text.encode("utf-8")).hexdigest()
            security_slug = deep_input.security_id.replace(":", "_").replace("/", "_")
            report_key = f"research/deep_research/{today.isoformat()}/{security_slug}/{report_sha}.txt"
            stored_in_r2 = r2.available()
            if stored_in_r2:
                r2.put_text(report_key, deep_input.report_text, evidence=True)

            report_source_url = (
                f"file://{deep_input.report_path}"
                if getattr(deep_input, "report_path", None)
                else f"deep_research://{deep_input.security_id}/{today.isoformat()}"
            )

            doc_version_id = repo.upsert_document_with_version(
                external_doc_id=f"{deep_input.security_id}:{report_sha}",
                source_system=str(getattr(deep_input, "source", "deep_research")),
                source_url=report_source_url,
                title=f"Deep Research {deep_input.security_id} {today.isoformat()}",
                published_at=retrieved_at,
                retrieved_at=retrieved_at,
                sha256=report_sha,
                mime_type="text/plain",
                r2_object_key=report_key,
                r2_text_key=report_key,
                page_count=1,
            )

            deep_snapshot = build_deep_research_snapshot(
                deep_input,
                api_key=secrets.openai_api_key,
                model=(sf_cfg.get("escalation", {}) or {}).get("heavy_model"),
            )
            deep_snapshot_payload = dict(deep_snapshot["snapshot"])
            deep_snapshot_payload.update(
                {
                    "doc_version_id": doc_version_id,
                    "r2_text_key": report_key,
                    "sha256": report_sha,
                }
            )
            if stored_in_r2:
                deep_snapshot_payload["raw_report_storage"] = "r2_evidence"
            else:
                # Fallback for local/dev environments without R2.
                deep_snapshot_payload["raw_report_storage"] = "fundamental_snapshot_inline"
                deep_snapshot_payload["raw_report_text"] = deep_input.report_text
            # Keep import idempotent by using today's snapshot with dedicated source.
            try:
                repo.upsert_fundamental_snapshot(
                    FundamentalSnapshot(
                        security_id=deep_input.security_id,
                        as_of_date=today,
                        source=deep_snapshot["source"],
                        rating=str(deep_snapshot["rating"]),
                        confidence="High",
                        summary=str(deep_snapshot["summary"]),
                        snapshot=deep_snapshot_payload,
                        created_by="deep-research-import",
                    ),
                )
                deep_saved = 1
            except KeyError:
                deep_saved = 0

        lifecycle_summary = _run_strategy_lifecycle(
            repo=repo,
            cfg=cfg,
            today=today,
            run_id=run_id,
        )

        kanban_counts: dict[str, int] = {}
        kanban_samples: dict[str, list[str]] = {}
        if hasattr(repo, "fetch_research_kanban_counts") and hasattr(repo, "fetch_research_kanban_samples"):
            try:
                kanban_counts = repo.fetch_research_kanban_counts()
                kanban_samples = repo.fetch_research_kanban_samples(limit_per_lane=2)
            except Exception:  # noqa: BLE001
                kanban_counts = {}
                kanban_samples = {}
        if kanban_counts:
            notifier.send_research_kanban(
                now=datetime.now(),
                counts=kanban_counts,
                samples=kanban_samples,
            )

        repo.finish_run(
            run_id,
            "success",
            metadata={
                "candidate_count": len(candidates),
                "processed": processed,
                "deep_research_imported": deep_saved,
                "validation_policy": validation_policy,
                "lifecycle": lifecycle_summary,
                "kanban_counts": kanban_counts,
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
