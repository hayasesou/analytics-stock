from __future__ import annotations

from datetime import date, datetime
from typing import Any

from src.jobs.research_support import _to_float_or_none
from src.types import StrategyEvaluation, StrategyLifecycleReview


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
    equities_req = req_cfg.get("equities", {})
    if not isinstance(crypto_req, dict):
        crypto_req = {}
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
            "crypto": {"min_days": max(1, int((crypto_req or {}).get("min_days", 14))), "min_round_trips": max(1, int((crypto_req or {}).get("min_round_trips", 50)))},
            "equities": {"min_days": max(1, int((equities_req or {}).get("min_days", 60))), "min_round_trips": max(1, int((equities_req or {}).get("min_round_trips", 10)))},
        },
    }


def _paper_requirements_for_scope(asset_scope: str, lifecycle_cfg: dict[str, Any]) -> dict[str, int]:
    req_cfg = lifecycle_cfg.get("paper_requirements", {})
    raw = req_cfg.get("crypto", {}) if asset_scope == "CRYPTO" else req_cfg.get("equities", {})
    if not isinstance(raw, dict):
        raw = {}
    return {"min_days": int(raw.get("min_days", 60)), "min_round_trips": int(raw.get("min_round_trips", 10))}


def _run_strategy_lifecycle(repo, cfg: dict[str, Any], today: date, run_id: str) -> dict[str, Any]:
    lifecycle_cfg = _resolve_lifecycle_cfg(cfg)
    if not bool(lifecycle_cfg.get("enabled", True)):
        return {"enabled": False}
    lookback_days = int(lifecycle_cfg.get("lookback_days", 365))
    rows = repo.fetch_strategies_for_lifecycle(statuses=["candidate", "approved", "paper"], limit=500)
    summary = {"enabled": True, "evaluated": len(rows), "promoted_to_paper": 0, "marked_live_candidate": 0, "demoted_live_candidate": 0}
    for row in rows:
        strategy_id = str(row.get("strategy_id", "")).strip()
        strategy_version_id = str(row.get("strategy_version_id", "")).strip()
        if not strategy_id or not strategy_version_id:
            continue
        current_status = str(row.get("status", "draft")).strip().lower()
        current_live_candidate = bool(row.get("live_candidate", False))
        asset_scope = str(row.get("asset_scope", "MIXED")).strip().upper()
        paper_metrics = repo.fetch_strategy_paper_metrics(strategy_version_id=strategy_version_id, lookback_days=lookback_days)
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
        should_be_live_candidate = days_ok and trades_ok and drawdown_ok and sharpe_ok
        period_start = first_intent_at.date() if isinstance(first_intent_at, datetime) else today
        period_end = last_intent_at.date() if isinstance(last_intent_at, datetime) else today
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
                    "risk_ok": drawdown_ok and sharpe_ok,
                    "live_candidate": should_be_live_candidate,
                    "min_days": req["min_days"],
                    "min_round_trips": req["min_round_trips"],
                    "max_drawdown_breach": max_drawdown_breach,
                    "min_sharpe_20d": min_sharpe_20d,
                },
                artifacts={"run_id": run_id, "lifecycle_policy": lifecycle_cfg},
            )
        )
        if current_status == "candidate" and bool(lifecycle_cfg.get("auto_promote_candidate_to_paper", True)):
            repo.update_strategy_lifecycle_state(strategy_id=strategy_id, status="paper", live_candidate=False)
            repo.insert_strategy_lifecycle_review(StrategyLifecycleReview(strategy_id=strategy_id, strategy_version_id=strategy_version_id, action="promote_paper", from_status="candidate", to_status="paper", live_candidate=False, acted_by="research-loop", metadata={"run_id": run_id, "paper_days": paper_days, "round_trips": round_trips}))
            summary["promoted_to_paper"] += 1
            current_status = "paper"
            current_live_candidate = False
        if current_status not in {"paper", "approved"}:
            continue
        if should_be_live_candidate and not current_live_candidate:
            repo.update_strategy_lifecycle_state(strategy_id=strategy_id, status=current_status, live_candidate=True)
            repo.insert_strategy_lifecycle_review(StrategyLifecycleReview(strategy_id=strategy_id, strategy_version_id=strategy_version_id, action="mark_live_candidate", from_status=current_status, to_status=current_status, live_candidate=True, acted_by="research-loop", metadata={"run_id": run_id, "paper_days": paper_days, "round_trips": round_trips, "max_drawdown": max_drawdown, "sharpe_20d": sharpe_20d}))
            summary["marked_live_candidate"] += 1
        elif not should_be_live_candidate and current_live_candidate:
            unmet = []
            if not days_ok:
                unmet.append("paper_days")
            if not trades_ok:
                unmet.append("round_trips")
            if not drawdown_ok:
                unmet.append("drawdown")
            if not sharpe_ok:
                unmet.append("sharpe_20d")
            repo.update_strategy_lifecycle_state(strategy_id=strategy_id, status=current_status, live_candidate=False)
            repo.insert_strategy_lifecycle_review(StrategyLifecycleReview(strategy_id=strategy_id, strategy_version_id=strategy_version_id, action="demote_live_candidate", from_status=current_status, to_status=current_status, live_candidate=False, reason="paper_gate_unmet", recheck_condition="all_paper_gates_pass", acted_by="research-loop", metadata={"run_id": run_id, "unmet": unmet, "paper_days": paper_days, "round_trips": round_trips, "max_drawdown": max_drawdown, "sharpe_20d": sharpe_20d}))
            summary["demoted_live_candidate"] += 1
    return summary


__all__ = ["_paper_requirements_for_scope", "_resolve_lifecycle_cfg", "_run_strategy_lifecycle"]
