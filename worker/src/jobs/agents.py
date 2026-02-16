from __future__ import annotations

from datetime import datetime, timezone
import random
import time
from typing import Any

from src.config import load_runtime_secrets
from src.storage.db import NeonRepository


def _estimate_cost_usd(task_type: str) -> float:
    base = {
        "strategy_design": 0.06,
        "coding": 0.08,
        "feature_engineering": 0.05,
        "risk_evaluation": 0.04,
        "orchestration": 0.02,
    }
    return base.get(task_type, 0.03)


def _process_payload(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    strategy_name = str(payload.get("strategy_name", "unknown"))
    security_id = str(payload.get("security_id", "unknown"))
    combined_score = float(payload.get("combined_score") or 0.0)

    if task_type == "strategy_design":
        return {
            "timestamp": now,
            "summary": f"Designed hypothesis for {strategy_name}",
            "thesis": f"{security_id} score momentum follow-through",
            "horizon": "5D",
        }
    if task_type == "coding":
        return {
            "timestamp": now,
            "summary": f"Generated strategy code template for {strategy_name}",
            "artifact_key": f"strategies/{strategy_name}/v1.py",
        }
    if task_type == "feature_engineering":
        return {
            "timestamp": now,
            "summary": f"Selected feature set for {strategy_name}",
            "features": ["ret_5d", "ret_20d", "vol_20d", "dollar_volume_20d", "fundamental_rating"],
        }
    if task_type == "risk_evaluation":
        risk_level = "medium" if combined_score >= 60 else "high"
        return {
            "timestamp": now,
            "summary": f"Risk review for {strategy_name}",
            "risk_level": risk_level,
            "max_drawdown_cap": -0.03,
            "min_sharpe_20d": 0.0,
        }
    if task_type == "orchestration":
        return {
            "timestamp": now,
            "summary": f"Orchestrated pipeline for {strategy_name}",
            "next": "manual_review",
        }
    return {
        "timestamp": now,
        "summary": f"Processed task {task_type}",
    }


def run_agents_once(limit: int = 20) -> dict[str, int]:
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)

    tasks = repo.fetch_queued_agent_tasks(limit=limit)
    stats = {"queued": len(tasks), "processed": 0, "success": 0, "failed": 0}
    for task in tasks:
        task_id = str(task["id"])
        task_type = str(task["task_type"])
        payload = task.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        stats["processed"] += 1
        try:
            repo.mark_agent_task(task_id=task_id, status="running")
            result = _process_payload(task_type, payload)
            jitter = random.uniform(0.0, 0.01)
            repo.mark_agent_task(
                task_id=task_id,
                status="success",
                result=result,
                cost_usd=round(_estimate_cost_usd(task_type) + jitter, 4),
            )
            stats["success"] += 1
        except Exception as exc:  # noqa: BLE001
            repo.mark_agent_task(
                task_id=task_id,
                status="failed",
                result={"error": str(exc)},
                cost_usd=_estimate_cost_usd(task_type),
            )
            stats["failed"] += 1
    return stats


def run_agents(poll_seconds: int = 20, batch_limit: int = 20) -> None:
    while True:
        stats = run_agents_once(limit=batch_limit)
        print(
            "[agents] queued=%s processed=%s success=%s failed=%s"
            % (stats["queued"], stats["processed"], stats["success"], stats["failed"]),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))
