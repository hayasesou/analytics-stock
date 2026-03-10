from __future__ import annotations

import time

from src.jobs.agents_support import (
    BuiltinTaskAdapter,
    DEFAULT_AGENT_TASK_TYPES,
    OpenClawTaskAdapter,
    _estimate_cost_usd,
    _resolve_agents_cfg,
    _select_adapter_for_task,
)


def run_agents_once_impl(
    limit: int = 20,
    *,
    load_yaml_config_fn,
    load_runtime_secrets_fn,
    NeonRepository_cls,
) -> dict[str, int]:
    cfg = load_yaml_config_fn()
    agents_cfg = _resolve_agents_cfg(cfg)
    repo = NeonRepository_cls(load_runtime_secrets_fn().database_url)
    builtin_adapter = BuiltinTaskAdapter()
    openclaw_adapter = OpenClawTaskAdapter(agents_cfg["openclaw_poc"]) if bool(agents_cfg["openclaw_poc"].get("enabled", False)) else None
    tasks = repo.fetch_queued_agent_tasks(limit=limit, task_types=list(DEFAULT_AGENT_TASK_TYPES))
    stats = {"queued": len(tasks), "processed": 0, "success": 0, "failed": 0, "openclaw_processed": 0, "openclaw_failed": 0, "openclaw_fallback": 0}
    for task in tasks:
        task_id = str(task["id"])
        task_type = str(task["task_type"])
        payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
        stats["processed"] += 1
        adapter_name, adapter = _select_adapter_for_task(task_type, agents_cfg, builtin_adapter, openclaw_adapter)
        if adapter_name == "openclaw_poc":
            stats["openclaw_processed"] += 1
        try:
            repo.mark_agent_task(task_id=task_id, status="running")
            try:
                outcome = adapter.execute(task_type, payload)
            except Exception as adapter_exc:  # noqa: BLE001
                if adapter_name == "openclaw_poc" and bool(agents_cfg["openclaw_poc"].get("fallback_to_builtin_on_error", True)):
                    stats["openclaw_fallback"] += 1
                    outcome = builtin_adapter.execute(task_type, payload)
                    outcome.result["adapter_error"] = str(adapter_exc)
                    outcome.result["fallback_from"] = "openclaw_poc"
                else:
                    raise
            repo.mark_agent_task(task_id=task_id, status="success", result=outcome.result, cost_usd=outcome.cost_usd)
            stats["success"] += 1
        except Exception as exc:  # noqa: BLE001
            if adapter_name == "openclaw_poc":
                stats["openclaw_failed"] += 1
            repo.mark_agent_task(task_id=task_id, status="failed", result={"error": str(exc), "provider": adapter_name}, cost_usd=_estimate_cost_usd(task_type))
            stats["failed"] += 1
    return stats


def run_agents_impl(
    poll_seconds: int = 20,
    batch_limit: int = 20,
    *,
    run_agents_once_fn,
) -> None:
    while True:
        stats = run_agents_once_fn(limit=batch_limit)
        print(
            "[agents] queued=%s processed=%s success=%s failed=%s openclaw_processed=%s openclaw_failed=%s openclaw_fallback=%s"
            % (
                stats["queued"],
                stats["processed"],
                stats["success"],
                stats["failed"],
                stats.get("openclaw_processed", 0),
                stats.get("openclaw_failed", 0),
                stats.get("openclaw_fallback", 0),
            ),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))


__all__ = ["run_agents_impl", "run_agents_once_impl"]
