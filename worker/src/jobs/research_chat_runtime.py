from __future__ import annotations

import time

from src.jobs.research_chat_support import SUPPORTED_TASK_TYPES, _resolve_runtime_cfg
from src.jobs.research_chat_tasks import process_research_task


def run_research_chat_once_impl(limit: int = 20, assigned_role: str | None = None, *, load_runtime_secrets_fn, NeonRepository_cls, process_research_task_fn=process_research_task) -> dict[str, int]:
    secrets = load_runtime_secrets_fn()
    repo = NeonRepository_cls(secrets.database_url)
    tasks = repo.fetch_queued_agent_tasks(limit=limit, task_types=list(SUPPORTED_TASK_TYPES), assigned_role=assigned_role)
    stats = {"queued": len(tasks), "processed": 0, "success": 0, "failed": 0}
    for task in tasks:
        task_id = str(task["id"])
        stats["processed"] += 1
        try:
            repo.mark_agent_task(task_id=task_id, status="running")
            result = process_research_task_fn(repo, task, load_runtime_secrets_fn=load_runtime_secrets_fn)
            repo.mark_agent_task(task_id=task_id, status="success", result=result, cost_usd=0.0)
            stats["success"] += 1
        except Exception as exc:  # noqa: BLE001
            repo.mark_agent_task(task_id=task_id, status="failed", result={"error": str(exc), "task_type": str(task.get("task_type", ""))}, cost_usd=0.0, error_text=str(exc))
            stats["failed"] += 1
    return stats


def run_research_chat_impl(limit: int | None = None, *, load_yaml_config_fn, run_research_chat_once_fn) -> dict[str, int]:
    runtime_cfg = _resolve_runtime_cfg(load_yaml_config_fn())
    batch_size = max(1, int(limit or runtime_cfg["batch_size"]))
    poll_interval_sec = float(runtime_cfg["poll_interval_sec"])
    while True:
        summary = run_research_chat_once_fn(limit=batch_size)
        print(f"job=research_chat summary={summary}", flush=True)
        time.sleep(poll_interval_sec)
