from __future__ import annotations

from types import SimpleNamespace

from src.jobs import agents as agents_job


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.marked: list[tuple[str, str]] = []

    def fetch_queued_agent_tasks(self, limit: int = 20):  # noqa: ARG002
        return [
            {
                "id": "task-1",
                "task_type": "strategy_design",
                "payload": {"strategy_name": "sf-jp-1111", "security_id": "JP:1111", "combined_score": 70.0},
            },
            {
                "id": "task-2",
                "task_type": "risk_evaluation",
                "payload": {"strategy_name": "sf-jp-1111", "security_id": "JP:1111", "combined_score": 50.0},
            },
        ]

    def mark_agent_task(self, task_id: str, status: str, result=None, cost_usd=None):  # noqa: ANN001
        self.marked.append((task_id, status))


def test_run_agents_once_processes_queued_tasks(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(agents_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(agents_job, "NeonRepository", lambda dsn: fake_repo)

    stats = agents_job.run_agents_once(limit=10)

    assert stats["queued"] == 2
    assert stats["processed"] == 2
    assert stats["success"] == 2
    assert ("task-1", "running") in fake_repo.marked
    assert ("task-1", "success") in fake_repo.marked
