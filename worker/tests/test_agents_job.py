from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.jobs import agents as agents_job


class _FakeRepo:
    def __init__(self, _dsn: str, tasks: list[dict] | None = None):
        self.marked: list[dict[str, object]] = []
        self._tasks = tasks or [
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
            {
                "id": "task-3",
                "task_type": "idea_analysis",
                "payload": {
                    "idea_id": "idea-1",
                    "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                    "claim_text": "BTC demand growth may continue.",
                    "causal_hypothesis": "BTC inflow increase can improve momentum.",
                    "ticker_candidates": ["BTC", "eth"],
                },
            },
        ]

    def fetch_queued_agent_tasks(self, limit: int = 20, task_types=None, assigned_role=None):  # noqa: ANN001,ARG002
        return list(self._tasks)[:limit]

    def mark_agent_task(self, task_id: str, status: str, result=None, cost_usd=None):  # noqa: ANN001
        self.marked.append(
            {
                "task_id": task_id,
                "status": status,
                "result": result,
                "cost_usd": cost_usd,
            }
        )


def test_run_agents_once_processes_queued_tasks(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(agents_job, "load_yaml_config", lambda: {"agents": {"adapter": {"mode": "builtin"}}})
    monkeypatch.setattr(agents_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(agents_job, "NeonRepository", lambda dsn: fake_repo)

    stats = agents_job.run_agents_once(limit=10)

    assert stats["queued"] == 3
    assert stats["processed"] == 3
    assert stats["success"] == 3
    assert any(entry["task_id"] == "task-1" and entry["status"] == "running" for entry in fake_repo.marked)
    assert any(entry["task_id"] == "task-1" and entry["status"] == "success" for entry in fake_repo.marked)
    assert any(entry["task_id"] == "task-3" and entry["status"] == "success" for entry in fake_repo.marked)
    task1_success = next(
        entry for entry in fake_repo.marked if entry["task_id"] == "task-1" and entry["status"] == "success"
    )
    assert isinstance(task1_success.get("result"), dict)
    assert task1_success["result"]["provider"] == "builtin"


def test_process_payload_idea_analysis_returns_structured_result() -> None:
    payload = {
        "idea_id": "idea-100",
        "source_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "claim_text": "ETH network growth can improve validator revenue.",
        "causal_hypothesis": "Fee burn and usage expansion can tighten supply.",
        "ticker_candidates": ["eth", "sol"],
    }

    result = agents_job._process_payload("idea_analysis", payload)

    assert result["summary"] == "Analyzed idea idea-100"
    assert result["idea_id"] == "idea-100"
    assert result["ticker_candidates"] == ["ETH", "SOL"]
    assert result["next"] == "queue_experiment_design"


def test_run_agents_once_openclaw_mode_strategy_design_only(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(
        agents_job,
        "load_yaml_config",
        lambda: {
            "agents": {
                "adapter": {
                    "mode": "openclaw_poc",
                    "openclaw_poc": {
                        "enabled": True,
                        "strategy_design_only": True,
                        "simulated_failure_rate": 0.0,
                        "max_retries": 1,
                        "fallback_to_builtin_on_error": True,
                        "deterministic": True,
                    },
                }
            }
        },
    )
    monkeypatch.setattr(agents_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(agents_job, "NeonRepository", lambda dsn: fake_repo)

    stats = agents_job.run_agents_once(limit=10)

    assert stats["success"] == 3
    assert stats["openclaw_processed"] == 1
    task1_success = next(
        entry for entry in fake_repo.marked if entry["task_id"] == "task-1" and entry["status"] == "success"
    )
    task2_success = next(
        entry for entry in fake_repo.marked if entry["task_id"] == "task-2" and entry["status"] == "success"
    )
    assert task1_success["result"]["provider"] == "openclaw_poc"
    assert task2_success["result"]["provider"] == "builtin"


def test_run_agents_once_openclaw_failure_without_fallback_marks_failed(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        tasks=[
            {
                "id": "task-oc-fail",
                "task_type": "strategy_design",
                "payload": {"strategy_name": "sf-jp-9999", "security_id": "JP:9999", "combined_score": 42.0},
            }
        ],
    )
    monkeypatch.setattr(
        agents_job,
        "load_yaml_config",
        lambda: {
            "agents": {
                "adapter": {
                    "mode": "openclaw_poc",
                    "openclaw_poc": {
                        "enabled": True,
                        "strategy_design_only": True,
                        "simulated_failure_rate": 1.0,
                        "max_retries": 2,
                        "fallback_to_builtin_on_error": False,
                        "deterministic": True,
                    },
                }
            }
        },
    )
    monkeypatch.setattr(agents_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(agents_job, "NeonRepository", lambda dsn: fake_repo)

    stats = agents_job.run_agents_once(limit=10)

    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert stats["openclaw_failed"] == 1
    failed_entry = next(
        entry for entry in fake_repo.marked if entry["task_id"] == "task-oc-fail" and entry["status"] == "failed"
    )
    assert failed_entry["result"]["provider"] == "openclaw_poc"


def test_build_openclaw_subprocess_env_blocks_gateway_secrets() -> None:
    source_env = {
        "PATH": "/usr/bin",
        "OPENCLAW_API_KEY": "openclaw-secret",
        "GATEWAY_BINANCE_API_KEY": "binance-secret",
        "JP_GATEWAY_AUTH_TOKEN": "jp-secret",
        "IBKR_ACCOUNT_ID": "ibkr-secret",
    }

    env = agents_job.build_openclaw_subprocess_env(source_env)
    boundary = agents_job.evaluate_openclaw_security_boundary(source_env)

    assert "OPENCLAW_API_KEY" in env
    assert "PATH" in env
    assert "GATEWAY_BINANCE_API_KEY" not in env
    assert "JP_GATEWAY_AUTH_TOKEN" not in env
    assert boundary["ok"] is True


def test_run_openclaw_evaluation_writes_go_no_go_memo(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "openclaw-evaluation.md"
    monkeypatch.setattr(
        agents_job,
        "load_yaml_config",
        lambda: {
            "agents": {
                "adapter": {
                    "openclaw_poc": {
                        "enabled": True,
                        "simulated_failure_rate": 0.0,
                        "deterministic": True,
                        "max_retries": 1,
                    }
                },
                "evaluation": {
                    "samples": 2,
                    "attempts_per_sample": 2,
                    "output_path": str(output_path),
                    "go_no_go": {
                        "max_failure_rate": 0.5,
                        "min_reproducibility": 0.5,
                        "max_latency_ratio_vs_builtin": 10.0,
                        "max_cost_ratio_vs_builtin": 10.0,
                    },
                },
            }
        },
    )

    summary = agents_job.run_openclaw_evaluation()

    assert summary["decision"] in {"LIMITED_GO", "NO_GO"}
    assert summary["output_path"] == str(output_path)
    text = output_path.read_text(encoding="utf-8")
    assert "OpenClaw Evaluation Memo" in text
    assert "Decision:" in text
    assert "| Metric | Builtin | OpenClaw PoC |" in text
