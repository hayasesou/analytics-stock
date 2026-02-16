from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from src.jobs import research as research_job


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.fundamentals = 0
        self.tasks = 0
        self.finished: tuple[str, dict] | None = None
        self.strategy_statuses: list[str] = []

    def create_run(self, run_type: str, config_version: str, metadata=None):  # noqa: ANN001
        assert run_type == "research"
        assert config_version
        return "run-research-1"

    def fetch_latest_weekly_candidates(self, limit: int = 50):
        return [
            {
                "security_id": "JP:1111",
                "market": "JP",
                "ticker": "1111",
                "name": "JP Corp 1111",
                "combined_score": 80.0,
                "confidence": "High",
                "missing_ratio": 0.1,
                "edge_score": 70.0,
                "primary_source_count": 3,
                "has_major_contradiction": False,
            }
        ][:limit]

    def upsert_strategy(self, strategy):  # noqa: ANN001
        assert strategy.name.startswith("sf-")
        self.strategy_statuses.append(strategy.status)
        return "strategy-1"

    def upsert_strategy_version(self, version):  # noqa: ANN001
        assert version.version == 1
        return "strategy-version-1"

    def insert_strategy_evaluation(self, evaluation):  # noqa: ANN001
        assert evaluation.eval_type == "quick_backtest"
        assert isinstance(evaluation.period_start, date)
        return "eval-1"

    def upsert_fundamental_snapshot(self, snapshot, security_uuid_map=None):  # noqa: ANN001, ARG002
        assert snapshot.rating in {"A", "B", "C"}
        self.fundamentals += 1

    def enqueue_agent_task(self, task_type: str, payload: dict, priority: int = 100):
        assert payload["strategy_name"].startswith("sf-")
        self.tasks += 1
        return f"task-{self.tasks}"

    def finish_run(self, run_id: str, status: str, metadata=None):  # noqa: ANN001
        self.finished = (status, metadata or {})


def test_run_research_creates_candidates_and_tasks(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(
        research_job,
        "load_yaml_config",
        lambda: {"version": "1.1", "strategy_factory": {"max_parallel_tasks": 5, "candidate_limit": 20}},
    )
    monkeypatch.setattr(research_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused", openai_api_key=None))
    monkeypatch.setattr(research_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(research_job, "parse_deep_research_file_if_configured", lambda: None)

    run_id = research_job.run_research(limit=1)

    assert run_id == "run-research-1"
    assert fake_repo.fundamentals == 1
    assert fake_repo.tasks == len(research_job.DEFAULT_AGENT_TASK_TYPES)
    assert fake_repo.finished is not None
    assert fake_repo.finished[0] == "success"
    assert fake_repo.strategy_statuses == ["candidate"]


def test_run_research_blocks_candidate_when_rating_c(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")

    def _cfg():
        return {
            "version": "1.1",
            "strategy_factory": {
                "max_parallel_tasks": 5,
                "candidate_limit": 20,
                "fundamental_overlay": {
                    "enabled": True,
                    "screening_allow_ratings": ["A", "B"],
                    "screening_pass_status": "candidate",
                    "screening_block_status": "draft",
                },
            },
        }

    def _candidates(limit: int = 50):  # noqa: ARG001
        return [
            {
                "security_id": "JP:1111",
                "market": "JP",
                "ticker": "1111",
                "name": "JP Corp 1111",
                "combined_score": 40.0,
                "confidence": "Low",
                "missing_ratio": 0.4,
                "edge_score": 10.0,
                "primary_source_count": 0,
                "has_major_contradiction": True,
            }
        ]

    fake_repo.fetch_latest_weekly_candidates = _candidates  # type: ignore[method-assign]
    monkeypatch.setattr(research_job, "load_yaml_config", _cfg)
    monkeypatch.setattr(
        research_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", openai_api_key=None),
    )
    monkeypatch.setattr(research_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(research_job, "parse_deep_research_file_if_configured", lambda: None)

    research_job.run_research(limit=1)

    assert fake_repo.strategy_statuses == ["draft"]
