from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd

from src.jobs import research as research_job


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.fundamentals = 0
        self.tasks = 0
        self.documents = 0
        self.finished: tuple[str, dict] | None = None
        self.strategy_statuses: list[str] = []
        self.lifecycle_rows: list[dict] = []
        self.lifecycle_updates: list[dict] = []
        self.lifecycle_reviews = []
        self.paper_metric_by_version: dict[str, dict] = {}
        self.kanban_counts: dict[str, int] = {}
        self.kanban_samples: dict[str, list[str]] = {}
        self._last_strategy_id = "strategy-1"
        self._last_strategy_version_id = "strategy-version-1"
        self._last_asset_scope = "JP_EQ"

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
        self._last_strategy_id = "strategy-1"
        self._last_asset_scope = strategy.asset_scope
        return self._last_strategy_id

    def upsert_strategy_version(self, version):  # noqa: ANN001
        assert version.version == 1
        self._last_strategy_version_id = "strategy-version-1"
        self.lifecycle_rows = [
            {
                "strategy_id": self._last_strategy_id,
                "strategy_name": version.strategy_name,
                "asset_scope": self._last_asset_scope,
                "status": self.strategy_statuses[-1] if self.strategy_statuses else "draft",
                "live_candidate": False,
                "strategy_version_id": self._last_strategy_version_id,
                "version": 1,
            }
        ]
        return self._last_strategy_version_id

    def insert_strategy_evaluation(self, evaluation):  # noqa: ANN001
        assert evaluation.eval_type in {"robust_backtest", "paper"}
        assert isinstance(evaluation.period_start, date)
        return "eval-1"

    def upsert_fundamental_snapshot(self, snapshot, security_uuid_map=None):  # noqa: ANN001, ARG002
        assert snapshot.rating in {"A", "B", "C"}
        self.fundamentals += 1

    def upsert_document_with_version(self, **kwargs):  # noqa: ANN003
        assert kwargs["source_system"] == "deep_research"
        assert kwargs["mime_type"] == "text/plain"
        self.documents += 1
        return "doc-version-1"

    def enqueue_agent_task(self, task_type: str, payload: dict, priority: int = 100):
        assert payload["strategy_name"].startswith("sf-")
        self.tasks += 1
        return f"task-{self.tasks}"

    def finish_run(self, run_id: str, status: str, metadata=None):  # noqa: ANN001
        self.finished = (status, metadata or {})

    def fetch_price_history_for_security(self, security_id: str, start_date: date, end_date: date):  # noqa: ANN001
        _ = (security_id, start_date, end_date)
        return pd.DataFrame(
            [
                {
                    "security_id": "JP:1111",
                    "market": "JP",
                    "trade_date": date(2025, 1, 10),
                    "open_raw": 100.0,
                    "high_raw": 101.0,
                    "low_raw": 99.0,
                    "close_raw": 100.5,
                }
            ]
        )

    def fetch_strategies_for_lifecycle(self, statuses=None, limit: int = 200):  # noqa: ANN001, ARG002
        rows = [row for row in self.lifecycle_rows if row.get("status") in set(statuses or [])]
        return rows[:limit]

    def fetch_strategy_paper_metrics(self, strategy_version_id: str, lookback_days: int = 365):  # noqa: ARG002
        return self.paper_metric_by_version.get(
            strategy_version_id,
            {
                "paper_days": 0,
                "round_trips": 0,
                "first_intent_at": None,
                "last_intent_at": None,
                "max_drawdown": None,
                "sharpe_20d": None,
            },
        )

    def update_strategy_lifecycle_state(self, *, strategy_id: str, status: str, live_candidate: bool):
        self.lifecycle_updates.append(
            {
                "strategy_id": strategy_id,
                "status": status,
                "live_candidate": live_candidate,
            }
        )
        for row in self.lifecycle_rows:
            if str(row.get("strategy_id")) == strategy_id:
                row["status"] = status
                row["live_candidate"] = live_candidate

    def insert_strategy_lifecycle_review(self, review):  # noqa: ANN001
        self.lifecycle_reviews.append(review)
        return f"lifecycle-review-{len(self.lifecycle_reviews)}"

    def fetch_research_kanban_counts(self, statuses=None):  # noqa: ANN001, ARG002
        return dict(self.kanban_counts)

    def fetch_research_kanban_samples(self, statuses=None, limit_per_lane: int = 3):  # noqa: ANN001, ARG002
        _ = limit_per_lane
        return {lane: list(items) for lane, items in self.kanban_samples.items()}


class _FakeNotifier:
    kanban_calls: list[dict] = []

    def __init__(self, webhook_url: str | None, timeout_sec: int = 10):  # noqa: ARG002
        self.webhook_url = webhook_url

    def send_research_kanban(self, now, counts, samples=None):  # noqa: ANN001
        _FakeNotifier.kanban_calls.append(
            {
                "now": now,
                "counts": dict(counts),
                "samples": dict(samples or {}),
            }
        )

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
    monkeypatch.setattr(
        research_job,
        "run_walk_forward_validation",
        lambda **kwargs: {  # noqa: ARG005
            "gate": {"passed": True, "primary_cost_profile": "strict", "reasons": []},
            "summary": {
                "strict": {
                    "fold_count": 3,
                    "total_trades": 10,
                    "mean_sharpe": 0.5,
                    "median_sharpe": 0.4,
                    "worst_max_dd": -0.15,
                    "mean_cagr": 0.08,
                }
            },
            "folds": [],
        },
    )

    run_id = research_job.run_research(limit=1)

    assert run_id == "run-research-1"
    assert fake_repo.fundamentals == 1
    assert fake_repo.tasks == len(research_job.DEFAULT_AGENT_TASK_TYPES)
    assert fake_repo.finished is not None
    assert fake_repo.finished[0] == "success"
    assert fake_repo.strategy_statuses == ["candidate"]
    assert any(update["status"] == "paper" for update in fake_repo.lifecycle_updates)
    assert any(review.action == "promote_paper" for review in fake_repo.lifecycle_reviews)


def test_run_research_sends_kanban_bulletin_when_counts_exist(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    fake_repo.kanban_counts = {
        "new": 1,
        "analyzing": 1,
        "rejected": 0,
        "candidate": 2,
        "paper": 1,
        "live": 0,
    }
    fake_repo.kanban_samples = {
        "new": ["yt claim #1"],
        "candidate": ["sf-btc-main"],
    }
    _FakeNotifier.kanban_calls = []

    monkeypatch.setattr(
        research_job,
        "load_yaml_config",
        lambda: {"version": "1.1", "strategy_factory": {"max_parallel_tasks": 5, "candidate_limit": 20}},
    )
    monkeypatch.setattr(
        research_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", openai_api_key=None, discord_webhook_url=None),
    )
    monkeypatch.setattr(research_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(research_job, "DiscordNotifier", _FakeNotifier)
    monkeypatch.setattr(research_job, "parse_deep_research_file_if_configured", lambda: None)
    monkeypatch.setattr(
        research_job,
        "run_walk_forward_validation",
        lambda **kwargs: {  # noqa: ARG005
            "gate": {"passed": True, "primary_cost_profile": "strict", "reasons": []},
            "summary": {
                "strict": {
                    "fold_count": 3,
                    "total_trades": 10,
                    "mean_sharpe": 0.5,
                    "median_sharpe": 0.4,
                    "worst_max_dd": -0.15,
                    "mean_cagr": 0.08,
                }
            },
            "folds": [],
        },
    )

    research_job.run_research(limit=1)

    assert len(_FakeNotifier.kanban_calls) == 1
    assert _FakeNotifier.kanban_calls[0]["counts"]["candidate"] == 2
    assert _FakeNotifier.kanban_calls[0]["samples"]["new"] == ["yt claim #1"]


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
    monkeypatch.setattr(
        research_job,
        "run_walk_forward_validation",
        lambda **kwargs: {  # noqa: ARG005
            "gate": {"passed": False, "primary_cost_profile": "strict", "reasons": ["median_sharpe<0.3"]},
            "summary": {
                "strict": {
                    "fold_count": 1,
                    "total_trades": 1,
                    "mean_sharpe": -0.1,
                    "median_sharpe": -0.1,
                    "worst_max_dd": -0.5,
                    "mean_cagr": -0.02,
                }
            },
            "folds": [],
        },
    )

    research_job.run_research(limit=1)

    assert fake_repo.strategy_statuses == ["draft"]


def test_run_research_imports_deep_research_and_stores_document(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    deep_input = SimpleNamespace(
        security_id="JP:1111",
        report_text="深い調査レポート本文",
        source="deep_research",
        report_path="/tmp/deep_research.txt",
    )

    class _FakeR2:
        def __init__(self, **kwargs):  # noqa: ANN003
            _ = kwargs

        def available(self) -> bool:
            return False

        def put_text(self, key: str, text: str, evidence: bool = False):  # noqa: ANN001
            _ = (key, text, evidence)

    monkeypatch.setattr(
        research_job,
        "load_yaml_config",
        lambda: {"version": "1.1", "strategy_factory": {"max_parallel_tasks": 5, "candidate_limit": 20}},
    )
    monkeypatch.setattr(
        research_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", openai_api_key=None),
    )
    monkeypatch.setattr(research_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(research_job, "R2Storage", lambda **kwargs: _FakeR2(**kwargs))
    monkeypatch.setattr(research_job, "parse_deep_research_file_if_configured", lambda: deep_input)
    monkeypatch.setattr(
        research_job,
        "build_deep_research_snapshot",
        lambda payload, api_key, model: {  # noqa: ARG005
            "source": payload.source,
            "rating": "A",
            "summary": "ok",
            "snapshot": {"drivers": ["d1"], "catalysts": ["c1"], "risks": ["r1"]},
        },
    )
    monkeypatch.setattr(
        research_job,
        "run_walk_forward_validation",
        lambda **kwargs: {  # noqa: ARG005
            "gate": {"passed": True, "primary_cost_profile": "strict", "reasons": []},
            "summary": {
                "strict": {
                    "fold_count": 3,
                    "total_trades": 10,
                    "mean_sharpe": 0.5,
                    "median_sharpe": 0.4,
                    "worst_max_dd": -0.15,
                    "mean_cagr": 0.08,
                }
            },
            "folds": [],
        },
    )

    research_job.run_research(limit=1)

    assert fake_repo.documents == 1
    assert fake_repo.fundamentals == 2


def test_run_research_marks_live_candidate_when_paper_gate_is_met(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    fake_repo.lifecycle_rows = [
        {
            "strategy_id": "strategy-crypto-1",
            "strategy_name": "sf-crypto-1",
            "asset_scope": "CRYPTO",
            "status": "paper",
            "live_candidate": False,
            "strategy_version_id": "strategy-version-crypto-1",
            "version": 3,
        }
    ]
    fake_repo.paper_metric_by_version["strategy-version-crypto-1"] = {
        "paper_days": 20,
        "round_trips": 65,
        "first_intent_at": None,
        "last_intent_at": None,
        "max_drawdown": -0.02,
        "sharpe_20d": 0.4,
    }

    monkeypatch.setattr(
        research_job,
        "load_yaml_config",
        lambda: {
            "version": "1.1",
            "strategy_factory": {
                "max_parallel_tasks": 5,
                "candidate_limit": 20,
                "lifecycle": {
                    "enabled": True,
                    "evaluation_lookback_days": 365,
                    "auto_promote_candidate_to_paper": True,
                    "live_candidate_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                    "paper_requirements": {
                        "crypto": {"min_days": 14, "min_round_trips": 50},
                        "equities": {"min_days": 60, "min_round_trips": 10},
                    },
                },
            },
        },
    )
    monkeypatch.setattr(
        research_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", openai_api_key=None),
    )
    monkeypatch.setattr(research_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(research_job, "parse_deep_research_file_if_configured", lambda: None)
    monkeypatch.setattr(
        research_job,
        "run_walk_forward_validation",
        lambda **kwargs: {  # noqa: ARG005
            "gate": {"passed": True, "primary_cost_profile": "strict", "reasons": []},
            "summary": {
                "strict": {
                    "fold_count": 3,
                    "total_trades": 10,
                    "mean_sharpe": 0.5,
                    "median_sharpe": 0.4,
                    "worst_max_dd": -0.15,
                    "mean_cagr": 0.08,
                }
            },
            "folds": [],
        },
    )
    fake_repo.fetch_latest_weekly_candidates = lambda limit=50: []  # type: ignore[method-assign, unused-argument]

    research_job.run_research(limit=0)

    assert any(update["live_candidate"] is True for update in fake_repo.lifecycle_updates)
    assert any(review.action == "mark_live_candidate" for review in fake_repo.lifecycle_reviews)
